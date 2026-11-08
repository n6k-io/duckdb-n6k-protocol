#pragma once

#include "catch.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_server_extension.hpp"
#include "n6k_stream_framing.hpp"
#include "n6k_table_function.hpp"
#include "n6k_testing_extension.hpp"
#include "ws_json.hpp"
#include "ws_transport_raw.hpp"

#include "duckdb.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <algorithm>
#include <atomic>
#include <fcntl.h>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace duckdb {

constexpr const char *FILTER = "filter";
constexpr const char *AGGREGATE = "aggregate";

struct TwinPlan {
	std::set<std::string> rels;
	std::vector<std::string> tables;

	bool Has(const char *rel) const {
		return rels.count(rel) != 0;
	}
	const std::vector<std::string> &Tables() const {
		return tables;
	}
};

inline unique_ptr<QueryResult> TwinRun(Connection &connection, const std::string &sql) {
	unique_ptr<QueryResult> result = connection.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(sql + "\n" + result->GetError());
	}
	return result;
}

inline std::vector<std::string> TwinRows(Connection &connection, const std::string &sql) {
	auto result = TwinRun(connection, sql);
	auto values = result->Collection().GetRows();
	std::vector<std::string> rows;
	for (idx_t row = 0; row < result->RowCount(); row++) {
		std::string cells;
		for (idx_t column = 0; column < result->GetTypes().size(); column++) {
			cells += (column == 0 ? "" : "|") + values.GetValue(column, row).ToString();
		}
		rows.push_back(cells);
	}
	return rows;
}

class TwinTicks {
public:
	void Add(idx_t count) {
		if (stopped.load()) {
			throw InvalidInputException("far_tick stopped");
		}
		ticks.fetch_add(static_cast<int64_t>(count));
	}

	void Stop() {
		stopped.store(true);
	}

	bool AwaitStarted() const {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
		while (ticks.load() == 0) {
			if (std::chrono::steady_clock::now() > deadline) {
				return false;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		return true;
	}

	bool AwaitQuiet() const {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
		auto last = ticks.load();
		while (std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
			const auto now = ticks.load();
			if (now == last) {
				return true;
			}
			last = now;
		}
		return false;
	}

private:
	std::atomic<int64_t> ticks {0};
	std::atomic<bool> stopped {false};
};

class TwinClient {
public:
	TwinClient(DuckDB &far_p, Connection &far_con_p, std::vector<std::string> catalogs_p)
	    : far(far_p), far_con(far_con_p), catalogs(std::move(catalogs_p)) {
		ExtensionLoader near_loader(*near_db->instance, "n6k_client");
		LoadInternal(near_loader);
		con = make_uniq<Connection>(*near_db);
	}

	~TwinClient() {
		con.reset();
		near_db.reset();
		for (auto &link : links) {
			for (int fd : {link->near_pair[0], link->near_pair[1], link->far_pair[0], link->far_pair[1]}) {
				n6k::ShutdownSocket(fd);
			}
		}
		for (auto &link : links) {
			for (auto *thread : {&link->to_far, &link->to_near, &link->serve}) {
				if (thread->joinable()) {
					thread->join();
				}
			}
			for (int fd : {link->near_pair[0], link->near_pair[1], link->far_pair[0], link->far_pair[1]}) {
				::close(fd);
			}
		}
	}

	TwinClient(const TwinClient &) = delete;
	TwinClient &operator=(const TwinClient &) = delete;

	int OpenLink(bool nonblocking = false) {
		links.push_back(make_uniq<TwinLink>());
		auto &link = *links.back();
		if (::socketpair(AF_UNIX, SOCK_STREAM, 0, link.near_pair) != 0 ||
		    ::socketpair(AF_UNIX, SOCK_STREAM, 0, link.far_pair) != 0) {
			throw std::runtime_error("socketpair failed");
		}
		SetNoSigPipe(link.near_pair[1]);
		SetNoSigPipe(link.far_pair[0]);
		if (nonblocking) {
			::fcntl(link.near_pair[0], F_SETFL, ::fcntl(link.near_pair[0], F_GETFL) | O_NONBLOCK);
		}
		link.to_far = std::thread([this, &link] {
			Pump(link.near_pair[1], link.far_pair[0], true);
			link.near_gone.store(true);
		});
		link.to_near = std::thread([this, &link] { Pump(link.far_pair[0], link.near_pair[1], false); });

		auto session = TwinRun(far_con, "SELECT session_id FROM n6k_serve_create_session(" +
		                                    std::to_string(link.far_pair[1]) + ")");
		const auto session_id = session->Collection().GetValue(0, 0).ToString();
		std::string serve_sql = "CALL n6k_serve_fd('" + session_id + "'::UUID";
		for (auto &catalog : catalogs) {
			serve_sql += ", '" + catalog + "'";
		}
		serve_sql += ")";
		link.serve_con = make_uniq<Connection>(far);
		link.serve = std::thread([this, &link, serve_sql] {
			auto result = link.serve_con->Query(serve_sql);
			if (result->HasError()) {
				std::lock_guard<std::mutex> lk(lock);
				serve_error = result->GetError();
			}
		});

		return link.near_pair[0];
	}

	std::string Connect() {
		return ConnectFd(OpenLink());
	}

	std::string ConnectFd(int fd) {
		auto connected =
		    TwinRun(*con, "SELECT connection_id FROM n6k_connect(wsFd => " + std::to_string(fd) + ", token => '')");
		return connected->Collection().GetValue(0, 0).ToString();
	}

	void CloseHostFd() {
		auto &link = *links.back();
		::close(link.near_pair[0]);
		link.near_pair[0] = -1;
	}

	void DropSocket() {
		n6k::ShutdownSocket(links.back()->near_pair[1]);
	}

	bool AwaitHostEof() const {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!links.back()->near_gone.load() && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		return links.back()->near_gone.load();
	}

	void CloseDatabase() {
		con.reset();
		near_db.reset();
	}

	std::vector<int64_t> SentNs(n6k::FrameType type) {
		std::lock_guard<std::mutex> lk(lock);
		std::vector<int64_t> values;
		for (auto &frame : sent) {
			size_t offset = 0;
			auto handle = msgpack::unpack(frame.data(), frame.size(), offset);
			if (n6k::GetInt(handle.get(), "t", -1) == static_cast<int64_t>(type)) {
				values.push_back(n6k::GetInt(handle.get(), "ns", 0));
			}
		}
		return values;
	}

	void AttachAs(const std::string &alias, const std::string &catalog, const std::string &connection_id) {
		TwinRun(*con, "ATTACH '' AS " + alias + " (TYPE n6k, CONNECTION '" + connection_id + "'" +
		                  (alias == catalog ? "" : ", CATALOG '" + catalog + "'") + ")");
	}

	void Attach() {
		first_connection = Connect();
		for (auto &catalog : catalogs) {
			AttachAs(catalog, catalog, first_connection);
		}
	}

	const std::string &ConnectionId() const {
		return first_connection;
	}

	unique_ptr<QueryResult> Near(const std::string &sql) {
		return TwinRun(*con, sql);
	}

	void NearErrors(const std::string &sql, const std::string &message = "") {
		INFO(sql);
		auto result = con->Query(sql);
		REQUIRE(result->HasError());
		INFO(result->GetError());
		REQUIRE(result->GetError().find(message) != std::string::npos);
	}

	void NearIgnoringError(const std::string &sql) {
		con->Query(sql);
	}

	std::string NearError(const std::string &sql) {
		INFO(sql);
		auto result = con->Query(sql);
		REQUIRE(result->HasError());
		return result->GetError();
	}

	void NearInterrupted(const std::string &sql, const std::function<bool()> &started,
	                     const std::function<void()> &give_up) {
		INFO(sql);
		std::string error;
		auto type = ExceptionType::INVALID;
		bool failed = false;
		std::atomic<bool> done {false};
		std::thread runner([&] {
			auto result = con->Query(sql);
			failed = result->HasError();
			if (failed) {
				error = result->GetError();
				type = result->GetErrorType();
			}
			done.store(true);
		});
		const bool reached = started();
		con->Interrupt();
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
		while (!done.load() && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		const bool stopped_in_time = done.load();
		if (!stopped_in_time) {
			give_up();
		}
		runner.join();
		INFO(error);
		REQUIRE(reached);
		REQUIRE(stopped_in_time);
		REQUIRE(failed);
		REQUIRE(type == ExceptionType::INTERRUPT);
	}

	void NearFails(const std::string &sql, ExceptionType type, const std::string &message) {
		INFO(sql);
		auto result = con->Query(sql);
		REQUIRE(result->HasError());
		INFO(result->GetError());
		REQUIRE(result->GetErrorType() == type);
		REQUIRE(result->GetError().find(message) != std::string::npos);
	}

	void NearFunction(ScalarFunction function) {
		CreateScalarFunctionInfo info(std::move(function));
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		con->context->RegisterFunction(info);
	}

	std::vector<std::string> Rows(const std::string &sql) {
		return TwinRows(*con, sql);
	}

	std::string Explain(const std::string &sql) {
		auto result = Near("EXPLAIN " + sql);
		std::string plan;
		for (idx_t row = 0; row < result->RowCount(); row++) {
			plan += result->Collection().GetValue(1, row).ToString() + "\n";
		}
		return plan;
	}

	void Crosses(const std::string &sql) {
		INFO(sql);
		const auto plan = Explain(sql);
		INFO(plan);
		REQUIRE(plan.find("Crossing Remote") != std::string::npos);
	}

	void Folds(const std::string &sql) {
		INFO(sql);
		const auto plan = Explain(sql);
		INFO(plan);
		REQUIRE(plan.substr(0, plan.find('\n')).find("Crossing Remote") != std::string::npos);
	}

	void Same(const std::string &sql) {
		INFO(sql);
		auto expected = TwinRun(far_con, sql);
		auto got = TwinRun(*con, sql);

		REQUIRE(got->GetTypes() == expected->GetTypes());
		REQUIRE(Names(*got) == Names(*expected));
		REQUIRE(got->RowCount() == expected->RowCount());
		auto got_values = got->Collection().GetRows();
		auto expected_values = expected->Collection().GetRows();
		for (idx_t row = 0; row < expected->RowCount(); row++) {
			for (idx_t column = 0; column < expected->GetTypes().size(); column++) {
				const auto have = got_values.GetValue(column, row);
				const auto want = expected_values.GetValue(column, row);
				INFO("row " << row << " column " << column << ": have " << have.ToString() << " want "
				            << want.ToString());
				REQUIRE(ValueOperations::NotDistinctFrom(have, want));
			}
		}
	}

	TwinPlan LastPlan() {
		std::lock_guard<std::mutex> lk(lock);
		std::map<int64_t, std::string> partial;
		std::string last_plan;
		for (auto &frame : sent) {
			size_t offset = 0;
			auto handle = msgpack::unpack(frame.data(), frame.size(), offset);
			const auto &header = handle.get();
			if (n6k::GetInt(header, "t", -1) != static_cast<int64_t>(n6k::FrameType::STREAM_DATA) ||
			    n6k::GetInt(header, "stream", -1) != 0) {
				continue;
			}
			auto &assembled = partial[n6k::GetInt(header, "link", -1)];
			assembled.append(frame.data() + offset, frame.size() - offset);
			const auto *fin = n6k::FindKey(header, "fin");
			if (!fin || !fin->via.boolean) {
				continue;
			}
			// `[u32 header_len]PLAN\n<ends transaction>\n<streams>\n<slices>\n<plan>`, as crossing frames it.
			constexpr size_t LENGTH_BYTES = 4;
			if (assembled.compare(LENGTH_BYTES, 5, "PLAN\n") == 0) {
				auto plan_at = LENGTH_BYTES + 5;
				for (int line = 0; line < 3; line++) {
					plan_at = assembled.find('\n', plan_at) + 1;
				}
				last_plan = assembled.substr(plan_at);
			}
			assembled.clear();
		}
		if (last_plan.empty()) {
			throw std::runtime_error("no plan crossed the wire");
		}
		n6k::JsonDoc doc(last_plan);
		if (!doc.Parsed()) {
			throw std::runtime_error("a plan frame's plan is not JSON");
		}
		TwinPlan plan;
		Walk(doc.Root(), plan);
		return plan;
	}

	size_t SentCount(n6k::FrameType type) {
		std::lock_guard<std::mutex> lk(lock);
		size_t count = 0;
		for (auto &frame : sent) {
			size_t offset = 0;
			auto handle = msgpack::unpack(frame.data(), frame.size(), offset);
			if (n6k::GetInt(handle.get(), "t", -1) == static_cast<int64_t>(type)) {
				count++;
			}
		}
		return count;
	}

	std::string ServeError() {
		std::lock_guard<std::mutex> lk(lock);
		return serve_error;
	}

	void HoldCreditsUntilPause() {
		hold_credits.store(true);
	}

private:
	static std::vector<std::string> Names(QueryResult &result) {
		std::vector<std::string> names;
		for (auto &name : result.GetNames()) {
			names.push_back(name.GetIdentifierName());
		}
		return names;
	}

	static void SetNoSigPipe(int fd) {
#ifdef SO_NOSIGPIPE
		int on = 1;
		::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
	}

	static bool ReadExact(int fd, char *buffer, size_t count) {
		size_t got = 0;
		while (got < count) {
			const auto read = ::read(fd, buffer + got, count - got);
			if (read <= 0) {
				return false;
			}
			got += static_cast<size_t>(read);
		}
		return true;
	}

	static bool WriteAll(int fd, const char *buffer, size_t count) {
#ifdef MSG_NOSIGNAL
		const int flags = MSG_NOSIGNAL;
#else
		const int flags = 0;
#endif
		size_t sent_bytes = 0;
		while (sent_bytes < count) {
			const auto written = ::send(fd, buffer + sent_bytes, count - sent_bytes, flags);
			if (written <= 0) {
				return false;
			}
			sent_bytes += static_cast<size_t>(written);
		}
		return true;
	}

	static bool IsCredit(const std::string &frame) {
		size_t offset = 0;
		auto handle = msgpack::unpack(frame.data(), frame.size(), offset);
		return n6k::GetInt(handle.get(), "t", -1) == static_cast<int64_t>(n6k::FrameType::CREDIT);
	}

	void AwaitCreditPause() {
		Connection stats(far);
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
		while (std::chrono::steady_clock::now() < deadline) {
			auto result = stats.Query("SELECT credit_pauses FROM n6k_serve_stats()");
			if (result->HasError() || result->Collection().GetValue(0, 0).GetValue<int64_t>() >= 1) {
				return;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
	}

	void Pump(int from, int to, bool record) {
		char header[n6k::STREAM_HEADER_BYTES];
		while (ReadExact(from, header, sizeof(header))) {
			std::string payload(n6k::StreamPayloadLength(header), '\0');
			if (!payload.empty() && !ReadExact(from, &payload[0], payload.size())) {
				break;
			}
			if (record &&
			    static_cast<unsigned char>(header[4]) == static_cast<unsigned char>(n6k::StreamFrameType::N6K)) {
				{
					std::lock_guard<std::mutex> lk(lock);
					sent.push_back(payload);
				}
				if (hold_credits.load() && IsCredit(payload)) {
					AwaitCreditPause();
					hold_credits.store(false);
				}
			}
			if (!WriteAll(to, header, sizeof(header)) || !WriteAll(to, payload.data(), payload.size())) {
				break;
			}
		}
		n6k::ShutdownSocket(from);
		n6k::ShutdownSocket(to);
	}

	static void Walk(duckdb_yyjson::yyjson_val *value, TwinPlan &plan) {
		if (duckdb_yyjson::yyjson_is_arr(value)) {
			size_t idx, max;
			duckdb_yyjson::yyjson_val *item;
			yyjson_arr_foreach(value, idx, max, item) {
				Walk(item, plan);
			}
			return;
		}
		if (!duckdb_yyjson::yyjson_is_obj(value)) {
			return;
		}
		size_t idx, max;
		duckdb_yyjson::yyjson_val *key, *child;
		yyjson_obj_foreach(value, idx, max, key, child) {
			const std::string name = duckdb_yyjson::yyjson_get_str(key);
			if (duckdb_yyjson::yyjson_is_obj(child) && duckdb_yyjson::yyjson_obj_get(child, "input")) {
				plan.rels.insert(name);
			}
			if (name == "read" && duckdb_yyjson::yyjson_is_obj(child)) {
				plan.rels.insert(name);
				auto *named = duckdb_yyjson::yyjson_obj_get(child, "namedTable");
				const auto names = n6k::JsonGetStrArray(named, "names");
				if (names.size() >= 2) {
					plan.tables.push_back(names.back());
				}
			}
			Walk(child, plan);
		}
	}

	struct TwinLink {
		int near_pair[2] = {-1, -1};
		int far_pair[2] = {-1, -1};
		std::thread to_far;
		std::thread to_near;
		std::thread serve;
		unique_ptr<Connection> serve_con;
		std::atomic<bool> near_gone {false};
	};

	DuckDB &far;
	Connection &far_con;
	std::vector<std::string> catalogs;

	unique_ptr<DuckDB> near_db = make_uniq<DuckDB>(nullptr);
	unique_ptr<Connection> con;
	std::vector<unique_ptr<TwinLink>> links;
	std::string first_connection;

	std::mutex lock;
	std::vector<std::string> sent;
	std::string serve_error;
	std::atomic<bool> hold_credits {false};
};

class TwinNear {
public:
	TwinNear() {
		ExtensionLoader loader(*db.instance, "n6k_client");
		LoadInternal(loader);
	}

	std::vector<std::string> Rows(const std::string &sql) {
		return TwinRows(con, sql);
	}

	std::string Connect(int fd, const std::string &args = ", token => \'\'") {
		return Rows("SELECT connection_id FROM n6k_connect(wsFd => " + std::to_string(fd) + args + ")").at(0);
	}

	void Attach(const std::string &alias, const std::string &connection_id) {
		Rows("ATTACH \'\' AS " + alias + " (TYPE n6k, CONNECTION \'" + connection_id + "\')");
	}

	void Fails(const std::string &sql, ExceptionType type, const std::string &message) {
		INFO(sql);
		auto result = con.Query(sql);
		REQUIRE(result->HasError());
		INFO(result->GetError());
		REQUIRE(result->GetErrorType() == type);
		REQUIRE(result->GetError().find(message) != std::string::npos);
	}

private:
	DuckDB db {nullptr};
	Connection con {db};
};

class N6kTwin {
public:
	explicit N6kTwin(std::vector<std::string> catalogs_p = {"db"})
	    : far(nullptr), far_con(far), catalogs(std::move(catalogs_p)) {
		ExtensionLoader far_loader(*far.instance, "n6k_server");
		N6kServerLoadInternal(far_loader);
		ExtensionLoader testing_loader(*far.instance, "n6k_testing");
		N6kTestingLoadInternal(testing_loader);
		for (auto &catalog : catalogs) {
			TwinRun(far_con, "ATTACH ':memory:' AS " + catalog);
		}
		auto counter = ticks;
		ScalarFunction far_tick("far_tick", {LogicalType::BIGINT}, LogicalType::BIGINT,
		                        [counter](DataChunk &args, ExpressionState &, Vector &result) {
			                        counter->Add(args.size());
			                        UnaryExecutor::Execute<int64_t, int64_t>(args.data[0], result,
			                                                                 [](int64_t value) { return value; });
		                        });
		CreateScalarFunctionInfo info(std::move(far_tick));
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		far_con.context->RegisterFunction(info);
	}

	~N6kTwin() {
		ticks->Stop();
	}

	N6kTwin(const N6kTwin &) = delete;
	N6kTwin &operator=(const N6kTwin &) = delete;

	void Far(const std::string &sql) {
		TwinRun(far_con, sql);
	}

	std::vector<std::string> FarRows(const std::string &sql) {
		return TwinRows(far_con, sql);
	}

	void FarFunction(ScalarFunction function) {
		CreateScalarFunctionInfo info(std::move(function));
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		far_con.context->RegisterFunction(info);
	}

	TwinClient &Client() {
		clients.push_back(make_uniq<TwinClient>(far, far_con, catalogs));
		clients.back()->Attach();
		return *clients.back();
	}

	void Attach() {
		Client();
	}

	TwinClient &BareClient() {
		clients.push_back(make_uniq<TwinClient>(far, far_con, catalogs));
		return *clients.back();
	}

	unique_ptr<QueryResult> Near(const std::string &sql) {
		return First().Near(sql);
	}
	void NearFails(const std::string &sql, ExceptionType type, const std::string &message) {
		First().NearFails(sql, type, message);
	}
	void NearErrors(const std::string &sql, const std::string &message = "") {
		First().NearErrors(sql, message);
	}
	void NearIgnoringError(const std::string &sql) {
		First().NearIgnoringError(sql);
	}
	std::string NearError(const std::string &sql) {
		return First().NearError(sql);
	}
	void NearInterrupted(const std::string &sql) {
		First().NearInterrupted(
		    sql, [this] { return ticks->AwaitStarted(); }, [this] { ticks->Stop(); });
	}
	bool FarQuiet() const {
		return ticks->AwaitQuiet();
	}
	std::string Connect() {
		return First().Connect();
	}
	void AttachAs(const std::string &alias, const std::string &catalog, const std::string &connection_id) {
		First().AttachAs(alias, catalog, connection_id);
	}
	const std::string &ConnectionId() {
		return First().ConnectionId();
	}
	void NearFunction(ScalarFunction function) {
		First().NearFunction(std::move(function));
	}
	std::vector<std::string> Rows(const std::string &sql) {
		return First().Rows(sql);
	}
	std::string Explain(const std::string &sql) {
		return First().Explain(sql);
	}
	void Crosses(const std::string &sql) {
		First().Crosses(sql);
	}
	void Folds(const std::string &sql) {
		First().Folds(sql);
	}
	void Same(const std::string &sql) {
		First().Same(sql);
	}
	TwinPlan LastPlan() {
		return First().LastPlan();
	}
	size_t SentCount(n6k::FrameType type) {
		return First().SentCount(type);
	}
	std::string ServeError() {
		return First().ServeError();
	}
	void HoldCreditsUntilPause() {
		First().HoldCreditsUntilPause();
	}

private:
	TwinClient &First() {
		return *clients.front();
	}

	DuckDB far;
	Connection far_con;
	std::vector<std::string> catalogs;
	std::shared_ptr<TwinTicks> ticks = std::make_shared<TwinTicks>();
	std::vector<unique_ptr<TwinClient>> clients;
};

} // namespace duckdb
