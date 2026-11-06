#pragma once

#include "catch.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_server_extension.hpp"
#include "n6k_stream_framing.hpp"
#include "n6k_table_function.hpp"
#include "ws_json.hpp"
#include "ws_transport_raw.hpp"

#include "duckdb.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <algorithm>
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

class N6kTwin {
public:
	N6kTwin() : far(nullptr), far_con(far) {
		ExtensionLoader far_loader(*far.instance, "n6k_server");
		N6kServerLoadInternal(far_loader);
		ExtensionLoader near_loader(*near_db->instance, "n6k_client");
		LoadInternal(near_loader);
		Run(far_con, "ATTACH ':memory:' AS db");
		if (::socketpair(AF_UNIX, SOCK_STREAM, 0, near_pair) != 0 ||
		    ::socketpair(AF_UNIX, SOCK_STREAM, 0, far_pair) != 0) {
			throw std::runtime_error("socketpair failed");
		}
		SetNoSigPipe(near_pair[1]);
		SetNoSigPipe(far_pair[0]);
	}

	~N6kTwin() {
		con.reset();
		near_db.reset();
		for (int fd : {near_pair[0], near_pair[1], far_pair[0], far_pair[1]}) {
			n6k::ShutdownSocket(fd);
		}
		for (auto *thread : {&to_far, &to_near, &serve}) {
			if (thread->joinable()) {
				thread->join();
			}
		}
		for (int fd : {near_pair[0], near_pair[1], far_pair[0], far_pair[1]}) {
			::close(fd);
		}
	}

	N6kTwin(const N6kTwin &) = delete;
	N6kTwin &operator=(const N6kTwin &) = delete;

	void Far(const std::string &sql) {
		Run(far_con, sql);
	}

	void Attach() {
		to_far = std::thread([this] { Pump(near_pair[1], far_pair[0], true); });
		to_near = std::thread([this] { Pump(far_pair[0], near_pair[1], false); });

		auto session =
		    Run(far_con, "SELECT session_id FROM n6k_serve_create_session(" + std::to_string(far_pair[1]) + ")");
		const auto session_id = session->Collection().GetValue(0, 0).ToString();
		serve_con = make_uniq<Connection>(far);
		serve = std::thread([this, session_id] {
			auto result = serve_con->Query("CALL n6k_serve_fd('" + session_id + "'::UUID, 'db')");
			if (result->HasError()) {
				std::lock_guard<std::mutex> lk(lock);
				serve_error = result->GetError();
			}
		});

		con = make_uniq<Connection>(*near_db);
		auto connected = Run(*con, "SELECT connection_id FROM n6k_connect(wsFd => " + std::to_string(near_pair[0]) +
		                               ", token => '')");
		Run(*con, "ATTACH '' AS db (TYPE n6k, CONNECTION '" + connected->Collection().GetValue(0, 0).ToString() + "')");
	}

	void Same(const std::string &sql) {
		INFO(sql);
		auto expected = Run(far_con, sql);
		auto got = Run(*con, sql);

		REQUIRE(got->GetTypes() == expected->GetTypes());
		REQUIRE(Names(*got) == Names(*expected));
		REQUIRE(got->RowCount() == expected->RowCount());
		for (idx_t row = 0; row < expected->RowCount(); row++) {
			for (idx_t column = 0; column < expected->GetTypes().size(); column++) {
				const auto have = got->Collection().GetValue(column, row);
				const auto want = expected->Collection().GetValue(column, row);
				INFO("row " << row << " column " << column << ": have " << have.ToString() << " want "
				            << want.ToString());
				REQUIRE(ValueOperations::NotDistinctFrom(have, want));
			}
		}
	}

	TwinPlan LastPlan() {
		std::lock_guard<std::mutex> lk(lock);
		for (auto frame = sent.rbegin(); frame != sent.rend(); ++frame) {
			size_t offset = 0;
			auto handle = msgpack::unpack(frame->data(), frame->size(), offset);
			const auto &header = handle.get();
			const auto op = n6k::GetInt(header, "op", -1);
			if (n6k::GetInt(header, "t", -1) != static_cast<int64_t>(n6k::FrameType::REQ) ||
			    (op != n6k::OP_READ_PLAN && op != n6k::OP_WRITE_PLAN)) {
				continue;
			}
			n6k::JsonDoc doc(n6k::GetStr(header, "plan"));
			if (!doc.Parsed()) {
				throw std::runtime_error("a plan frame's plan is not JSON");
			}
			TwinPlan plan;
			Walk(doc.Root(), plan);
			return plan;
		}
		throw std::runtime_error("no plan crossed the wire");
	}

	std::string ServeError() {
		std::lock_guard<std::mutex> lk(lock);
		return serve_error;
	}

private:
	static unique_ptr<QueryResult> Run(Connection &connection, const std::string &sql) {
		unique_ptr<QueryResult> result = connection.Query(sql);
		if (result->HasError()) {
			throw std::runtime_error(sql + "\n" + result->GetError());
		}
		return result;
	}

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

	void Pump(int from, int to, bool record) {
		char header[n6k::STREAM_HEADER_BYTES];
		while (ReadExact(from, header, sizeof(header))) {
			std::string payload(n6k::StreamPayloadLength(header), '\0');
			if (!payload.empty() && !ReadExact(from, &payload[0], payload.size())) {
				break;
			}
			if (record &&
			    static_cast<unsigned char>(header[4]) == static_cast<unsigned char>(n6k::StreamFrameType::N6K)) {
				std::lock_guard<std::mutex> lk(lock);
				sent.push_back(payload);
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

	DuckDB far;
	Connection far_con;
	unique_ptr<DuckDB> near_db = make_uniq<DuckDB>(nullptr);
	unique_ptr<Connection> con;
	unique_ptr<Connection> serve_con;

	int near_pair[2] = {-1, -1};
	int far_pair[2] = {-1, -1};
	std::thread to_far;
	std::thread to_near;
	std::thread serve;

	std::mutex lock;
	std::vector<std::string> sent;
	std::string serve_error;
};

} // namespace duckdb
