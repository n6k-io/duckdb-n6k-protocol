#include "n6k_serve_http_function.hpp"

#include "n6k_served.hpp"
#include "serve_bind_common.hpp"
#include "serve_clients.hpp"
#include "serve_options.hpp"
#include "ws_json.hpp"
#include "ws_serve_connection.hpp"

#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "ixwebsocket/IXHttp.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace duckdb {

static constexpr const char *FN_NAME = "n6k_serve_http";
static constexpr const char *WS_FN_NAME = "n6k_serve_ws";
static constexpr int POLL_INTERVAL_MS = 250;

namespace {

struct N6kServeHttpBindData : public TableFunctionData {
	vector<string> catalogs;
	string host;
	int32_t port = 0;
	size_t max_connections = ix::SocketServer::kDefaultMaxConnections;
	n6k::ServeOptions options;
};

struct N6kServeWsBindData : public TableFunctionData {
	vector<string> catalogs;
	hugeint_t session_id;
	ix::HttpRequestPtr request;
	n6k::ServeOptions options;
};

struct N6kServeHttpState : public GlobalTableFunctionState {
	bool done = false;
};

} // namespace

static void N6kServeHttpReturnSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"connections", "requests_handled", "url"};
	return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR};
}

static unique_ptr<FunctionData> N6kServeHttpBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	N6kServeHttpReturnSchema(return_types, names);
	n6k::ThrowIfServed(context, FN_NAME);

	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("%s requires a host and a port: CALL %s('127.0.0.1', 7823)", FN_NAME, FN_NAME);
	}
	auto host = input.inputs[0].GetValue<string>();
	if (host.empty()) {
		throw BinderException("%s: host must not be empty", FN_NAME);
	}
	auto port = input.inputs[1].GetValue<int32_t>();
	if (port < 1 || port > 65535) {
		throw BinderException("%s: port %d is out of range (1-65535)", FN_NAME, port);
	}

	auto result = make_uniq<N6kServeHttpBindData>();
	auto max_connections = input.named_parameters.find("max_connections");
	if (max_connections != input.named_parameters.end() && !max_connections->second.IsNull()) {
		auto n = max_connections->second.GetValue<int64_t>();
		if (n < 1) {
			throw BinderException("%s: max_connections must be positive", FN_NAME);
		}
		result->max_connections = static_cast<size_t>(n);
	}
	result->catalogs = n6k::ResolveServedCatalogs(context, input.inputs, 2, FN_NAME);
	result->host = std::move(host);
	result->port = port;
	result->options = n6k::BindServeOptions(input, FN_NAME);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> N6kServeHttpInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_uniq<N6kServeHttpState>();
}

static void N6kServeHttpScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kServeHttpBindData>();
	auto &state = data_p.global_state->Cast<N6kServeHttpState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	// Every client shares this one DatabaseInstance, and so one catalog set: concurrent writers meet
	// DuckDB's MVCC, and the reactor reports transaction conflicts as retriable.
	auto &db = DatabaseInstance::GetDatabase(context);

	std::atomic<int64_t> handled {0};
	std::atomic<int64_t> connections {0};
	// Declared before `server`, so on unwind the server stops before its clients are joined.
	n6k::ServeClients<n6k::WsServeTransport> clients;

	n6k::N6kWsServer server(
	    bind_data.port, bind_data.host,
	    [&](int fd, std::shared_ptr<n6k::WsServeTransport> transport, std::string request) {
		    connections.fetch_add(1);
		    clients.Spawn(std::move(transport), [&db, &context, &handled, &bind_data, fd,
		                                         request = std::move(request)](n6k::WsServeTransport &conn) {
			    try {
				    auto session_id = n6k::CreateServeSession(db, fd);
				    n6k::ServeSessionClaim session(db, session_id);
				    handled.fetch_add(n6k::ServeWsConnection(conn, db, context, bind_data.catalogs, bind_data.options,
				                                             session_id, request)
				                          .requests_handled);
			    } catch (const std::exception &e) {
				    // `run` must not throw (ServeClients::Spawn); log so a failed reactor is visible.
				    DUCKDB_LOG_WARNING(context, "n6k_serve_http: connection reactor failed: %s", e.what());
			    }
		    });
	    },
	    bind_data.max_connections);

	if (!server.listenAndStart()) {
		throw IOException("%s: could not listen on %s:%d", FN_NAME, bind_data.host, bind_data.port);
	}

	// ixwebsocket owns the accept loop on its own threads, so this one only has to stay alive and
	// watch for Ctrl-C — the same interrupt contract as the Unix-socket form's poll timeout.
	while (!context.IsInterrupted()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
		clients.ReapFinished();
	}

	// Stop accepting first, then tell whatever is still connected that its input has ended, so
	// each reactor unwinds on its own and drains its in-flight workers before we join it.
	server.stop();
	clients.CloseAndJoin();

	// Declared, but not reachable in practice: the only way out of the loop above is the
	// interrupt, and DuckDB aborts an interrupted statement rather than materialising its
	// result. A table function must still declare a schema, and this is the honest one.
	output.SetValue(0, 0, Value::BIGINT(connections.load()));
	output.SetValue(1, 0, Value::BIGINT(handled.load()));
	output.SetValue(2, 0, Value("ws://" + bind_data.host + ":" + std::to_string(bind_data.port)));

	state.done = true;
	output.SetCardinality(1);
}

// Missing or non-string fields read as "", non-string header values are dropped, and a non-object "headers" is ignored.
static ix::HttpRequestPtr BindUpgradeRequest(const string &json) {
	n6k::JsonDoc doc(json);
	auto *root = doc.Root();
	if (!root || !duckdb_yyjson::yyjson_is_obj(root)) {
		throw BinderException("%s: request must be a JSON object {method, uri, version, headers}", WS_FN_NAME);
	}
	ix::WebSocketHttpHeaders headers;
	auto *members = duckdb_yyjson::yyjson_obj_get(root, "headers");
	if (members && duckdb_yyjson::yyjson_is_obj(members)) {
		duckdb_yyjson::yyjson_obj_iter it;
		duckdb_yyjson::yyjson_obj_iter_init(members, &it);
		while (auto *key = duckdb_yyjson::yyjson_obj_iter_next(&it)) {
			auto *value = duckdb_yyjson::yyjson_obj_iter_get_val(key);
			if (duckdb_yyjson::yyjson_is_str(value)) {
				headers[duckdb_yyjson::yyjson_get_str(key)] = duckdb_yyjson::yyjson_get_str(value);
			}
		}
	}
	return std::make_shared<ix::HttpRequest>(n6k::JsonGetStr(root, "uri"), n6k::JsonGetStr(root, "method"),
	                                         n6k::JsonGetStr(root, "version"), "", headers);
}

static unique_ptr<FunctionData> N6kServeWsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names = {"connected",  "requests_handled", "frames_in",   "bytes_in",
	         "frames_out", "bytes_out",        "duration_ms", "authenticate_ms"};
	return_types = vector<LogicalType>(names.size(), LogicalType::BIGINT);
	return_types[0] = LogicalType::BOOLEAN;
	n6k::ThrowIfServed(context, WS_FN_NAME);
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("%s requires a session id from n6k_serve_create_session and its upgrade request",
		                      WS_FN_NAME);
	}
	auto result = make_uniq<N6kServeWsBindData>();
	result->session_id = input.inputs[0].GetValue<hugeint_t>();
	result->request = BindUpgradeRequest(input.inputs[1].GetValue<string>());
	result->catalogs = n6k::ResolveServedCatalogs(context, input.inputs, 2, WS_FN_NAME);
	result->options = n6k::BindServeOptions(input, WS_FN_NAME);
	return std::move(result);
}

// The reactor runs on this CALL's thread, ix's read loop on its own; the scan returns once the WebSocket closes
// or the upgrade fails.
static void N6kServeWsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kServeWsBindData>();
	auto &state = data_p.global_state->Cast<N6kServeHttpState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	n6k::ServeSessionClaim session(db, bind_data.session_id);

	std::mutex mu;
	std::condition_variable cv;
	std::shared_ptr<n6k::WsServeTransport> transport;
	string request;
	bool upgrade_over = false;
	n6k::N6kWsServer server(0, "", [&](int, std::shared_ptr<n6k::WsServeTransport> opened, std::string opened_request) {
		{
			std::lock_guard<std::mutex> lk(mu);
			transport = std::move(opened);
			request = std::move(opened_request);
		}
		cv.notify_all();
	});
	std::thread adopted([&] {
		server.Adopt(session.Fd(), bind_data.request);
		{
			std::lock_guard<std::mutex> lk(mu);
			upgrade_over = true;
		}
		cv.notify_all();
	});
	{
		std::unique_lock<std::mutex> lk(mu);
		cv.wait(lk, [&] { return transport || upgrade_over; });
	}

	n6k::SessionStats stats {};
	if (transport) {
		try {
			stats = n6k::ServeWsConnection(*transport, db, context, bind_data.catalogs, bind_data.options,
			                               bind_data.session_id, request);
		} catch (...) {
			transport->ShutdownWrite();
			adopted.join();
			throw;
		}
		// Run() returns without ShutdownWrite when ReadHello throws.
		transport->ShutdownWrite();
	}
	adopted.join();

	output.SetValue(0, 0, Value::BOOLEAN(transport != nullptr));
	const vector<int64_t> values {stats.requests_handled, stats.frames_in,   stats.bytes_in,       stats.frames_out,
	                              stats.bytes_out,        stats.duration_ms, stats.authenticate_ms};
	for (idx_t i = 0; i < values.size(); i++) {
		output.SetValue(i + 1, 0, Value::BIGINT(values[i]));
	}
	state.done = true;
	output.SetCardinality(1);
}

void RegisterN6kServeHttpFunction(ExtensionLoader &loader) {
	TableFunction func(FN_NAME, {LogicalType::VARCHAR, LogicalType::INTEGER}, N6kServeHttpScan, N6kServeHttpBind,
	                   N6kServeHttpInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	n6k::AddServeOptionParameters(func);
	func.named_parameters["max_connections"] = LogicalType::BIGINT;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

void RegisterN6kServeWsFunction(ExtensionLoader &loader) {
	TableFunction func(WS_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServeWsScan, N6kServeWsBind,
	                   N6kServeHttpInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	n6k::AddServeOptionParameters(func);
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
