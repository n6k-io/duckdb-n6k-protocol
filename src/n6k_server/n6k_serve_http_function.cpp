#include "n6k_serve_http_function.hpp"

#include "n6k_served.hpp"
#include "serve_bind_common.hpp"
#include "serve_clients.hpp"
#include "serve_reactor.hpp"
#include "ws_serve_transport.hpp"

#include "duckdb.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "ixwebsocket/IXConnectionState.h"
#include "ixwebsocket/IXWebSocket.h"
#include "ixwebsocket/IXWebSocketServer.h"

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

namespace duckdb {

static constexpr const char *FN_NAME = "n6k_serve_http";
static constexpr int POLL_INTERVAL_MS = 250;

namespace {

struct N6kServeHttpBindData : public TableFunctionData {
	vector<string> catalogs;
	string host;
	int32_t port = 0;
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
	result->catalogs = n6k::ResolveServedCatalogs(context, input.inputs, 2, FN_NAME);
	result->host = std::move(host);
	result->port = port;
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

	std::mutex mu;
	// Keyed by WebSocket address rather than ConnectionState id: ixwebsocket hands the same
	// WebSocket& to every callback for a connection, so it is a stable key for its lifetime.
	std::map<ix::WebSocket *, std::shared_ptr<n6k::WsServeTransport>> live;
	std::atomic<int64_t> handled {0};
	std::atomic<int64_t> connections {0};
	// Declared before `server`, so on unwind the server stops before its clients are joined.
	n6k::ServeClients<n6k::WsServeTransport> clients;

	ix::WebSocketServer server(bind_data.port, bind_data.host);
	// n6k frames are msgpack and Arrow IPC — already compact, and deflating each one would only
	// add CPU per message.
	server.disablePerMessageDeflate();

	server.setOnClientMessageCallback(
	    [&](const std::shared_ptr<ix::ConnectionState> &, ix::WebSocket &ws, const ix::WebSocketMessagePtr &msg) {
		    switch (msg->type) {
		    case ix::WebSocketMessageType::Open: {
			    auto transport = std::make_shared<n6k::WsServeTransport>(ws);
			    connections.fetch_add(1);
			    {
				    std::lock_guard<std::mutex> lk(mu);
				    live[&ws] = transport;
			    }

			    // Everything captured by reference outlives it: `clients` joins every worker before the scan
			    // returns.
			    const auto *catalogs = &bind_data.catalogs;
			    clients.Spawn(transport, [&db, &context, &handled, catalogs](n6k::WsServeTransport &conn) {
				    try {
					    n6k::ServeReactor reactor(conn, db, context, *catalogs, false);
					    handled.fetch_add(reactor.Run());
				    } catch (const std::exception &e) {
					    // One client's reactor dying must not take the listener down, but it must not
					    // look like a clean disconnect either -- without this the connection just goes
					    // quiet and its requests vanish from the count.
					    DUCKDB_LOG_WARNING(context, "n6k_serve_http: connection reactor failed: %s", e.what());
				    }
			    });
			    break;
		    }
		    case ix::WebSocketMessageType::Message: {
			    std::shared_ptr<n6k::WsServeTransport> transport;
			    {
				    std::lock_guard<std::mutex> lk(mu);
				    auto it = live.find(&ws);
				    if (it != live.end()) {
					    transport = it->second;
				    }
			    }
			    if (transport) {
				    transport->EnqueueFrameUnlessClosed(
				        msg->binary ? n6k::StreamFrameType::N6K : n6k::StreamFrameType::JSONRPC, msg->str);
			    }
			    break;
		    }
		    case ix::WebSocketMessageType::Close:
		    case ix::WebSocketMessageType::Error: {
			    std::shared_ptr<n6k::WsServeTransport> transport;
			    {
				    std::lock_guard<std::mutex> lk(mu);
				    auto it = live.find(&ws);
				    if (it != live.end()) {
					    transport = it->second;
					    live.erase(it);
				    }
			    }
			    if (transport) {
				    transport->CloseInbound();
			    }
			    break;
		    }
		    default:
			    break;
		    }
	    });

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
	{
		std::lock_guard<std::mutex> lk(mu);
		live.clear();
	}
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

void RegisterN6kServeHttpFunction(ExtensionLoader &loader) {
	TableFunction func(FN_NAME, {LogicalType::VARCHAR, LogicalType::INTEGER}, N6kServeHttpScan, N6kServeHttpBind,
	                   N6kServeHttpInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
