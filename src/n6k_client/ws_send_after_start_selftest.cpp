#include "ws_send_after_start_selftest.hpp"
#include "duckdb/function/table_function.hpp"

#include <chrono>
#include <cstdint>
#include <string>

#ifndef WASM_LOADABLE_EXTENSIONS
#include "ws_client.hpp"
#include "ws_reactor.hpp"

#include <atomic>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketMessage.h>
#include <ixwebsocket/IXWebSocketMessageType.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <memory>
#include <thread>
#endif

namespace duckdb {

namespace {

struct SendAfterStartResult {
	std::string mode;
	bool ok = false;
	bool listening = false;
	bool probe_received = false; // THE assertion: the post-Start frame reached the wire
	int32_t port = 0;
	int64_t elapsed_ms = 0;
};

#ifndef WASM_LOADABLE_EXTENSIONS

// Payload chosen so a stray n6k frame can never be mistaken for the probe.
const char *const kProbe = "N6K-SEND-AFTER-START-PROBE";

// How long the probe gets to land. Generously above a loopback connect+handshake (sub-millisecond
// in practice) so a slow CI box can't produce a false failure; a dropped frame never arrives at all,
// so the bug does not depend on this bound.
constexpr int kProbeWaitMs = 3000;

// ix::SocketServer::getPort() reports the requested port verbatim and is never rewritten after bind,
// so port 0 would hand back 0 and leave us with no URL to dial. Pick candidates from a base derived
// from the calling thread id (so concurrent runs are unlikely to collide) and let listen() tell us
// which one is actually free.
int CandidateBasePort() {
	const auto tid = std::hash<std::thread::id> {}(std::this_thread::get_id());
	return 39000 + static_cast<int>(tid % 2000);
}

SendAfterStartResult RunSendAfterStartSelftest() {
	SendAfterStartResult r;
	r.mode = "native";
	const auto t0 = std::chrono::steady_clock::now();
	auto stamp = [&]() {
		r.elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	};

	std::atomic<bool> saw_probe {false};

	std::unique_ptr<ix::WebSocketServer> server;
	const int base = CandidateBasePort();
	for (int attempt = 0; attempt < 16 && !r.listening; ++attempt) {
		const int port = base + attempt;
		std::unique_ptr<ix::WebSocketServer> candidate(new ix::WebSocketServer(port, "127.0.0.1"));
		// Match the client, which disables deflate in IxWebSocketTransport::Start.
		candidate->disablePerMessageDeflate();
		candidate->setOnClientMessageCallback([&saw_probe](const std::shared_ptr<ix::ConnectionState> &,
		                                                   ix::WebSocket &, const ix::WebSocketMessagePtr &msg) {
			if (msg->type == ix::WebSocketMessageType::Message && msg->str == kProbe) {
				saw_probe.store(true, std::memory_order_release);
			}
		});
		if (!candidate->listen().first) {
			continue;
		}
		candidate->start();
		server = std::move(candidate);
		r.listening = true;
		r.port = port;
	}
	if (!r.listening) {
		stamp();
		return r;
	}

	n6k::WsClientOptions opts;
	opts.url = "ws://127.0.0.1:" + std::to_string(r.port) + "/ws";

	try {
		auto reactor = n6k::CreateReactor(opts);
		// Callbacks are no-ops: this test asserts only on the OUTBOUND direction.
		reactor->Start([](const std::string &, bool) {}, [](const std::string &) {});
		// The racing enqueue — the same position HELLO occupies in WsClient::StartReactorAndHandshake.
		reactor->Send(kProbe);

		const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kProbeWaitMs);
		while (!saw_probe.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		r.probe_received = saw_probe.load(std::memory_order_acquire);
		reactor->Stop();
	} catch (...) {
	}

	server->stop();
	r.ok = r.listening && r.probe_received;
	stamp();
	return r;
}

#else

SendAfterStartResult RunSendAfterStartSelftest() {
	SendAfterStartResult r;
	r.mode = "wasm_no_ix";
	r.ok = true;
	return r;
}

#endif

struct N6kWsSendAfterStartBind : public TableFunctionData {};

struct N6kWsSendAfterStartState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"mode", "ok", "listening", "probe_received", "port", "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::BOOLEAN, LogicalType::INTEGER, LogicalType::INTEGER};
	return make_uniq<N6kWsSendAfterStartBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kWsSendAfterStartState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kWsSendAfterStartState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	const SendAfterStartResult r = RunSendAfterStartSelftest();
	output.SetValue(0, 0, Value(r.mode));
	output.SetValue(1, 0, Value::BOOLEAN(r.ok));
	output.SetValue(2, 0, Value::BOOLEAN(r.listening));
	output.SetValue(3, 0, Value::BOOLEAN(r.probe_received));
	output.SetValue(4, 0, Value::INTEGER(r.port));
	output.SetValue(5, 0, Value::INTEGER(static_cast<int32_t>(r.elapsed_ms)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kSelftestSendAfterStart(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_send_after_start", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
