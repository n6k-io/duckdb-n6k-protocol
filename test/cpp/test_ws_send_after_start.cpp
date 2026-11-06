#include "catch.hpp"
#include "connection_hub.hpp"
#include "framework/n6k_peer.hpp"

#include <atomic>
#include <chrono>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketMessage.h>
#include <ixwebsocket/IXWebSocketMessageType.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <memory>
#include <string>
#include <thread>

using namespace duckdb;

namespace {

struct SendAfterStartResult {
	std::string error;
	bool listening = false;
	bool probe_received = false;
};

// Payload chosen so a stray n6k frame can never be mistaken for the probe.
const char *const kProbe = "N6K-SEND-AFTER-START-PROBE";

// Generously above a loopback connect and handshake. A dropped frame never arrives at all, so the bug does not
// depend on this bound.
constexpr int kProbeWaitMs = 3000;

// ix::SocketServer::getPort() reports the requested port verbatim, so port 0 would leave no URL to dial.
int CandidateBasePort() {
	const auto tid = std::hash<std::thread::id> {}(std::this_thread::get_id());
	return 39000 + static_cast<int>(tid % 2000);
}

SendAfterStartResult RunSendAfterStart() {
	SendAfterStartResult r;
	std::atomic<bool> saw_probe {false};
	int bound_port = 0;

	std::unique_ptr<ix::WebSocketServer> server;
	const int base = CandidateBasePort();
	for (int attempt = 0; attempt < 16 && !r.listening; ++attempt) {
		const int port = base + attempt;
		std::unique_ptr<ix::WebSocketServer> candidate(new ix::WebSocketServer(port, "127.0.0.1"));
		// Match the client, which disables deflate in IxWebSocketTransport::Start.
		candidate->disablePerMessageDeflate();
		candidate->setOnClientMessageCallback([&saw_probe](const std::shared_ptr<ix::ConnectionState> &,
		                                                   ix::WebSocket &ws, const ix::WebSocketMessagePtr &msg) {
			if (msg->type != ix::WebSocketMessageType::Message) {
				return;
			}
			if (msg->str == kProbe) {
				saw_probe.store(true, std::memory_order_release);
			} else {
				ws.sendBinary(n6k_test::PackHelloAck());
			}
		});
		if (!candidate->listen().first) {
			continue;
		}
		candidate->start();
		server = std::move(candidate);
		r.listening = true;
		bound_port = port;
	}
	if (!r.listening) {
		return r;
	}

	try {
		auto hub = n6k::ConnectionHub::Create(
		    nullptr, n6k::WebSocketOptions("ws://127.0.0.1:" + std::to_string(bound_port) + "/ws", /*token=*/""));
		// Returns only once the HELLO the hub queued on Open has been answered.
		hub->EnsureConnected(nullptr);
		hub->Send(kProbe);

		const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kProbeWaitMs);
		while (!saw_probe.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		r.probe_received = saw_probe.load(std::memory_order_acquire);
		hub->Disconnect("test done");
	} catch (const std::exception &e) {
		r.error = e.what();
	}

	server->stop();
	return r;
}

} // namespace

TEST_CASE("the frame after a dialed HELLO reaches the wire", "[transport]") {
	const auto r = RunSendAfterStart();
	INFO(r.error);

	REQUIRE(r.listening);
	REQUIRE(r.error.empty());
	REQUIRE(r.probe_received);
}
