#include "catch.hpp"
#include "connection_hub.hpp"
#include "duckdb/common/exception.hpp"
#include "hub_session.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "ws_client.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketMessage.h>
#include <ixwebsocket/IXWebSocketMessageType.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

namespace {

constexpr int kConnectTimeoutMs = 300;
constexpr int kStallMs = 600;
constexpr int kBeatMs = 150;
constexpr int kPendingForMs = 1000;
constexpr const char *kStatus = "warming";

struct OpenPendingResult {
	bool connected = false;
	std::string error;
	int64_t elapsed_ms = 0;
	bool interrupted = false;
	bool cancel_seen = false;
	int32_t opens = 0;
	int32_t aborts_after_connect = 0;
	int32_t request_polls = 0;
	std::vector<int64_t> open_ns;
	// (OPENs seen before it, ns)
	std::vector<std::pair<int32_t, int64_t>> cancels;
};

std::string PackHelloAck() {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(3);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::HELLO_ACK));
	n6k::PackString(pk, "protocol_version");
	pk.pack(n6k::N6K_PROTOCOL_VERSION);
	n6k::PackString(pk, "capabilities");
	pk.pack_array(0);
	return std::string(sbuf.data(), sbuf.size());
}

std::string PackOpenAck(int64_t ns) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(5);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::OPEN_ACK));
	n6k::PackString(pk, "ns");
	pk.pack(ns);
	n6k::PackString(pk, "max_concurrent_reqs");
	pk.pack(8);
	n6k::PackString(pk, "default_batch_credits");
	pk.pack(8);
	n6k::PackString(pk, "capabilities");
	pk.pack_array(0);
	return std::string(sbuf.data(), sbuf.size());
}

std::string PackOpenErr(int64_t ns, const std::string &message) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(4);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::OPEN_ERR));
	n6k::PackString(pk, "ns");
	pk.pack(ns);
	n6k::PackString(pk, "exception_type");
	pk.pack(std::string("IOException"));
	n6k::PackString(pk, "exception_message");
	pk.pack(message);
	return std::string(sbuf.data(), sbuf.size());
}

std::string PackHelloPending() {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(2);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::HELLO_PENDING));
	n6k::PackString(pk, "message");
	pk.pack(std::string(kStatus));
	return std::string(sbuf.data(), sbuf.size());
}

bool IsHelloScenario(const std::string &scenario) {
	return scenario.rfind("hello_", 0) == 0;
}

std::string PackOpenPending(int64_t ns) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(3);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::OPEN_PENDING));
	n6k::PackString(pk, "ns");
	pk.pack(ns);
	n6k::PackString(pk, "message");
	pk.pack(std::string(kStatus));
	return std::string(sbuf.data(), sbuf.size());
}

// Each run gets its own candidates: every scenario in this process binds a server on one thread.
int CandidateBasePort() {
	static std::atomic<int> next {0};
	const auto tid = std::hash<std::thread::id> {}(std::this_thread::get_id());
	return 43000 + static_cast<int>(tid % 1000) + next.fetch_add(16);
}

// Played on its own thread so the server callback returns and ix keeps reading.
void PlayScript(const std::string &scenario, ix::WebSocket &ws, int64_t ns, const std::atomic<bool> &abort) {
	auto sleep_ms = [&](int ms) {
		const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
		while (!abort.load() && std::chrono::steady_clock::now() < until) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		return !abort.load();
	};
	const bool hello = IsHelloScenario(scenario);
	const std::string step = hello ? scenario.substr(6) : scenario;
	const std::string pending = hello ? PackHelloPending() : PackOpenPending(ns);
	if (step == "no_reply") {
		return;
	}
	if (step == "late_pending") {
		if (sleep_ms(500)) {
			ws.sendBinary(pending);
		}
		return;
	}
	for (int t = 0; t < kPendingForMs; t += kBeatMs) {
		ws.sendBinary(pending);
		if (!sleep_ms(kBeatMs)) {
			return;
		}
	}
	if (step == "ack") {
		ws.sendBinary(hello ? PackHelloAck() : PackOpenAck(ns));
	} else if (scenario == "err") {
		ws.sendBinary(PackOpenErr(ns, "backend exploded"));
	} else if (scenario == "drop") {
		ws.close();
	}
}

OpenPendingResult RunOpenPending(const std::string &scenario) {
	OpenPendingResult r;

	std::atomic<bool> abort {false};
	std::atomic<bool> cancel_seen {false};
	std::mutex script_mu;
	std::thread script;
	const bool reopen = scenario == "reopen_after_timeout";
	const bool reconnect = scenario == "reconnect_dead_abort" || scenario == "reconnect_abort" || reopen;
	std::atomic<int32_t> opens {0};
	std::atomic<ix::WebSocket *> first_ws {nullptr};
	std::mutex wire_mu;
	std::vector<int64_t> open_ns;
	std::vector<std::pair<int32_t, int64_t>> cancels;

	std::unique_ptr<ix::WebSocketServer> server;
	const int base = CandidateBasePort();
	int port = 0;
	for (int attempt = 0; attempt < 16 && !server; ++attempt) {
		const int candidate_port = base + attempt;
		std::unique_ptr<ix::WebSocketServer> candidate(new ix::WebSocketServer(candidate_port, "127.0.0.1"));
		candidate->disablePerMessageDeflate();
		candidate->setOnClientMessageCallback(
		    [&](const std::shared_ptr<ix::ConnectionState> &, ix::WebSocket &ws, const ix::WebSocketMessagePtr &msg) {
			    if (msg->type != ix::WebSocketMessageType::Message || !msg->binary) {
				    return;
			    }
			    msgpack::object_handle oh;
			    size_t off = 0;
			    try {
				    oh = msgpack::unpack(msg->str.data(), msg->str.size(), off);
			    } catch (...) {
				    return;
			    }
			    const msgpack::object &h = oh.get();
			    if (h.type != msgpack::type::MAP) {
				    return;
			    }
			    const auto type = static_cast<n6k::FrameType>(n6k::GetInt(h, "t", -1));
			    const bool hello_script = IsHelloScenario(scenario);
			    if (type == n6k::FrameType::HELLO && !hello_script) {
				    ws.sendBinary(PackHelloAck());
				    return;
			    }
			    if (type == n6k::FrameType::CANCEL && n6k::GetInt(h, "id", -1) == 0) {
				    cancel_seen.store(true);
				    std::lock_guard<std::mutex> lk(wire_mu);
				    cancels.emplace_back(opens.load(), n6k::GetInt(h, "ns", 0));
				    return;
			    }
			    if (type == n6k::FrameType::OPEN && hello_script) {
				    ws.sendBinary(PackOpenAck(n6k::GetInt(h, "ns", 0)));
				    return;
			    }
			    if (type != (hello_script ? n6k::FrameType::HELLO : n6k::FrameType::OPEN)) {
				    return;
			    }
			    const int64_t ns = n6k::GetInt(h, "ns", 0);
			    if (type == n6k::FrameType::OPEN) {
				    std::lock_guard<std::mutex> lk(wire_mu);
				    open_ns.push_back(ns);
			    }
			    const int32_t open_index = reconnect ? opens.fetch_add(1) : 0;
			    if (reconnect && open_index == 0) {
				    first_ws.store(&ws);
				    ws.sendBinary(PackOpenAck(ns));
				    return;
			    }
			    if (reopen) {
				    if (open_index == 2) {
					    ws.sendBinary(PackOpenAck(ns));
				    }
				    return;
			    }
			    std::lock_guard<std::mutex> lk(script_mu);
			    if (!script.joinable()) {
				    script = std::thread(PlayScript, reconnect ? std::string("ack") : scenario, std::ref(ws), ns,
				                         std::cref(abort));
			    }
		    });
		if (!candidate->listen().first) {
			continue;
		}
		candidate->start();
		server = std::move(candidate);
		port = candidate_port;
	}
	if (!server) {
		r.error = "no port to listen on";
		return r;
	}

	auto hub_opts = n6k::WebSocketOptions("ws://127.0.0.1:" + std::to_string(port) + "/ws?catalog=pendtest", "");
	hub_opts.connect_timeout = std::chrono::milliseconds(kConnectTimeoutMs);
	hub_opts.hello_pending_stall = std::chrono::milliseconds(kStallMs);
	auto hub = n6k::ConnectionHub::Create(nullptr, std::move(hub_opts));

	n6k::WsClientOptions opts;
	opts.catalog = "pendtest";
	opts.connect_timeout = std::chrono::milliseconds(kConnectTimeoutMs);
	opts.open_pending_stall = std::chrono::milliseconds(kStallMs);
	opts.open_channel = [hub](uint64_t ns, const n6k::AbortCheck &abort_check) {
		hub->EnsureConnected(abort_check);
		return std::unique_ptr<n6k::Reactor>(new n6k::HubSession(hub, ns));
	};

	const auto t0 = std::chrono::steady_clock::now();
	n6k::AbortCheck connect_abort;
	if (scenario == "abort") {
		connect_abort = [t0] {
			return std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(500);
		};
	}
	// Stands in for the ATTACH's ClientContext, which may be gone once Connect returns.
	auto attach_done = std::make_shared<std::atomic<bool>>(false);
	auto aborts_after_connect = std::make_shared<std::atomic<int32_t>>(0);
	std::atomic<int32_t> request_polls {0};
	if (reconnect) {
		connect_abort = [attach_done, aborts_after_connect] {
			if (attach_done->load()) {
				(*aborts_after_connect)++;
			}
			return false;
		};
	}
	auto client = n6k::WsClient::Create(opts);
	try {
		client->Connect(connect_abort);
		r.connected = true;
		if (reconnect) {
			attach_done->store(true);
			first_ws.load()->close();
			const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(1);
			while (client->IsConnected() && std::chrono::steady_clock::now() < until) {
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			r.connected = false;
			if (reopen) {
				try {
					client->AwaitReadySessionAndSendRequest(n6k::OP_SCHEMA_LIST, std::string(), nullptr);
				} catch (const std::exception &) {
					// The unanswered OPEN: a bare timeout, which leaves the client to reopen on the next request.
				}
			}
			const auto t_reconnect = std::chrono::steady_clock::now();
			const bool interrupt = scenario == "reconnect_abort";
			client->AwaitReadySessionAndSendRequest(
			    n6k::OP_SCHEMA_LIST, std::string(), [&request_polls, interrupt, t_reconnect] {
				    request_polls++;
				    return interrupt &&
				           std::chrono::steady_clock::now() - t_reconnect >= std::chrono::milliseconds(500);
			    });
			r.connected = client->IsConnected();
		}
	} catch (const InterruptException &e) {
		r.interrupted = true;
		r.error = e.what();
	} catch (const std::exception &e) {
		r.error = e.what();
	}
	r.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	if (r.interrupted) {
		const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(1);
		while (!cancel_seen.load() && std::chrono::steady_clock::now() < until) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}

	abort.store(true);
	{
		std::lock_guard<std::mutex> lk(script_mu);
		if (script.joinable()) {
			script.join();
		}
	}
	// Before Close, whose own CANCEL {ns, id: 0} would otherwise read as the one under test.
	r.cancel_seen = cancel_seen.load();
	{
		std::lock_guard<std::mutex> lk(wire_mu);
		r.open_ns = open_ns;
		r.cancels = cancels;
	}
	client->Close();
	hub->Disconnect("test done");
	server->stop();
	r.opens = opens.load();
	r.aborts_after_connect = aborts_after_connect->load();
	r.request_polls = request_polls.load();
	return r;
}

bool AllDistinct(const std::vector<int64_t> &values) {
	for (size_t i = 0; i < values.size(); i++) {
		for (size_t j = i + 1; j < values.size(); j++) {
			if (values[i] == values[j]) {
				return false;
			}
		}
	}
	return true;
}

} // namespace

TEST_CASE("connect waits out OPEN_PENDING past its timeout, then connects on OPEN_ACK", "[open_pending]") {
	const auto r = RunOpenPending("ack");
	INFO(r.error);

	REQUIRE(r.connected);
	REQUIRE(r.elapsed_ms >= kPendingForMs);
}

TEST_CASE("connect reports the OPEN_ERR that follows OPEN_PENDING", "[open_pending]") {
	const auto r = RunOpenPending("err");

	REQUIRE(!r.connected);
	REQUIRE_THAT(r.error, Catch::Contains("backend exploded"));
}

TEST_CASE("connect fails with the last status when OPEN_PENDING stalls", "[open_pending]") {
	const auto r = RunOpenPending("stall");

	REQUIRE(!r.connected);
	REQUIRE_THAT(r.error, Catch::Contains("open stalled"));
	REQUIRE_THAT(r.error, Catch::Contains(kStatus));
	REQUIRE(r.elapsed_ms >= kPendingForMs + kStallMs - kBeatMs);
}

TEST_CASE("connect reports the last status when the socket drops while pending", "[open_pending]") {
	const auto r = RunOpenPending("drop");

	REQUIRE(!r.connected);
	REQUIRE_THAT(r.error, Catch::Contains(kStatus));
	REQUIRE(r.elapsed_ms >= kPendingForMs);
}

TEST_CASE("connect times out with no reply, or a first OPEN_PENDING past the timeout", "[open_pending]") {
	const std::string scenario = GENERATE(as<std::string> {}, "no_reply", "late_pending");
	INFO(scenario);
	const auto r = RunOpenPending(scenario);

	REQUIRE(!r.connected);
	REQUIRE_THAT(r.error, Catch::Contains("connect timeout"));
	REQUIRE(r.elapsed_ms < kPendingForMs);
}

TEST_CASE("an abort during OPEN_PENDING interrupts and sends CANCEL", "[open_pending]") {
	const auto r = RunOpenPending("abort");

	REQUIRE(!r.connected);
	REQUIRE(r.interrupted);
	REQUIRE(r.cancel_seen);
	REQUIRE(r.elapsed_ms < kPendingForMs);
}

TEST_CASE("connect waits out HELLO_PENDING past its timeout, then connects on HELLO_ACK", "[open_pending]") {
	const auto r = RunOpenPending("hello_ack");
	INFO(r.error);

	REQUIRE(r.connected);
	REQUIRE(r.elapsed_ms >= kPendingForMs);
}

TEST_CASE("connect fails with the last status when HELLO_PENDING stalls", "[open_pending]") {
	const auto r = RunOpenPending("hello_stall");

	REQUIRE(!r.connected);
	REQUIRE_THAT(r.error, Catch::Contains("handshake stalled"));
	REQUIRE_THAT(r.error, Catch::Contains(kStatus));
	REQUIRE(r.elapsed_ms >= kPendingForMs + kStallMs - kBeatMs);
}

TEST_CASE("a reconnect polls the request's abort, never Connect's, and opens a fresh ns", "[open_pending]") {
	const auto r = RunOpenPending("reconnect_dead_abort");
	INFO(r.error);

	REQUIRE(r.connected);
	REQUIRE(r.opens == 2);
	REQUIRE(r.open_ns.size() == 2);
	REQUIRE(AllDistinct(r.open_ns));
	REQUIRE(r.aborts_after_connect == 0);
	REQUIRE(r.request_polls > 0);
}

TEST_CASE("a request's abort interrupts a reconnect and sends CANCEL", "[open_pending]") {
	const auto r = RunOpenPending("reconnect_abort");
	INFO(r.error);

	REQUIRE(!r.connected);
	REQUIRE(r.interrupted);
	REQUIRE(r.cancel_seen);
	REQUIRE(r.opens == 2);
	REQUIRE(r.open_ns.size() == 2);
	REQUIRE(AllDistinct(r.open_ns));
	REQUIRE(r.aborts_after_connect == 0);
	REQUIRE(r.request_polls > 0);
}

TEST_CASE("a reopen after a timed-out OPEN first releases that session's ns", "[open_pending]") {
	const auto r = RunOpenPending("reopen_after_timeout");
	INFO(r.error);

	REQUIRE(r.connected);
	REQUIRE(r.open_ns.size() == 3);
	REQUIRE(AllDistinct(r.open_ns));
	REQUIRE(r.aborts_after_connect == 0);

	bool released_before_reopen = false;
	for (auto &cancel : r.cancels) {
		if (cancel.second == r.open_ns[1] && cancel.first == 2) {
			released_before_reopen = true;
		}
	}
	REQUIRE(released_before_reopen);
}
