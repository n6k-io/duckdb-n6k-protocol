// `n6k_selftest_open_pending(scenario)` — pins how WsClient::Connect waits on FT_OPEN_PENDING and
// FT_HELLO_PENDING.
//
// Drives the REAL WsClient on a ConnectionHub against an in-process ix::WebSocketServer that answers
// HELLO_ACK and then plays a scripted reply to OPEN, with connect_timeout=kConnectTimeoutMs and
// open_pending_stall=kStallMs:
//   ack          PENDING(kStatus) every kBeatMs for kPendingForMs, then OPEN_ACK -> connected
//   err          same, then OPEN_ERR("backend exploded")                   -> error carries it
//   stall        same, then silence                                        -> "open stalled" + status
//   drop         same, then the server closes the socket                   -> error carries the status
//   no_reply     no reply at all                                           -> connect timeout
//   late_pending first PENDING past connect_timeout                        -> connect timeout
//   abort        PENDING, then Connect's should_abort turns true            -> InterruptException + CANCEL
//   hello_ack    the ack script, played on HELLO with HELLO_PENDING         -> connected
//   hello_stall  the stall script, played on HELLO with HELLO_PENDING       -> "handshake stalled" + status
// The reconnect scenarios answer the first OPEN with OPEN_ACK, drop the socket, then play the ack script
// on the reconnect's OPEN; Connect's should_abort must never run after Connect returns:
//   reconnect_dead_abort  the request's should_abort is polled       -> connected
//   reconnect_abort       the request's should_abort turns true      -> InterruptException + CANCEL
// Every reconnect's OPEN carries a fresh ns. reopen_after_timeout answers the first OPEN, drops the socket,
// leaves the reconnect's OPEN unanswered (connect timeout, hub still up), then answers a third: the second
// session's ns must be released with CANCEL {ns, id: 0} before that third OPEN, on the same socket.
//
// Native only; wasm reports mode='wasm_no_ix'.

#include "open_pending_selftest.hpp"
#include "duckdb/function/table_function.hpp"

#include <chrono>
#include <cstdint>
#include <string>

#ifndef WASM_LOADABLE_EXTENSIONS
#include "duckdb/common/exception.hpp"
#include "n6k_msgpack.hpp"
#include "connection_hub.hpp"
#include "hub_session.hpp"
#include "n6k_protocol_generated.hpp"
#include "ws_client.hpp"

#include <atomic>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketMessage.h>
#include <ixwebsocket/IXWebSocketMessageType.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#endif

namespace duckdb {

namespace {

struct OpenPendingResult {
	std::string mode;
	bool ok = false;
	bool connected = false;
	std::string error;
	int32_t pendings_sent = 0;
	int64_t elapsed_ms = 0;
	bool interrupted = false;
	bool cancel_seen = false;
};

#ifndef WASM_LOADABLE_EXTENSIONS

constexpr int kConnectTimeoutMs = 300;
constexpr int kStallMs = 600;
constexpr int kBeatMs = 150;
constexpr int kPendingForMs = 1000;
constexpr const char *kStatus = "warming";

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

int CandidateBasePort() {
	const auto tid = std::hash<std::thread::id> {}(std::this_thread::get_id());
	return 43000 + static_cast<int>(tid % 2000);
}

bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

// Played on its own thread so the server callback returns and ix keeps reading.
void PlayScript(const std::string &scenario, ix::WebSocket &ws, int64_t ns, const std::atomic<bool> &abort,
                std::atomic<int32_t> &pendings) {
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
			pendings++;
		}
		return;
	}
	for (int t = 0; t < kPendingForMs; t += kBeatMs) {
		ws.sendBinary(pending);
		pendings++;
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

OpenPendingResult RunOpenPendingSelftest(const std::string &scenario) {
	OpenPendingResult r;
	r.mode = "native";

	std::atomic<bool> abort {false};
	std::atomic<int32_t> pendings {0};
	std::atomic<bool> cancel_seen {false};
	std::mutex script_mu;
	std::thread script;
	const bool reopen = scenario == "reopen_after_timeout";
	const bool reconnect = scenario == "reconnect_dead_abort" || scenario == "reconnect_abort" || reopen;
	std::atomic<int32_t> opens {0};
	std::atomic<ix::WebSocket *> first_ws {nullptr};
	// What the server saw, in order: each OPEN's ns, and each CANCEL {ns, id: 0}'s ns.
	std::mutex wire_mu;
	std::vector<int64_t> open_ns;
	std::vector<std::pair<int32_t, int64_t>> cancels; // (OPENs seen before it, ns)

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
				                         std::cref(abort), std::ref(pendings));
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
	opts.open_channel = [hub](uint64_t ns, const n6k::AbortCheck &abort) {
		hub->EnsureConnected(abort);
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
	std::vector<int64_t> seen_open_ns;
	std::vector<std::pair<int32_t, int64_t>> seen_cancels;
	{
		std::lock_guard<std::mutex> lk(wire_mu);
		seen_open_ns = open_ns;
		seen_cancels = cancels;
	}
	client->Close();
	hub->Disconnect("selftest done");
	server->stop();
	r.pendings_sent = pendings.load();

	const bool waited_past_connect_timeout = r.elapsed_ms >= kPendingForMs;
	if (scenario == "ack") {
		r.ok = r.connected && waited_past_connect_timeout;
	} else if (scenario == "err") {
		r.ok = !r.connected && Contains(r.error, "backend exploded");
	} else if (scenario == "stall") {
		r.ok = !r.connected && Contains(r.error, "open stalled") && Contains(r.error, kStatus) &&
		       r.elapsed_ms >= kPendingForMs + kStallMs - kBeatMs;
	} else if (scenario == "drop") {
		r.ok = !r.connected && Contains(r.error, kStatus) && waited_past_connect_timeout;
	} else if (scenario == "hello_ack") {
		r.ok = r.connected && waited_past_connect_timeout;
	} else if (scenario == "hello_stall") {
		r.ok = !r.connected && Contains(r.error, "handshake stalled") && Contains(r.error, kStatus) &&
		       r.elapsed_ms >= kPendingForMs + kStallMs - kBeatMs;
	} else if (scenario == "abort") {
		r.ok = !r.connected && r.interrupted && r.cancel_seen && r.elapsed_ms < kPendingForMs;
	} else if (reopen) {
		const bool fresh = seen_open_ns.size() == 3 && seen_open_ns[0] != seen_open_ns[1] &&
		                   seen_open_ns[1] != seen_open_ns[2] && seen_open_ns[0] != seen_open_ns[2];
		bool released_before_reopen = false;
		for (auto &cancel : seen_cancels) {
			if (fresh && cancel.second == seen_open_ns[1] && cancel.first == 2) {
				released_before_reopen = true;
			}
		}
		r.ok = r.connected && fresh && released_before_reopen && aborts_after_connect->load() == 0;
	} else if (reconnect) {
		const auto stale = aborts_after_connect->load();
		const bool fresh = seen_open_ns.size() == 2 && seen_open_ns[0] != seen_open_ns[1];
		const bool reconnected = opens.load() == 2 && fresh && stale == 0 && request_polls.load() > 0;
		if (scenario == "reconnect_abort") {
			r.ok = reconnected && !r.connected && r.interrupted && r.cancel_seen;
		} else {
			r.ok = reconnected && r.connected;
		}
		if (stale > 0) {
			r.error = "Connect's should_abort ran " + std::to_string(stale) + " times after Connect";
		}
	} else if (scenario == "no_reply" || scenario == "late_pending") {
		r.ok = !r.connected && Contains(r.error, "connect timeout") && r.elapsed_ms < kPendingForMs;
	} else {
		r.error = "unknown scenario \"" + scenario + "\"";
	}
	return r;
}

#else

OpenPendingResult RunOpenPendingSelftest(const std::string &) {
	OpenPendingResult r;
	r.mode = "wasm_no_ix";
	r.ok = true;
	return r;
}

#endif

struct N6kOpenPendingBind : public TableFunctionData {
	std::string scenario;
};

struct N6kOpenPendingState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                              vector<Identifier> &names) {
	names = {"mode", "ok", "connected", "error", "pendings_sent", "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT};
	auto result = make_uniq<N6kOpenPendingBind>();
	result->scenario = input.inputs[0].GetValue<string>();
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kOpenPendingState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kOpenPendingState>();
	if (state.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	state.emitted = true;

	const OpenPendingResult r = RunOpenPendingSelftest(data_p.bind_data->Cast<N6kOpenPendingBind>().scenario);
	output.data[0].SetValue(0, Value(r.mode));
	output.data[1].SetValue(0, Value::BOOLEAN(r.ok));
	output.data[2].SetValue(0, Value::BOOLEAN(r.connected));
	output.data[3].SetValue(0, Value(r.error));
	output.data[4].SetValue(0, Value::INTEGER(r.pendings_sent));
	output.data[5].SetValue(0, Value::BIGINT(r.elapsed_ms));
	output.SetChildCardinality(1);
}

} // namespace

void RegisterN6kSelftestOpenPending(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_open_pending", {LogicalType::VARCHAR}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
