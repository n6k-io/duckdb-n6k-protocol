#include "connection_hub_selftest.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

#include <chrono>
#include <string>
#include <vector>

#if !defined(WASM_LOADABLE_EXTENSIONS) && !defined(_WIN32)
#define N6K_HAS_RAW_SOCKET 1
#include "connection_hub.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_stream_framing.hpp"
#include "hub_session.hpp"
#include "ws_transport_raw.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace {

struct HubRoutingResult {
	std::string mode = "native";
	bool ok = false;
	int32_t got_ns1 = 0;
	int32_t got_ns2 = 0;
	// Each session saw exactly its own ns.
	bool ns1_only_own = false;
	bool ns2_only_own = false;
	// An untagged frame no request claims reaches no session.
	bool untagged_dropped = false;
	// A frame naming a session nobody claims is dropped, never handed to a sibling.
	bool unknown_ns_dropped = false;
	// An untagged RESP_CHUNK whose id is a bound connection-scoped request reaches only that request.
	bool request_routed = false;
	// An untagged PONG with the same id is not a response: the request never sees it.
	bool pong_not_stolen = false;
	// The hub answered an untagged PING with a PONG carrying its id, and no session saw the PING.
	bool ping_answered = false;
	bool single_hub = false;
	// Close() did not return while that session's callback was still running.
	bool close_waits = false;
	// Close() put CANCEL {ns, id: 0} on the wire for every closed session.
	bool close_sends_cancel = false;
	int64_t elapsed_ms = 0;
};

#ifdef N6K_HAS_RAW_SOCKET

constexpr int64_t T_RESP_CHUNK = static_cast<int64_t>(n6k::FrameType::RESP_CHUNK);
constexpr int64_t T_PONG = static_cast<int64_t>(n6k::FrameType::PONG);
constexpr int64_t T_PING = static_cast<int64_t>(n6k::FrameType::PING);
constexpr int64_t T_CANCEL = static_cast<int64_t>(n6k::FrameType::CANCEL);
constexpr int64_t PING_ID = 77;

std::string MakeFrame(int64_t ns, int64_t t = T_RESP_CHUNK, int64_t id = -1) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(1 + (ns != 0 ? 1 : 0) + (id >= 0 ? 1 : 0));
	n6k::PackString(pk, "t");
	pk.pack(t);
	if (ns != 0) {
		n6k::PackString(pk, "ns");
		pk.pack(ns);
	}
	if (id >= 0) {
		n6k::PackString(pk, "id");
		pk.pack(id);
	}
	return std::string(sb.data(), sb.size());
}

std::string MakeHelloAck() {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::HELLO_ACK));
	n6k::PackString(pk, "protocol_version");
	pk.pack(static_cast<int>(n6k::N6K_PROTOCOL_VERSION));
	n6k::PackString(pk, "capabilities");
	pk.pack_array(0);
	return std::string(sb.data(), sb.size());
}

bool WriteFramed(int fd, const std::string &frame) {
	std::string buf;
	n6k::AppendStreamHeader(buf, n6k::StreamFrameType::N6K, frame.size());
	buf.append(frame);
	size_t sent = 0;
	while (sent < buf.size()) {
		ssize_t w = ::write(fd, buf.data() + sent, buf.size() - sent);
		if (w <= 0) {
			return false;
		}
		sent += static_cast<size_t>(w);
	}
	return true;
}

bool ReadExactWithin(int fd, char *buf, size_t n, std::chrono::steady_clock::time_point deadline) {
	size_t got = 0;
	while (got < n) {
		auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
		if (left.count() <= 0) {
			return false;
		}
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = POLLIN;
		pfd.revents = 0;
		if (::poll(&pfd, 1, static_cast<int>(left.count())) <= 0) {
			return false;
		}
		ssize_t r = ::read(fd, buf + got, n - got);
		if (r <= 0) {
			return false;
		}
		got += static_cast<size_t>(r);
	}
	return true;
}

// True once the peer reads a frame of type `t` with this `ns` and `id` among the frames the hub wrote.
bool PeerSaw(int fd, int64_t t, int64_t ns, int64_t id) {
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	for (;;) {
		char header[n6k::STREAM_HEADER_BYTES];
		if (!ReadExactWithin(fd, header, n6k::STREAM_HEADER_BYTES, deadline)) {
			return false;
		}
		std::string frame(n6k::StreamPayloadLength(header), '\0');
		if (!frame.empty() && !ReadExactWithin(fd, &frame[0], frame.size(), deadline)) {
			return false;
		}
		try {
			size_t off = 0;
			auto oh = msgpack::unpack(frame.data(), frame.size(), off);
			if (n6k::GetInt(oh.get(), "t", -1) == t && n6k::GetInt(oh.get(), "ns", 0) == ns &&
			    n6k::GetInt(oh.get(), "id", -1) == id) {
				return true;
			}
		} catch (...) {
			return false;
		}
	}
}

// Holds a session's callback mid-dispatch until released.
struct Gate {
	std::mutex mu;
	std::condition_variable cv;
	bool entered = false;
	bool released = false;

	void EnterAndWait() {
		std::unique_lock<std::mutex> lk(mu);
		entered = true;
		cv.notify_all();
		cv.wait(lk, [&] { return released; });
	}
	bool AwaitEntered() {
		std::unique_lock<std::mutex> lk(mu);
		return cv.wait_for(lk, std::chrono::seconds(5), [&] { return entered; });
	}
	void Release() {
		std::lock_guard<std::mutex> lk(mu);
		released = true;
		cv.notify_all();
	}
};

struct Seen {
	int64_t ns;
	int64_t t;
};

struct Sink {
	std::mutex mu;
	std::vector<Seen> seen;

	void Record(const std::string &frame) {
		Seen s {0, -1};
		try {
			size_t off = 0;
			auto oh = msgpack::unpack(frame.data(), frame.size(), off);
			if (oh.get().type == msgpack::type::MAP) {
				s.ns = n6k::GetInt(oh.get(), "ns", 0);
				s.t = n6k::GetInt(oh.get(), "t", -1);
			}
		} catch (...) {
			s.ns = -1;
		}
		std::lock_guard<std::mutex> lk(mu);
		seen.push_back(s);
	}

	std::vector<Seen> Snapshot() {
		std::lock_guard<std::mutex> lk(mu);
		return seen;
	}
};

int CountOf(const std::vector<Seen> &values, int64_t ns, int64_t t = T_RESP_CHUNK) {
	int n = 0;
	for (auto &v : values) {
		if (v.ns == ns && v.t == t) {
			n++;
		}
	}
	return n;
}

HubRoutingResult RunHubRoutingSelftest() {
	HubRoutingResult r;
	const auto t0 = std::chrono::steady_clock::now();
	const auto stamp = [&]() {
		r.elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	};

	int sv[2] = {-1, -1};
	if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
		r.mode = "socketpair_failed";
		stamp();
		return r;
	}
	const int client_fd = sv[0];
	const int peer_fd = sv[1];

	Sink sink1;
	Sink sink2;
	Sink sink_req;
	try {
		n6k::ConnectionHub::Options opts;
		opts.dial = [client_fd]() {
			return std::unique_ptr<n6k::Transport>(new n6k::RawSocketTransport(client_fd));
		};
		opts.endpoint = "selftest socket";
		auto hub = n6k::ConnectionHub::Create(nullptr, std::move(opts));
		// Queued ahead, so the hub's HELLO finds its answer already waiting.
		WriteFramed(peer_fd, MakeHelloAck());
		hub->EnsureConnected(nullptr);

		n6k::HubSession s1(hub, /*ns=*/1);
		n6k::HubSession s2(hub, /*ns=*/2);
		s1.Start([&sink1](const std::string &bytes, bool) { sink1.Record(bytes); }, nullptr);
		s2.Start([&sink2](const std::string &bytes, bool) { sink2.Record(bytes); }, nullptr);
		r.single_hub = hub->SessionCount() == 2;
		const uint32_t req_id =
		    hub->BindRequest([&sink_req](const std::string &bytes, bool) { sink_req.Record(bytes); }, nullptr);

		WriteFramed(peer_fd, MakeFrame(1));
		WriteFramed(peer_fd, MakeFrame(2));
		WriteFramed(peer_fd, MakeFrame(0));  // untagged, no request claims it: dropped
		WriteFramed(peer_fd, MakeFrame(99)); // nobody's session: dropped
		WriteFramed(peer_fd, MakeFrame(0, T_RESP_CHUNK, req_id));
		WriteFramed(peer_fd, MakeFrame(0, T_PONG, req_id));
		WriteFramed(peer_fd, MakeFrame(0, T_PING, PING_ID));

		r.ping_answered = PeerSaw(peer_fd, T_PONG, 0, PING_ID);
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (std::chrono::steady_clock::now() < deadline) {
			if (!sink1.Snapshot().empty() && !sink2.Snapshot().empty() && !sink_req.Snapshot().empty()) {
				break;
			}
			::usleep(2000);
		}
		// Give a stray/misrouted frame a chance to land before asserting it did not.
		::usleep(50000);
		hub->UnbindRequest(req_id);

		Gate gate;
		n6k::HubSession s3(hub, /*ns=*/3);
		s3.Start([&gate](const std::string &, bool) { gate.EnterAndWait(); }, nullptr);
		WriteFramed(peer_fd, MakeFrame(3));
		if (gate.AwaitEntered()) {
			std::atomic<bool> closed {false};
			std::thread closer([&]() {
				s3.Close();
				closed.store(true);
			});
			::usleep(50000);
			const bool returned_early = closed.load();
			gate.Release();
			closer.join();
			r.close_waits = !returned_early && closed.load();
		} else {
			gate.Release();
		}

		s1.Close();
		s2.Close();
		r.close_sends_cancel =
		    PeerSaw(peer_fd, T_CANCEL, 3, 0) && PeerSaw(peer_fd, T_CANCEL, 1, 0) && PeerSaw(peer_fd, T_CANCEL, 2, 0);
		hub->Disconnect("selftest done");
	} catch (const std::exception &e) {
		r.mode = std::string("error: ") + e.what();
	}

	const auto seen1 = sink1.Snapshot();
	const auto seen2 = sink2.Snapshot();
	const auto seen_req = sink_req.Snapshot();
	r.got_ns1 = static_cast<int32_t>(seen1.size());
	r.got_ns2 = static_cast<int32_t>(seen2.size());
	r.ns1_only_own = CountOf(seen1, 1) == 1 && CountOf(seen1, 2) == 0;
	r.ns2_only_own = CountOf(seen2, 2) == 1 && CountOf(seen2, 1) == 0;
	r.untagged_dropped = CountOf(seen1, 0) == 0 && CountOf(seen2, 0) == 0;
	r.unknown_ns_dropped = CountOf(seen1, 99) == 0 && CountOf(seen2, 99) == 0;
	r.request_routed = seen_req.size() == 1 && CountOf(seen_req, 0) == 1;
	r.pong_not_stolen = CountOf(seen_req, 0, T_PONG) == 0;
	r.ping_answered = r.ping_answered && CountOf(seen1, 0, T_PING) == 0 && CountOf(seen2, 0, T_PING) == 0;
	r.ok = r.single_hub && r.ns1_only_own && r.ns2_only_own && r.untagged_dropped && r.unknown_ns_dropped &&
	       r.request_routed && r.pong_not_stolen && r.ping_answered && r.close_waits && r.close_sends_cancel &&
	       r.got_ns1 == 1 && r.got_ns2 == 1;

	::close(peer_fd);
	::close(client_fd);
	stamp();
	return r;
}

#else

HubRoutingResult RunHubRoutingSelftest() {
	HubRoutingResult r;
	r.mode = "no_raw_socket";
	r.ok = true;
	return r;
}

#endif

struct N6kHubRoutingBind : public TableFunctionData {};

struct N6kHubRoutingState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<Identifier> &names) {
	names = {"mode",
	         "ok",
	         "got_ns1",
	         "got_ns2",
	         "ns1_only_own",
	         "ns2_only_own",
	         "untagged_dropped",
	         "unknown_ns_dropped",
	         "single_hub",
	         "request_routed",
	         "pong_not_stolen",
	         "ping_answered",
	         "close_waits",
	         "close_sends_cancel",
	         "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::INTEGER, LogicalType::INTEGER,
	                LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::INTEGER};
	return make_uniq<N6kHubRoutingBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kHubRoutingState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kHubRoutingState>();
	if (state.emitted) {
		output.SetChildCardinality(0);
		return;
	}
	state.emitted = true;

	const HubRoutingResult r = RunHubRoutingSelftest();
	output.data[0].SetValue(0, Value(r.mode));
	output.data[1].SetValue(0, Value::BOOLEAN(r.ok));
	output.data[2].SetValue(0, Value::INTEGER(r.got_ns1));
	output.data[3].SetValue(0, Value::INTEGER(r.got_ns2));
	output.data[4].SetValue(0, Value::BOOLEAN(r.ns1_only_own));
	output.data[5].SetValue(0, Value::BOOLEAN(r.ns2_only_own));
	output.data[6].SetValue(0, Value::BOOLEAN(r.untagged_dropped));
	output.data[7].SetValue(0, Value::BOOLEAN(r.unknown_ns_dropped));
	output.data[8].SetValue(0, Value::BOOLEAN(r.single_hub));
	output.data[9].SetValue(0, Value::BOOLEAN(r.request_routed));
	output.data[10].SetValue(0, Value::BOOLEAN(r.pong_not_stolen));
	output.data[11].SetValue(0, Value::BOOLEAN(r.ping_answered));
	output.data[12].SetValue(0, Value::BOOLEAN(r.close_waits));
	output.data[13].SetValue(0, Value::BOOLEAN(r.close_sends_cancel));
	output.data[14].SetValue(0, Value::INTEGER(static_cast<int32_t>(r.elapsed_ms)));
	output.SetChildCardinality(1);
}

} // namespace

void RegisterN6kSelftestHubRouting(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_hub_routing", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
