#include "catch.hpp"
#include "connection_hub.hpp"
#include "hub_session.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_stream_framing.hpp"
#include "ws_transport_raw.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace duckdb;

namespace {

struct HubRoutingResult {
	std::string error;
	int32_t got_ns1 = 0;
	int32_t got_ns2 = 0;
	bool ns1_only_own = false;
	bool ns2_only_own = false;
	bool untagged_dropped = false;
	bool unknown_ns_dropped = false;
	bool request_routed = false;
	bool pong_not_stolen = false;
	bool ping_answered = false;
	bool single_hub = false;
	bool close_waits = false;
	bool close_sends_cancel = false;
};

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

HubRoutingResult RunHubRouting() {
	HubRoutingResult r;

	int sv[2] = {-1, -1};
	if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
		r.error = "socketpair failed";
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
		opts.endpoint = "test socket";
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
		WriteFramed(peer_fd, MakeFrame(0));
		WriteFramed(peer_fd, MakeFrame(99));
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
		// Give a stray or misrouted frame a chance to land before asserting it did not.
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
		hub->Disconnect("test done");
	} catch (const std::exception &e) {
		r.error = e.what();
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

	::close(peer_fd);
	::close(client_fd);
	return r;
}

} // namespace

TEST_CASE("one hub routes each frame by ns, answers PING and closes sessions in order", "[hub]") {
	const auto r = RunHubRouting();
	INFO(r.error);

	REQUIRE(r.error.empty());
	REQUIRE(r.single_hub);
	REQUIRE(r.got_ns1 == 1);
	REQUIRE(r.got_ns2 == 1);
	REQUIRE(r.ns1_only_own);
	REQUIRE(r.ns2_only_own);
	REQUIRE(r.untagged_dropped);
	REQUIRE(r.unknown_ns_dropped);
	REQUIRE(r.request_routed);
	REQUIRE(r.pong_not_stolen);
	REQUIRE(r.ping_answered);
	REQUIRE(r.close_waits);
	REQUIRE(r.close_sends_cancel);
}
