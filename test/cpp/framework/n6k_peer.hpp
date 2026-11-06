#pragma once

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_stream_framing.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace duckdb {
namespace n6k_test {

inline std::string PackHelloAck() {
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

inline std::string PackOpenAck(int64_t ns) {
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

inline bool WriteFramed(int fd, const std::string &frame) {
	std::string buf;
	n6k::AppendStreamHeader(buf, n6k::StreamFrameType::N6K, frame.size());
	buf.append(frame);
	size_t sent = 0;
	while (sent < buf.size()) {
		ssize_t written = ::write(fd, buf.data() + sent, buf.size() - sent);
		if (written <= 0) {
			return false;
		}
		sent += static_cast<size_t>(written);
	}
	return true;
}

inline bool ReadExactWithin(int fd, char *buf, size_t count, std::chrono::steady_clock::time_point deadline) {
	size_t got = 0;
	while (got < count) {
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
		ssize_t bytes = ::read(fd, buf + got, count - got);
		if (bytes <= 0) {
			return false;
		}
		got += static_cast<size_t>(bytes);
	}
	return true;
}

using SchemaWriter = std::function<void(msgpack::packer<msgpack::sbuffer> &)>;

inline std::string PackPush(int64_t ns, const SchemaWriter &schemas) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(4);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::PUSH));
	n6k::PackString(pk, "ns");
	pk.pack(ns);
	n6k::PackString(pk, "op");
	pk.pack(static_cast<int>(n6k::OP_CATALOG_INVALIDATED));
	n6k::PackString(pk, "schemas");
	schemas(pk);
	return std::string(sbuf.data(), sbuf.size());
}

struct PeerRequest {
	int64_t ns;
	uint32_t id;
	uint8_t op;
};

class ScriptedPeer {
public:
	ScriptedPeer() {
		int pair[2];
		if (::socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) {
			throw std::runtime_error("socketpair failed");
		}
		client_fd = pair[0];
		peer_fd = pair[1];
		reader = std::thread([this] { Read(); });
	}

	~ScriptedPeer() {
		::shutdown(peer_fd, SHUT_RDWR);
		reader.join();
		::close(peer_fd);
		::close(client_fd);
	}

	ScriptedPeer(const ScriptedPeer &) = delete;
	ScriptedPeer &operator=(const ScriptedPeer &) = delete;

	int ClientFd() const {
		return client_fd;
	}

	std::string Token() {
		std::lock_guard<std::mutex> lk(lock);
		return token;
	}

	int64_t OpenNs() const {
		return open_ns.load();
	}

	void Send(const std::string &frame) {
		std::lock_guard<std::mutex> lk(write_lock);
		WriteFramed(peer_fd, frame);
	}

	void SendRaw(const std::string &bytes) {
		std::lock_guard<std::mutex> lk(write_lock);
		if (::write(peer_fd, bytes.data(), bytes.size()) != static_cast<ssize_t>(bytes.size())) {
			throw std::runtime_error("short write");
		}
	}

	// Runs on the reader thread. Requests are otherwise ignored.
	void OnRequest(std::function<void(const PeerRequest &)> handler) {
		std::lock_guard<std::mutex> lk(lock);
		on_request = std::move(handler);
	}

	void Hangup() {
		::shutdown(peer_fd, SHUT_RDWR);
	}

	bool RoundTrip() {
		const auto id = next_ping.fetch_add(1);
		msgpack::sbuffer sbuf;
		msgpack::packer<msgpack::sbuffer> pk(&sbuf);
		pk.pack_map(2);
		n6k::PackString(pk, "t");
		pk.pack(static_cast<int>(n6k::FrameType::PING));
		n6k::PackString(pk, "id");
		pk.pack(id);
		Send(std::string(sbuf.data(), sbuf.size()));
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (last_pong.load() != id && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return last_pong.load() == id;
	}

	bool AwaitEof() const {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!eof.load() && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		return eof.load();
	}

private:
	void Read() {
		for (;;) {
			const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(5);
			char header[n6k::STREAM_HEADER_BYTES];
			if (!ReadExactWithin(peer_fd, header, sizeof(header), deadline)) {
				break;
			}
			std::string frame(n6k::StreamPayloadLength(header), '\0');
			if (!frame.empty() && !ReadExactWithin(peer_fd, &frame[0], frame.size(), deadline)) {
				break;
			}
			if (static_cast<unsigned char>(header[4]) != static_cast<unsigned char>(n6k::StreamFrameType::N6K)) {
				continue;
			}
			size_t offset = 0;
			auto handle = msgpack::unpack(frame.data(), frame.size(), offset);
			const auto &message = handle.get();
			const auto type = n6k::GetInt(message, "t", -1);
			if (type == static_cast<int64_t>(n6k::FrameType::HELLO)) {
				{
					std::lock_guard<std::mutex> lk(lock);
					token = n6k::GetStr(message, "token");
				}
				Send(PackHelloAck());
			} else if (type == static_cast<int64_t>(n6k::FrameType::OPEN)) {
				const auto ns = n6k::GetInt(message, "ns", 0);
				open_ns.store(ns);
				Send(PackOpenAck(ns));
			} else if (type == static_cast<int64_t>(n6k::FrameType::PONG)) {
				last_pong.store(n6k::GetInt(message, "id", -1));
			} else if (type == static_cast<int64_t>(n6k::FrameType::REQ)) {
				std::function<void(const PeerRequest &)> handler;
				{
					std::lock_guard<std::mutex> lk(lock);
					handler = on_request;
				}
				if (handler) {
					handler(PeerRequest {n6k::GetInt(message, "ns", 0),
					                     static_cast<uint32_t>(n6k::GetInt(message, "id", 0)),
					                     static_cast<uint8_t>(n6k::GetInt(message, "op", 0))});
				}
			}
		}
		eof.store(true);
	}

	int client_fd = -1;
	int peer_fd = -1;
	std::thread reader;
	std::mutex lock;
	std::mutex write_lock;
	std::string token;
	std::function<void(const PeerRequest &)> on_request;
	std::atomic<int64_t> open_ns {0};
	std::atomic<int64_t> last_pong {-1};
	std::atomic<int64_t> next_ping {1000};
	std::atomic<bool> eof {false};
};

} // namespace n6k_test
} // namespace duckdb
