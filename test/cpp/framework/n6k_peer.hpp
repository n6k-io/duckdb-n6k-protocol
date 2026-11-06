#pragma once

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_stream_framing.hpp"

#include <chrono>
#include <cstdint>
#include <poll.h>
#include <string>
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

} // namespace n6k_test
} // namespace duckdb
