#include "n6k_fd_hello.hpp"

#if !defined(_WIN32)

#include "n6k_protocol_generated.hpp"
#include "n6k_stream_framing.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {
namespace n6k {

static constexpr int HANDSHAKE_TIMEOUT_MS = 5000;
static constexpr uint32_t MAX_HANDSHAKE_FRAME_BYTES = 1u << 20;
static constexpr int ABORT_POLL_MS = 250;

using Clock = std::chrono::steady_clock;

static int RemainingMs(Clock::time_point deadline) {
	auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
	return left > 0 ? static_cast<int>(left) : 0;
}

static void AwaitReady(int fd, decltype(pollfd::events) events, const FdReplyWait &wait) {
	for (;;) {
		if (wait.context.IsInterrupted()) {
			throw InterruptException();
		}
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = events;
		pfd.revents = 0;
		int rc = ::poll(&pfd, 1, std::min(RemainingMs(wait.deadline), ABORT_POLL_MS));
		if (rc > 0) {
			return;
		}
		if (rc == 0) {
			if (Clock::now() >= wait.deadline) {
				throw IOException("%s: %s", wait.fn_name, wait.on_timeout);
			}
			continue;
		}
		if (errno != EINTR) {
			throw IOException("%s: poll() failed: %s", wait.fn_name, std::strerror(errno));
		}
	}
}

static void WriteAll(int fd, const std::string &buf, const FdReplyWait &wait) {
	size_t sent = 0;
	while (sent < buf.size()) {
#ifdef MSG_NOSIGNAL
		ssize_t w = ::send(fd, buf.data() + sent, buf.size() - sent, MSG_NOSIGNAL);
#else
		ssize_t w = ::write(fd, buf.data() + sent, buf.size() - sent);
#endif
		if (w >= 0) {
			sent += static_cast<size_t>(w);
			continue;
		}
		if (errno == EINTR) {
			continue;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			AwaitReady(fd, POLLOUT, wait);
			continue;
		}
		throw IOException("%s: writing to the peer failed: %s", wait.fn_name, std::strerror(errno));
	}
}

static void ReadExact(int fd, char *buf, size_t n, const FdReplyWait &wait) {
	size_t got = 0;
	while (got < n) {
		AwaitReady(fd, POLLIN, wait);
		ssize_t r = ::read(fd, buf + got, n - got);
		if (r == 0) {
			throw IOException("%s: peer closed the socket", wait.fn_name);
		}
		if (r < 0) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
			throw IOException("%s: reading from the peer failed: %s", wait.fn_name, std::strerror(errno));
		}
		got += static_cast<size_t>(r);
	}
}

void WriteN6kFrameOnFd(int fd, const std::string &frame, const FdReplyWait &wait) {
	std::string buf;
	AppendStreamHeader(buf, StreamFrameType::N6K, frame.size());
	buf.append(frame);
	WriteAll(fd, buf, wait);
}

msgpack::object_handle ReadN6kFrameOnFd(int fd, const FdReplyWait &wait) {
	char header[STREAM_HEADER_BYTES];
	ReadExact(fd, header, STREAM_HEADER_BYTES, wait);
	if (static_cast<unsigned char>(header[4]) != static_cast<unsigned char>(StreamFrameType::N6K)) {
		throw IOException("%s: peer sent a non-n6k frame (type 0x%02x)", wait.fn_name,
		                  static_cast<unsigned>(static_cast<unsigned char>(header[4])));
	}
	const uint32_t len = StreamPayloadLength(header);
	if (len == 0 || len > MAX_HANDSHAKE_FRAME_BYTES) {
		throw IOException("%s: peer sent a frame of invalid length %u", wait.fn_name, len);
	}
	std::string frame(len, '\0');
	ReadExact(fd, &frame[0], len, wait);

	msgpack::object_handle oh;
	try {
		size_t off = 0;
		oh = msgpack::unpack(frame.data(), frame.size(), off);
	} catch (const std::exception &e) {
		throw IOException("%s: malformed frame from the peer: %s", wait.fn_name, e.what());
	}
	if (oh.get().type != msgpack::type::MAP) {
		throw IOException("%s: malformed frame from the peer: header is not a msgpack map", wait.fn_name);
	}
	return oh;
}

std::vector<std::string> HelloOnFd(ClientContext &context, int fd, const std::string &token, const char *fn_name) {
	FdReplyWait wait {context, fn_name, Clock::now() + std::chrono::milliseconds(HANDSHAKE_TIMEOUT_MS),
	                  "no reply to FT_HELLO within " + std::to_string(HANDSHAKE_TIMEOUT_MS) + "ms"};

	WriteN6kFrameOnFd(fd, PackHello(static_cast<int>(FrameType::HELLO), token, HELLO_CAPS), wait);

	msgpack::object_handle oh;
	bool pending = false;
	std::string pending_message;
	for (;;) {
		oh = ReadN6kFrameOnFd(fd, wait);
		if (static_cast<FrameType>(GetInt(oh.get(), "t", -1)) != FrameType::HELLO_PENDING) {
			break;
		}
		pending = true;
		pending_message = GetStr(oh.get(), "message");
		wait.deadline = Clock::now() + std::chrono::milliseconds(OPEN_PENDING_STALL_MS);
		wait.on_timeout = "handshake stalled: no FT_HELLO_PENDING for " + std::to_string(OPEN_PENDING_STALL_MS) +
		                  "ms (last status: " + pending_message + ")";
	}
	const msgpack::object &h = oh.get();
	const auto type = static_cast<FrameType>(GetInt(h, "t", -1));
	if (type == FrameType::HELLO_ERR) {
		const auto etype = GetStr(h, "exception_type");
		const auto emsg = GetStr(h, "exception_message");
		auto message = etype.empty() ? emsg : etype + ": " + emsg;
		if (pending) {
			message += " (handshake was pending: " + pending_message + ")";
		}
		throw IOException("%s: %s", fn_name, message);
	}
	if (type != FrameType::HELLO_ACK) {
		throw IOException("%s: expected FT_HELLO_ACK, got frame type %d", fn_name, static_cast<int>(type));
	}
	const auto version = GetInt(h, "protocol_version", 0);
	if (version != N6K_PROTOCOL_VERSION) {
		throw IOException("%s: server protocol_version=%d, expected %d", fn_name, static_cast<int>(version),
		                  static_cast<int>(N6K_PROTOCOL_VERSION));
	}
	return GetStrArray(h, "capabilities");
}

} // namespace n6k
} // namespace duckdb

#endif
