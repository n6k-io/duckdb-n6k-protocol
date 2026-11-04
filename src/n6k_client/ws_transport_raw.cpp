#ifndef WASM_LOADABLE_EXTENSIONS
#ifndef _WIN32

#include "ws_transport_raw.hpp"

#include "n6k_stream_framing.hpp"

#include "duckdb/common/exception.hpp"

#include <cerrno>
#include <cstring>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {
namespace n6k {

// Bounds a corrupt/hostile length prefix; 1 GiB is far above any real frame.
static constexpr uint32_t MAX_FRAME_BYTES = 1u << 30;

RawSocketTransport::RawSocketTransport(int fd) {
	fd_ = ::dup(fd);
	if (fd_ < 0) {
		throw IOException("n6k: dup(%d) failed: %s", fd, std::strerror(errno));
	}
#ifdef SO_NOSIGPIPE
	// macOS: suppress SIGPIPE at the socket level (Linux uses MSG_NOSIGNAL per send).
	int on = 1;
	::setsockopt(fd_, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
	int wake[2] = {-1, -1};
	if (::pipe(wake) != 0) {
		::close(fd_);
		fd_ = -1;
		throw IOException("n6k: wake pipe failed: %s", std::strerror(errno));
	}
	wake_r_ = wake[0];
	wake_w_ = wake[1];
}

RawSocketTransport::~RawSocketTransport() {
	Stop();
	for (int *slot : {&fd_, &wake_r_, &wake_w_}) {
		if (*slot >= 0) {
			::close(*slot);
			*slot = -1;
		}
	}
}

void RawSocketTransport::Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) {
	reader_ = std::thread([this, on_message, on_close]() { ReaderLoop(on_message, on_close); });
	if (on_open) {
		on_open();
	}
}

void RawSocketTransport::Stop() {
	bool expected = false;
	if (!stopping_.compare_exchange_strong(expected, true)) {
		return;
	}
	const char byte = 1;
	ssize_t ignored = ::write(wake_w_, &byte, 1);
	(void)ignored;
	if (reader_.joinable()) {
		reader_.join();
	}
}

// Wait until `fd` is ready for `events`; false once Stop fires or poll fails.
static bool AwaitUnlessStopped(int fd, int16_t events, int wake_fd, const std::atomic<bool> &stopping) {
	for (;;) {
		struct pollfd fds[2];
		fds[0].fd = fd;
		fds[0].events = events;
		fds[0].revents = 0;
		fds[1].fd = wake_fd;
		fds[1].events = POLLIN;
		fds[1].revents = 0;
		int ready = ::poll(fds, 2, -1);
		if (ready < 0) {
			if (errno == EINTR) {
				continue;
			}
			return false;
		}
		return !stopping.load() && (fds[1].revents & POLLIN) == 0;
	}
}

static bool ReadExactUnlessStopped(int fd, int wake_fd, const std::atomic<bool> &stopping, char *buf, size_t n) {
	size_t got = 0;
	while (got < n) {
		if (!AwaitUnlessStopped(fd, POLLIN, wake_fd, stopping)) {
			return false;
		}
		ssize_t r = ::read(fd, buf + got, n - got);
		if (r == 0) {
			return false;
		}
		if (r < 0) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
			return false;
		}
		got += static_cast<size_t>(r);
	}
	return true;
}

void RawSocketTransport::ReaderLoop(const MessageFn &on_message, const CloseFn &on_close) {
	std::string reason = "peer closed the socket";
	std::string frame;
	for (;;) {
		char header[STREAM_HEADER_BYTES];
		if (!ReadExactUnlessStopped(fd_, wake_r_, stopping_, header, STREAM_HEADER_BYTES)) {
			break;
		}
		auto raw_type = static_cast<unsigned char>(header[4]);
		const bool binary = raw_type == static_cast<unsigned char>(StreamFrameType::N6K);
		if (!binary && raw_type != static_cast<unsigned char>(StreamFrameType::JSONRPC)) {
			reason = "unknown stream frame type " + std::to_string(raw_type);
			break;
		}
		uint32_t len = StreamPayloadLength(header);
		if (len > MAX_FRAME_BYTES) {
			reason = "frame of " + std::to_string(len) + " bytes exceeds the limit";
			break;
		}
		frame.resize(len);
		if (len > 0 && !ReadExactUnlessStopped(fd_, wake_r_, stopping_, &frame[0], len)) {
			break;
		}
		on_message(frame, binary);
	}
	if (stopping_.load()) {
		return;
	}
	// Nothing reads this socket again: without this, a host pump on the other end never sees EOF.
	::shutdown(fd_, SHUT_RDWR);
	if (on_close) {
		on_close(reason);
	}
}

bool RawSocketTransport::Send(const std::string &frame) {
	return SendFramed(StreamFrameType::N6K, frame);
}

bool RawSocketTransport::SendText(const std::string &text) {
	return SendFramed(StreamFrameType::JSONRPC, text);
}

bool RawSocketTransport::SendFramed(StreamFrameType type, const std::string &payload) {
	std::string buf;
	buf.reserve(STREAM_HEADER_BYTES + payload.size());
	AppendStreamHeader(buf, type, payload.size());
	buf.append(payload);

	std::lock_guard<std::mutex> lk(write_mu_);
	size_t sent = 0;
	while (sent < buf.size()) {
#ifdef MSG_NOSIGNAL
		ssize_t w = ::send(fd_, buf.data() + sent, buf.size() - sent, MSG_NOSIGNAL);
#else
		ssize_t w = ::write(fd_, buf.data() + sent, buf.size() - sent);
#endif
		if (w >= 0) {
			sent += static_cast<size_t>(w);
			continue;
		}
		if (errno == EINTR) {
			continue;
		}
		// The host may hand over a non-blocking socket.
		if ((errno == EAGAIN || errno == EWOULDBLOCK) && AwaitUnlessStopped(fd_, POLLOUT, wake_r_, stopping_)) {
			continue;
		}
		return false;
	}
	return true;
}

} // namespace n6k
} // namespace duckdb

#endif // !_WIN32
#endif // !WASM_LOADABLE_EXTENSIONS
