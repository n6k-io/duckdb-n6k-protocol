#include "uds_transport.hpp"

#include "n6k_stream_framing.hpp"

#include "duckdb/common/exception.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace duckdb {
namespace n6k {

// Bound a corrupt/hostile length prefix; 1 GiB is far above any real frame.
static constexpr uint32_t MAX_FRAME_BYTES = 1u << 30;

UdsConnection::~UdsConnection() {
	Close();
}

void UdsConnection::Close() {
	if (fd_ >= 0) {
		::close(fd_);
		fd_ = -1;
	}
}

bool UdsPathFits(const std::string &path) {
	return path.size() < sizeof(sockaddr_un::sun_path);
}

uint64_t UdsPathMax() {
	return sizeof(sockaddr_un::sun_path) - 1;
}

static sockaddr_un UdsAddress(const std::string &path) {
	if (!UdsPathFits(path)) {
		throw IOException("n6k_server: socket path too long (%lu bytes, max %lu): %s",
		                  static_cast<uint64_t>(path.size()), UdsPathMax(), path);
	}
	sockaddr_un addr;
	std::memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	std::memcpy(addr.sun_path, path.c_str(), path.size());
	return addr;
}

void UdsConnection::AdoptBlockingDupOfFd(int host_fd) {
	if (host_fd < 0) {
		throw IOException("n6k_server: cannot adopt file descriptor %d", host_fd);
	}
	int fd = ::dup(host_fd);
	if (fd < 0) {
		throw IOException("n6k_server: dup(%d) failed: %s", host_fd, std::strerror(errno));
	}
	Adopt(fd);
}

void UdsConnection::Adopt(int fd) {
#ifdef SO_NOSIGPIPE
	// macOS: suppress SIGPIPE at socket level (Linux uses MSG_NOSIGNAL per send). Without this a
	// write to a hung-up peer kills the host process, which for an adopted fd is not ours to kill.
	int on = 1;
	::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
	// ReadExactOrThrow/WriteFrame retry EINTR but not EAGAIN, so the fd has to be blocking. A host that
	// handed the socket to an event loop first will have marked its copy non-blocking, and on BSDs an
	// accepted fd inherits the listener's O_NONBLOCK.
	int flags = ::fcntl(fd, F_GETFL, 0);
	if (flags >= 0 && (flags & O_NONBLOCK) != 0) {
		if (::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) < 0) {
			int err = errno;
			::close(fd);
			throw IOException("n6k_server: could not clear O_NONBLOCK on fd %d: %s", fd, std::strerror(err));
		}
	}
	Close();
	fd_ = fd;
}

static bool PollReadableBy(int fd, std::chrono::steady_clock::time_point deadline) {
	for (;;) {
		auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
		if (left.count() <= 0) {
			return false;
		}
		struct pollfd pfd;
		pfd.fd = fd;
		pfd.events = POLLIN;
		pfd.revents = 0;
		int rc = ::poll(&pfd, 1, static_cast<int>(left.count()));
		if (rc > 0) {
			return true;
		}
		if (rc < 0 && errno != EINTR) {
			throw IOException("n6k_server: poll() failed: %s", std::strerror(errno));
		}
	}
}

// CLOSED on clean EOF before any byte; TIMED_OUT once `deadline` passes; throws on partial/hard error.
static FrameRead ReadExactOrThrow(int fd, char *buf, size_t n, const std::chrono::steady_clock::time_point *deadline) {
	size_t got = 0;
	while (got < n) {
		if (deadline && !PollReadableBy(fd, *deadline)) {
			return FrameRead::TIMED_OUT;
		}
		ssize_t r = ::read(fd, buf + got, n - got);
		if (r == 0) {
			if (got == 0) {
				return FrameRead::CLOSED;
			}
			throw IOException("n6k_server: peer closed mid-frame (%lu of %lu bytes)", static_cast<uint64_t>(got),
			                  static_cast<uint64_t>(n));
		}
		if (r < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("n6k_server: read() failed: %s", std::strerror(errno));
		}
		got += static_cast<size_t>(r);
	}
	return FrameRead::FRAME;
}

static FrameRead ReadFrameFrom(int fd, std::string &out, StreamFrameType &type,
                               const std::chrono::steady_clock::time_point *deadline) {
	char header[STREAM_HEADER_BYTES];
	auto rc = ReadExactOrThrow(fd, header, STREAM_HEADER_BYTES, deadline);
	if (rc != FrameRead::FRAME) {
		return rc;
	}
	type = StreamFrameTypeOf(header, "n6k_server");
	uint32_t len = StreamPayloadLength(header);
	if (len > MAX_FRAME_BYTES) {
		throw IOException("n6k_server: frame length %u exceeds limit %u", len, MAX_FRAME_BYTES);
	}
	out.resize(len);
	if (len == 0) {
		return FrameRead::FRAME;
	}
	rc = ReadExactOrThrow(fd, &out[0], len, deadline);
	if (rc == FrameRead::CLOSED) {
		throw IOException("n6k_server: peer closed before frame body (%u bytes expected)", len);
	}
	return rc;
}

bool UdsConnection::WaitReadable(int timeout_ms) {
	struct pollfd pfd;
	pfd.fd = fd_;
	pfd.events = POLLIN;
	pfd.revents = 0;
	int rc;
	do {
		rc = ::poll(&pfd, 1, timeout_ms);
	} while (rc < 0 && errno == EINTR);
	if (rc < 0) {
		throw IOException("n6k_server: poll() failed: %s", std::strerror(errno));
	}
	// POLLHUP/POLLERR also mean "readable" — ReadFrame will observe the EOF/error.
	return rc > 0 && (pfd.revents & (POLLIN | POLLHUP | POLLERR)) != 0;
}

bool UdsConnection::ReadFrame(std::string &out, StreamFrameType &type) {
	return ReadFrameFrom(fd_, out, type, nullptr) == FrameRead::FRAME;
}

FrameRead UdsConnection::ReadFrameBy(std::string &out, StreamFrameType &type,
                                     std::chrono::steady_clock::time_point deadline) {
	return ReadFrameFrom(fd_, out, type, &deadline);
}

void UdsConnection::WriteFrame(StreamFrameType type, const std::string &frame) {
	// Header + body in one contiguous buffer, so the partial-send loop below resumes mid-frame.
	std::string buf;
	buf.reserve(STREAM_HEADER_BYTES + frame.size());
	AppendStreamHeader(buf, type, frame.size());
	buf.append(frame);

	size_t sent = 0;
	while (sent < buf.size()) {
#ifdef MSG_NOSIGNAL
		ssize_t w = ::send(fd_, buf.data() + sent, buf.size() - sent, MSG_NOSIGNAL);
#else
		ssize_t w = ::write(fd_, buf.data() + sent, buf.size() - sent);
#endif
		if (w < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("n6k_server: write() failed: %s", std::strerror(errno));
		}
		sent += static_cast<size_t>(w);
	}
}

void UdsConnection::ShutdownWrite() {
	if (fd_ >= 0) {
		::shutdown(fd_, SHUT_WR);
	}
}

void UdsConnection::CloseInbound() {
	if (fd_ >= 0) {
		::shutdown(fd_, SHUT_RD);
	}
}

// A socket file outlives a crashed server, and only a refused connect tells a stale one from a live one.
// The connect alone can't be trusted with the unlink: Linux refuses a connect to any non-socket inode, so
// a mistyped path naming a regular file would be deleted.
static void ClaimSocketPath(const std::string &path, const sockaddr_un &addr) {
	struct stat st;
	if (::lstat(path.c_str(), &st) == 0 && !S_ISSOCK(st.st_mode)) {
		throw IOException("n6k_server: %s exists and is not a socket", path);
	}
	int probe = ::socket(AF_UNIX, SOCK_STREAM, 0);
	if (probe < 0) {
		throw IOException("n6k_server: socket() failed: %s", std::strerror(errno));
	}
	int rc;
	do {
		rc = ::connect(probe, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr));
	} while (rc < 0 && errno == EINTR);
	int err = errno;
	::close(probe);
	if (rc == 0) {
		throw IOException("n6k_server: %s is in use by another server", path);
	}
	if (err == ECONNREFUSED && ::unlink(path.c_str()) < 0 && errno != ENOENT) {
		throw IOException("n6k_server: could not remove stale socket %s: %s", path, std::strerror(errno));
	}
}

UdsListener::UdsListener(const std::string &path) : path_(path) {
	auto addr = UdsAddress(path);
	ClaimSocketPath(path, addr);
	int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		throw IOException("n6k_server: socket() failed: %s", std::strerror(errno));
	}
	::fcntl(fd, F_SETFD, FD_CLOEXEC);
	// Non-blocking so a client that leaves between poll() and accept() cannot park the accept loop past
	// an interrupt.
	::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
	if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
		int err = errno;
		::close(fd);
		throw IOException("n6k_server: bind(%s) failed: %s", path, std::strerror(err));
	}
	if (::chmod(path.c_str(), 0600) < 0 || ::listen(fd, SOMAXCONN) < 0) {
		int err = errno;
		::close(fd);
		::unlink(path.c_str());
		throw IOException("n6k_server: listen(%s) failed: %s", path, std::strerror(err));
	}
	fd_ = fd;
}

UdsListener::~UdsListener() {
	::close(fd_);
	::unlink(path_.c_str());
}

int UdsListener::Accept(int timeout_ms) {
	struct pollfd pfd;
	pfd.fd = fd_;
	pfd.events = POLLIN;
	pfd.revents = 0;
	int rc = ::poll(&pfd, 1, timeout_ms);
	if (rc < 0 && errno != EINTR) {
		throw IOException("n6k_server: poll() failed: %s", std::strerror(errno));
	}
	if (rc <= 0) {
		return -1;
	}
	int fd = ::accept(fd_, nullptr, nullptr);
	if (fd < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR || errno == ECONNABORTED) {
			return -1;
		}
		throw IOException("n6k_server: accept(%s) failed: %s", path_, std::strerror(errno));
	}
	::fcntl(fd, F_SETFD, FD_CLOEXEC);
	return fd;
}

} // namespace n6k
} // namespace duckdb
