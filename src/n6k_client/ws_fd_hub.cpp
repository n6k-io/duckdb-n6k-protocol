#ifndef WASM_LOADABLE_EXTENSIONS
#ifndef _WIN32

#include "ws_fd_hub.hpp"

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_stream_framing.hpp"

#include "duckdb/common/exception.hpp"

#include <cerrno>
#include <cstring>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace duckdb {
namespace n6k {

// Bounds a corrupt/hostile length prefix; 1 GiB is far above any real frame.
static constexpr uint32_t MAX_FRAME_BYTES = 1u << 30;

namespace {

std::mutex &RegistryMutex() {
	static std::mutex mu;
	return mu;
}

// Keyed by the caller's fd number. weak_ptr so the hub dies with its last session.
std::map<int, std::weak_ptr<FdHub>> &Registry() {
	static std::map<int, std::weak_ptr<FdHub>> reg;
	return reg;
}

// Outlives hubs: n6k_connect handshakes before any ATTACH creates one.
std::map<int, std::vector<std::string>> &HelloCapsRegistry() {
	static std::map<int, std::vector<std::string>> reg;
	return reg;
}

} // namespace

std::shared_ptr<FdHub> FdHub::Acquire(int host_fd) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	auto &reg = Registry();
	auto it = reg.find(host_fd);
	if (it != reg.end()) {
		if (auto existing = it->second.lock()) {
			return existing;
		}
	}
	// make_shared can't reach the private ctor; the raw new is immediately owned.
	std::shared_ptr<FdHub> hub(new FdHub(host_fd));
	reg[host_fd] = hub;
	return hub;
}

bool FdHub::IsLive(int host_fd) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	auto &reg = Registry();
	auto it = reg.find(host_fd);
	return it != reg.end() && !it->second.expired();
}

void FdHub::SetHelloCapabilities(int host_fd, std::vector<std::string> capabilities) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	HelloCapsRegistry()[host_fd] = std::move(capabilities);
}

std::vector<std::string> FdHub::HelloCapabilities(int host_fd) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	auto &reg = HelloCapsRegistry();
	auto it = reg.find(host_fd);
	return it != reg.end() ? it->second : std::vector<std::string>();
}

FdHub::FdHub(int host_fd) : host_fd_(host_fd) {
	// dup() so our copy survives until the last session drops, regardless of when the host closes its own.
	fd_ = ::dup(host_fd);
	if (fd_ < 0) {
		throw IOException("n6k: wsFd dup(%d) failed: %s", host_fd, std::strerror(errno));
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
		throw IOException("n6k: wsFd wake pipe failed: %s", std::strerror(errno));
	}
	wake_r_ = wake[0];
	wake_w_ = wake[1];
}

FdHub::~FdHub() {
	{
		std::lock_guard<std::mutex> lk(RegistryMutex());
		auto &reg = Registry();
		auto it = reg.find(host_fd_);
		// Erase only OUR slot: if an ATTACH raced our teardown it already owns this fd number, and its
		// entry is live (not expired) — dropping it would orphan a hub that has subscribers.
		if (it != reg.end() && it->second.expired()) {
			reg.erase(it);
		}
	}
	Shutdown();
}

void FdHub::Shutdown() {
	bool expected = false;
	if (!stopping_.compare_exchange_strong(expected, true)) {
		return;
	}
	if (wake_w_ >= 0) {
		// Wake a parked poll() without shutdown()ing the socket — that would tear the connection down
		// for every other session sharing this fd.
		const char byte = 1;
		ssize_t ignored = ::write(wake_w_, &byte, 1);
		(void)ignored;
	}
	if (reader_.joinable()) {
		reader_.join();
	}
	for (int *slot : {&fd_, &wake_r_, &wake_w_}) {
		if (*slot >= 0) {
			::close(*slot);
			*slot = -1;
		}
	}
}

void FdHub::BindSessionAndStartReader(uint64_t ns, MessageFn on_message, CloseFn on_close) {
	std::lock_guard<std::mutex> lk(subs_mu_);
	auto sub = std::make_shared<Subscriber>();
	sub->on_message = std::move(on_message);
	sub->on_close = std::move(on_close);
	subs_[ns] = std::move(sub);
	StartReaderLocked();
}

void FdHub::Unsubscribe(uint64_t ns) {
	std::lock_guard<std::mutex> lk(subs_mu_);
	subs_.erase(ns);
}

uint32_t FdHub::BindRequestAndStartReader(MessageFn on_message, CloseFn on_close) {
	std::lock_guard<std::mutex> lk(subs_mu_);
	auto sub = std::make_shared<Subscriber>();
	sub->on_message = std::move(on_message);
	sub->on_close = std::move(on_close);
	const uint32_t req_id = next_req_id_++;
	requests_[req_id] = std::move(sub);
	StartReaderLocked();
	return req_id;
}

void FdHub::UnbindRequest(uint32_t req_id) {
	std::lock_guard<std::mutex> lk(subs_mu_);
	requests_.erase(req_id);
}

void FdHub::StartReaderLocked() {
	if (!reader_started_) {
		reader_started_ = true;
		reader_ = std::thread(&FdHub::ReaderLoop, this);
	}
}

size_t FdHub::SubscriberCount() {
	std::lock_guard<std::mutex> lk(subs_mu_);
	return subs_.size();
}

// Read exactly `n` bytes, waking early if Shutdown fires; false on EOF, hard error, or stop.
static bool ReadExactUnlessStopped(int fd, int wake_fd, const std::atomic<bool> &stopping, char *buf, size_t n) {
	size_t got = 0;
	while (got < n) {
		struct pollfd fds[2];
		fds[0].fd = fd;
		fds[0].events = POLLIN;
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
		if (stopping.load() || (fds[1].revents & POLLIN) != 0) {
			return false;
		}
		ssize_t r = ::read(fd, buf + got, n - got);
		if (r == 0) {
			return false;
		}
		if (r < 0) {
			if (errno == EINTR || errno == EAGAIN) {
				continue;
			}
			return false;
		}
		got += static_cast<size_t>(r);
	}
	return true;
}

void FdHub::ReaderLoop() {
	std::string frame;
	for (;;) {
		char header[STREAM_HEADER_BYTES];
		if (!ReadExactUnlessStopped(fd_, wake_r_, stopping_, header, STREAM_HEADER_BYTES)) {
			break;
		}
		auto raw_type = static_cast<unsigned char>(header[4]);
		if (raw_type != static_cast<unsigned char>(StreamFrameType::N6K) &&
		    raw_type != static_cast<unsigned char>(StreamFrameType::JSONRPC)) {
			break;
		}
		uint32_t len = StreamPayloadLength(header);
		if (len > MAX_FRAME_BYTES) {
			break;
		}
		frame.resize(len);
		if (len > 0 && !ReadExactUnlessStopped(fd_, wake_r_, stopping_, &frame[0], len)) {
			break;
		}
		if (raw_type == static_cast<unsigned char>(StreamFrameType::JSONRPC)) {
			auto reply = JsonRpcMethodNotFoundReply(frame);
			if (!reply.empty()) {
				SendLengthPrefixed(StreamFrameType::JSONRPC, reply);
			}
			continue;
		}
		DispatchInbound(frame);
	}
	NotifySubscribersOfClose(stopping_.load() ? "wsFd transport stopped" : "wsFd peer closed");
}

void FdHub::DispatchInbound(const std::string &frame) {
	// Peek the header for `ns`. A frame we cannot parse is treated as untagged rather than dropped —
	// the session layer already tolerates junk, and dropping would hide a protocol change.
	int64_t ns = 0;
	int64_t type = -1;
	int64_t req_id = -1;
	msgpack::object_handle oh;
	try {
		size_t off = 0;
		oh = msgpack::unpack(frame.data(), frame.size(), off);
		if (oh.get().type == msgpack::type::MAP) {
			ns = GetInt(oh.get(), "ns", 0);
			type = GetInt(oh.get(), "t", -1);
			req_id = GetInt(oh.get(), "id", -1);
		}
	} catch (...) {
		ns = 0;
		type = -1;
	}
	// PING also carries an untagged `id`, so the frame type gates request routing.
	const bool is_response = type == static_cast<int64_t>(FrameType::RESP_CHUNK) ||
	                         type == static_cast<int64_t>(FrameType::RESP_END) ||
	                         type == static_cast<int64_t>(FrameType::ERR);

	// Collect targets under the lock, then call them without it: a callback runs arbitrary session code
	// and must not be able to deadlock against Subscribe/Unsubscribe.
	std::vector<std::shared_ptr<Subscriber>> targets;
	{
		std::lock_guard<std::mutex> lk(subs_mu_);
		auto request =
		    (ns == 0 && is_response && req_id >= 0) ? requests_.find(static_cast<uint32_t>(req_id)) : requests_.end();
		if (request != requests_.end()) {
			targets.push_back(request->second);
		} else if (ns != 0) {
			auto it = subs_.find(static_cast<uint64_t>(ns));
			if (it != subs_.end()) {
				targets.push_back(it->second);
			}
			// An unknown ns is dropped, never broadcast: a sibling must not see another session's frames.
		} else if (subs_.size() == 1) {
			// Untagged frame with one session bound — it can only be for that session.
			targets.push_back(subs_.begin()->second);
		} else {
			// Untagged with several bound: PING and HELLO_ERR are connection-scoped and carry no ns, so
			// every session is a legitimate recipient.
			for (auto &entry : subs_) {
				targets.push_back(entry.second);
			}
		}
	}
	for (auto &sub : targets) {
		sub->on_message(frame, /*binary=*/true);
	}
}

void FdHub::NotifySubscribersOfClose(const std::string &reason) {
	std::vector<std::shared_ptr<Subscriber>> targets;
	{
		std::lock_guard<std::mutex> lk(subs_mu_);
		for (auto &entry : subs_) {
			targets.push_back(entry.second);
		}
		for (auto &entry : requests_) {
			targets.push_back(entry.second);
		}
	}
	for (auto &sub : targets) {
		if (sub->on_close) {
			sub->on_close(reason);
		}
	}
}

bool FdHub::SendLengthPrefixed(StreamFrameType type, const std::string &frame) {
	std::string buf;
	buf.reserve(STREAM_HEADER_BYTES + frame.size());
	AppendStreamHeader(buf, type, frame.size());
	buf.append(frame);

	// One frame must not interleave with any other writer's: hold the lock for the whole write.
	std::lock_guard<std::mutex> lk(write_mu_);
	if (fd_ < 0) {
		return false;
	}
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
			return false;
		}
		sent += static_cast<size_t>(w);
	}
	return true;
}

} // namespace n6k
} // namespace duckdb

#endif // !_WIN32
#endif // !WASM_LOADABLE_EXTENSIONS
