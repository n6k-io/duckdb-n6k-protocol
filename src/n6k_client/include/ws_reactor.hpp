#pragma once

// Reactor: one catalog session's channel beneath WsClient — frames tagged with one `ns`, over a connection
// a ConnectionHub owns. WsClient owns OPEN and everything after.

#include <cstdint>
#include <functional>
#include <string>

namespace duckdb {
namespace n6k {

class Reactor {
public:
	using FrameFn = std::function<void(const std::string &bytes, bool binary)>;
	using CloseFn = std::function<void(const std::string &reason)>;

	virtual ~Reactor() = default;

	// Frames may be delivered before this returns.
	virtual void Start(FrameFn on_frame, CloseFn on_close) = 0;

	// Thread-safe, ordered, never blocks. A frame that cannot be delivered closes the channel instead, unless the
	// transport throws on it (Transport::Send).
	virtual void Send(const std::string &frame) = 0;

	// Idempotent. Returns once no callback is running or will run, then releases the session's `ns` on the
	// server. Not from within on_frame/on_close.
	virtual void Close() = 0;

	// Changes when the connection beneath was replaced: a session opened on an older one must reopen.
	virtual uint64_t Generation() = 0;
};

} // namespace n6k
} // namespace duckdb
