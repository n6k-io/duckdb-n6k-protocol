#pragma once

#ifndef WASM_LOADABLE_EXTENSIONS

#include <functional>
#include <string>

namespace duckdb {
namespace n6k {

// Message transport beneath WsClient; WsClient owns all n6k frame logic.
// One Send()/on_message == exactly one frame (WebSocket gets this free; raw-fd must length-frame).
class Transport {
public:
	using MessageFn = std::function<void(const std::string &bytes, bool binary)>;
	using CloseFn = std::function<void(const std::string &reason)>;
	// The endpoint is writable: everything Send() is handed from here on can reach the peer. Fires at
	// most once, before any on_message. A transport that dials asynchronously MUST defer this until the
	// handshake completes — Send() before it is not guaranteed to reach the wire.
	using OpenFn = std::function<void()>;

	virtual ~Transport() = default;

	// Wire callbacks and begin pumping inbound. Non-blocking; callbacks may run on a transport thread.
	// on_open may fire before Start() returns (an already-connected endpoint) or long after (a dial).
	virtual void Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) = 0;

	// Send one complete frame. Single-writer (the reactor's writer thread). Returns false when the frame
	// did NOT reach the socket, which callers must surface rather than swallow.
	virtual bool Send(const std::string &frame) = 0;

	// Idempotent. Not from within an on_message/on_close callback.
	virtual void Stop() = 0;
};

} // namespace n6k
} // namespace duckdb

#endif // !WASM_LOADABLE_EXTENSIONS
