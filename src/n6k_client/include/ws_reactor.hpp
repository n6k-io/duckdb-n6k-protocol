#pragma once

// Reactor: the one platform-specific seam of WsClient (native threads vs wasm SAB-ring pump).
// WaitUntil contract: predicates must self-lock and MUST NOT be called with a client mutex held.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace duckdb {
namespace n6k {

struct WsClientOptions;

class Reactor {
public:
	// One inbound frame (msgpack header + optional raw body); wired to WsClient::OnMessage.
	using FrameFn = std::function<void(const std::string &bytes, bool binary)>;
	using CloseFn = std::function<void(const std::string &reason)>;

	virtual ~Reactor() = default;

	// Wire callbacks and begin moving frames. Non-blocking: does NOT wait for the handshake reply.
	virtual void Start(FrameFn on_frame, CloseFn on_close) = 0;

	// Enqueue one complete outbound frame. Thread-safe; non-blocking enqueue.
	virtual void Send(const std::string &frame) = 0;

	// Block until ready() or deadline; returns ready(). Invoked with no client mutex held (see contract).
	virtual bool WaitUntil(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) = 0;

	// Idempotent teardown; wakes a blocked WaitUntil. Not from within on_frame/on_close.
	virtual void Stop() = 0;

	// Reconnect variant: stop delivering but do NOT close the channel or fire on_close.
	virtual void RetireKeepingChannelOpen() {
		Stop();
	}

	// Transport channel reset counter (wasm vsock epoch, bumped on host replaceWebsocket). Native: 0.
	virtual uint64_t Epoch() {
		return 0;
	}
};

std::unique_ptr<Reactor> CreateReactor(const WsClientOptions &opts);

} // namespace n6k
} // namespace duckdb
