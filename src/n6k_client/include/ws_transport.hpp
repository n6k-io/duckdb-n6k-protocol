#pragma once

#include <functional>
#include <string>

namespace duckdb {
namespace n6k {

// Moves whole messages over one connection; their contents are opaque to it.
// One Send()/on_message == exactly one message (WebSocket gets this free; a raw socket must length-frame).
class Transport {
public:
	// `binary`: an n6k frame; otherwise a JSON-RPC text message.
	using MessageFn = std::function<void(const std::string &bytes, bool binary)>;
	using CloseFn = std::function<void(const std::string &reason)>;
	// The endpoint is writable: everything Send() is handed from here on can reach the peer. Fires at
	// most once; a transport already reading may deliver on_message first. A transport that loses a Send()
	// made before its handshake completes MUST defer this until then; one that holds such frames until the
	// peer is reachable may fire it at once.
	using OpenFn = std::function<void()>;

	virtual ~Transport() = default;

	// Wire callbacks and begin pumping inbound. Never waits on the peer, but VsockTransport waits on the main
	// runtime thread. Callbacks may run on a transport thread. on_open may fire before Start() returns or long
	// after: see OpenFn.
	virtual void Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) = 0;

	// Send one complete n6k frame. Safe from several threads at once; one message never interleaves with
	// another. Returns false when the frame was refused, which callers must surface rather than swallow; true
	// means accepted, not on the wire. May throw on a frame the transport cannot carry (VsockTransport::Send).
	virtual bool Send(const std::string &frame) = 0;
	// As Send, for one JSON-RPC text message.
	virtual bool SendText(const std::string &text) = 0;

	// Idempotent. Not from within an on_message/on_close callback.
	virtual void Stop() = 0;

	// Ends the connection without waiting for the transport's threads, so it is safe inside a callback. Stop() is
	// still required. A transport with no such close (VsockTransport) keeps delivering until Stop().
	virtual void Shutdown() {
	}
};

} // namespace n6k
} // namespace duckdb
