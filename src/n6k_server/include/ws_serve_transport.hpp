#pragma once

#include "serve_transport.hpp"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <utility>

namespace ix {
class WebSocket;
} // namespace ix

namespace duckdb {
namespace n6k {

// One accepted WebSocket client, as a transport the reactor can pull from.
//
// ixwebsocket PUSHES messages at a callback on its own per-client thread, while ServeReactor
// PULLS. This queue is the adapter between the two: the callback thread calls EnqueueFrameUnlessClosed, the
// reactor thread drains it through WaitReadable/ReadFrame.
//
// Note there is NO length prefix here, unlike UdsConnection — a WebSocket message already IS a
// frame boundary. Frame type = WS opcode (protocol.py Stream framing).
class WsServeTransport : public ServeTransport {
public:
	explicit WsServeTransport(ix::WebSocket &ws);

	WsServeTransport(const WsServeTransport &) = delete;
	WsServeTransport &operator=(const WsServeTransport &) = delete;

	bool WaitReadable(int timeout_ms) override;
	bool ReadFrame(std::string &out, StreamFrameType &type) override;
	FrameRead ReadFrameBy(std::string &out, StreamFrameType &type,
	                      std::chrono::steady_clock::time_point deadline) override;
	void WriteFrame(StreamFrameType type, const std::string &frame) override;
	void ShutdownWrite() override;
	void SendClose(int code, const std::string &reason) override;

	// Hand one received frame to the reactor. Called from ixwebsocket's callback thread.
	void EnqueueFrameUnlessClosed(StreamFrameType type, std::string frame);

	// Drains what is queued, then reports EOF from ReadFrame, which unwinds the reactor exactly as a
	// closed Unix socket does. Nothing is sent after it returns: ix may destroy the WebSocket next.
	void CloseInbound();

private:
	ix::WebSocket &ws_;

	std::mutex mu_;
	std::condition_variable cv_;
	std::deque<std::pair<StreamFrameType, std::string>> in_q_;
	bool closed_ = false;

	std::mutex send_mu_;
	bool send_closed_ = false;
};

} // namespace n6k
} // namespace duckdb
