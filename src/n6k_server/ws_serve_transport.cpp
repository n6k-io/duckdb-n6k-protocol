#include "ws_serve_transport.hpp"

#include "duckdb/common/exception.hpp"

#include "ixwebsocket/IXWebSocket.h"

#include <chrono>
#include <utility>

namespace duckdb {
namespace n6k {

WsServeTransport::WsServeTransport(ix::WebSocket &ws) : ws_(ws) {
}

void WsServeTransport::EnqueueFrameUnlessClosed(StreamFrameType type, std::string frame) {
	{
		std::lock_guard<std::mutex> lk(mu_);
		// Dropped rather than queued: nothing will ever read it, and queueing after close would
		// keep ReadFrame from reporting the EOF the reactor is waiting for.
		if (closed_) {
			return;
		}
		in_q_.emplace_back(type, std::move(frame));
	}
	cv_.notify_all();
}

void WsServeTransport::CloseInbound() {
	{
		std::lock_guard<std::mutex> lk(mu_);
		closed_ = true;
	}
	cv_.notify_all();
	std::lock_guard<std::mutex> lk(send_mu_);
	send_closed_ = true;
}

bool WsServeTransport::WaitReadable(int timeout_ms) {
	std::unique_lock<std::mutex> lk(mu_);
	if (!in_q_.empty() || closed_) {
		return true;
	}
	cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] { return !in_q_.empty() || closed_; });
	return !in_q_.empty() || closed_;
}

bool WsServeTransport::ReadFrame(std::string &out, StreamFrameType &type) {
	std::unique_lock<std::mutex> lk(mu_);
	cv_.wait(lk, [&] { return !in_q_.empty() || closed_; });
	// Queued frames win over a close: a client that sends its last request and immediately
	// disconnects must still have that request answered, not dropped.
	if (in_q_.empty()) {
		return false;
	}
	type = in_q_.front().first;
	out = std::move(in_q_.front().second);
	in_q_.pop_front();
	return true;
}

FrameRead WsServeTransport::ReadFrameBy(std::string &out, StreamFrameType &type,
                                        std::chrono::steady_clock::time_point deadline) {
	std::unique_lock<std::mutex> lk(mu_);
	if (!cv_.wait_until(lk, deadline, [&] { return !in_q_.empty() || closed_; })) {
		return FrameRead::TIMED_OUT;
	}
	if (in_q_.empty()) {
		return FrameRead::CLOSED;
	}
	type = in_q_.front().first;
	out = std::move(in_q_.front().second);
	in_q_.pop_front();
	return FrameRead::FRAME;
}

void WsServeTransport::WriteFrame(StreamFrameType type, const std::string &frame) {
	std::lock_guard<std::mutex> lk(send_mu_);
	if (send_closed_) {
		throw IOException("n6k_server: the client's WebSocket is closed");
	}
	auto sent = type == StreamFrameType::JSONRPC ? ws_.sendUtf8Text(frame) : ws_.sendBinary(frame);
	if (!sent.success) {
		throw IOException("n6k_server: sending a %llu byte frame to the client failed",
		                  static_cast<uint64_t>(frame.size()));
	}
}

void WsServeTransport::ShutdownWrite() {
	std::lock_guard<std::mutex> lk(send_mu_);
	if (!send_closed_) {
		ws_.close();
	}
}

void WsServeTransport::SendClose(int code, const std::string &reason) {
	std::lock_guard<std::mutex> lk(send_mu_);
	if (!send_closed_) {
		ws_.close(static_cast<uint16_t>(code), reason);
	}
}

} // namespace n6k
} // namespace duckdb
