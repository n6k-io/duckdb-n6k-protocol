#pragma once

#include "n6k_protocol_generated.hpp"

#include <chrono>
#include <string>

namespace duckdb {
namespace n6k {

enum class FrameRead { FRAME, CLOSED, TIMED_OUT };

// What ServeReactor needs from a connection; framing is owned entirely by the transport.
class ServeTransport {
public:
	virtual ~ServeTransport() = default;

	// Block up to timeout_ms for a frame to be available; false on timeout, so the reader loop
	// gets a chance to notice an interrupt on an otherwise idle peer.
	virtual bool WaitReadable(int timeout_ms) = 0;

	// Read one whole frame; false on orderly EOF (which ends the session), throws on hard error.
	virtual bool ReadFrame(std::string &out, StreamFrameType &type) = 0;

	virtual FrameRead ReadFrameBy(std::string &out, StreamFrameType &type,
	                              std::chrono::steady_clock::time_point deadline) = 0;

	// Write one whole frame; throws on error. Never called concurrently: before Run() starts the
	// writer thread only the reader writes (the handshake), after that only the writer does, so
	// implementations need no send-side lock of their own.
	virtual void WriteFrame(StreamFrameType type, const std::string &frame) = 0;

	virtual void ShutdownWrite() = 0;
};

} // namespace n6k
} // namespace duckdb
