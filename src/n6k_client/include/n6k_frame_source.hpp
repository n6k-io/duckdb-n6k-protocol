#pragma once

// Per-request state for driving an n6k RequestState
// frame-by-frame instead of through a blocking ArrowArrayStream, and its non-blocking drain.

#include "n6k_arrow_frame_decoder.hpp"
#include "n6k_err_throw.hpp"
#include "n6k_protocol_generated.hpp"
#include "ws_client.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace duckdb {

struct N6kFrameSourceState {
	~N6kFrameSourceState() {
		if (req) {
			req->SetOnFrame({}); // stop waking a state that's being destroyed
			if (!stream_done) {
				req->Cancel(); // abort an in-flight request (LIMIT reached, error elsewhere, teardown)
			}
		}
	}

	std::shared_ptr<n6k::RequestState> req;
	n6k::N6kArrowFrameDecoder decoder;

	bool stream_done = false; // a terminal frame (RESP_END / EOS / ERR) was seen, or decoding failed
	bool saw_error = false;
	bool err_from_frame = false; // error source: server ERR frame (typed throw) vs decoder failure
	std::string err_payload;     // ERR frame body → typed rethrow (may be empty)
	std::string err_text;        // decoder/transport failure text
};

// Drain every currently-buffered frame into the decoder. Non-blocking; returns early once a batch
// is available so credits stay paced and latency low. Returns the chunks consumed: the caller
// credits them, outside any lock of its own.
inline uint32_t N6kDrainFrames(N6kFrameSourceState &state) {
	uint32_t consumed = 0;
	n6k::Frame f;
	while (state.req->TryNext(f)) {
		if (f.type == n6k::FrameType::ERR) {
			state.saw_error = true;
			state.err_from_frame = true;
			state.err_payload = f.payload;
			state.stream_done = true;
			return consumed;
		}
		if (f.type == n6k::FrameType::RESP_SCHEMA || f.type == n6k::FrameType::RESP_CHUNK) {
			if (!state.decoder.PushFrame(f.payload)) {
				state.saw_error = true;
				state.err_text = state.decoder.ErrorMessage();
				state.stream_done = true;
				return consumed;
			}
			if (f.type == n6k::FrameType::RESP_CHUNK) {
				consumed++;
			}
		} else if (f.type == n6k::FrameType::RESP_END) {
			state.stream_done = true;
		}
		if ((f.flags & n6k::FLAG_END_OF_STREAM) != 0) {
			state.stream_done = true;
		}
		if (state.decoder.HasBatch()) {
			return consumed;
		}
	}
	return consumed;
}

// Rethrow a terminal error. A server ERR frame is always the typed throw, even with an empty
// payload (ThrowN6kError handles that); anything else is a decoder/transport failure.
[[noreturn]] inline void N6kThrowSourceError(const N6kFrameSourceState &state, const string &catalog, uint8_t op,
                                             const string &label) {
	if (state.err_from_frame) {
		n6k::ThrowN6kError(catalog, op, state.err_payload);
	}
	throw IOException("n6k[" + catalog + "] " + label + ": " + state.err_text);
}

} // namespace duckdb
