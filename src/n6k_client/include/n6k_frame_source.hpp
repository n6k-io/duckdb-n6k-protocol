#pragma once

// Shared machinery for PhysicalOperator sources that drive an n6k RequestState frame-by-frame
// instead of through a blocking ArrowArrayStream: the per-scan state (request + decoder + reused
// ArrowScanLocalState + terminal flags) and the non-blocking drain.
//
// The point is that a source with nothing buffered returns SourceResultType::BLOCKED with NO task,
// so the worker goes fully idle; RequestState::SetOnFrame then fires interrupt_state.Callback()
// when the next frame lands and reschedules the parked pipeline. That matters most on
// wasm, where a blocking stream freezes everything for the whole round trip.

#include "n6k_arrow_frame_decoder.hpp"
#include "n6k_err_throw.hpp"
#include "n6k_protocol_generated.hpp"
#include "ws_client.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/parallel/interrupt.hpp"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace duckdb {

// Everything a frame-driven source needs per execution. Subclass to add operator-specific state.
struct N6kFrameSourceState : public GlobalSourceState {
	explicit N6kFrameSourceState(ClientContext &ctx) : scan_state(make_uniq<ArrowArrayWrapper>(), ctx) {
	}

	~N6kFrameSourceState() override {
		if (req) {
			req->SetOnFrame({}); // stop waking a state that's being destroyed
			if (!stream_done) {
				req->Cancel(); // abort an in-flight request (LIMIT reached, error elsewhere, teardown)
			}
		}
	}

	std::shared_ptr<n6k::RequestState> req;
	n6k::N6kArrowFrameDecoder decoder;
	ArrowScanLocalState scan_state; // reused across batches; holds the current batch + chunk_offset

	bool has_batch = false;   // scan_state.chunk holds an un-exhausted batch
	bool stream_done = false; // a terminal frame (RESP_END / EOS / ERR) was seen, or decoding failed
	bool saw_error = false;
	bool err_from_frame = false; // error source: server ERR frame (typed throw) vs decoder failure
	std::string err_payload;     // ERR frame body → typed rethrow (may be empty)
	std::string err_text;        // decoder/transport failure text

	idx_t MaxThreads() override {
		return 1;
	}
};

// Drain every currently-buffered frame into the decoder. Non-blocking; returns early once a batch
// is available so credits stay paced and latency low.
inline void N6kDrainFrames(N6kFrameSourceState &state) {
	n6k::Frame f;
	while (state.req->TryNext(f)) {
		if (f.type == n6k::FrameType::ERR) {
			state.saw_error = true;
			state.err_from_frame = true;
			state.err_payload = f.payload;
			state.stream_done = true;
			return;
		}
		if (f.type == n6k::FrameType::RESP_SCHEMA || f.type == n6k::FrameType::RESP_CHUNK) {
			if (!state.decoder.PushFrame(f.payload)) {
				state.saw_error = true;
				state.err_text = state.decoder.ErrorMessage();
				state.stream_done = true;
				return;
			}
			if (f.type == n6k::FrameType::RESP_CHUNK) {
				state.req->Credit(1); // one credit per consumed chunk keeps the server's window open
			}
		} else if (f.type == n6k::FrameType::RESP_END) {
			state.stream_done = true;
		}
		if ((f.flags & n6k::FLAG_END_OF_STREAM) != 0) {
			state.stream_done = true;
		}
		if (state.decoder.HasBatch()) {
			return;
		}
	}
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
