#pragma once

// Push-based Arrow-IPC frame decoder. The blocking scan drives nanoarrow's PULL-based stream reader
// (ArrowIpcArrayStreamReaderInit), whose read() callback blocks with no "resume me" return, so it can
// never yield the single wasm worker. This decoder inverts that: the caller PUSHES one WebSocket frame
// payload at a time via PushFrame(), and each fully-decoded record batch is pulled off with
// TryPopBatch(). No call ever blocks, so an async source operator can hand its thread back between
// frames and resume when the next one lands.
//
// It handles the REAL pyarrow framing: pyarrow flushes the schema lazily, so the RESP_SCHEMA frame is EMPTY and the
// schema IPC message is PREPENDED to the first RESP_CHUNK — one frame can carry TWO complete messages. PushFrame
// therefore treats each payload as a message STREAM: loop DecodeHeader -> (DecodeSchema | DecodeArray), advancing by
// header_size_bytes + body_size_bytes, until the payload is consumed.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

// C Data Interface structs (defined identically by nanoarrow and duckdb under the shared
// ARROW_C_DATA_INTERFACE guard); forward-declared here so the header stays nanoarrow-free.
struct ArrowArray;
struct ArrowSchema;

namespace duckdb {
namespace n6k {

class N6kArrowFrameDecoder {
public:
	N6kArrowFrameDecoder();
	~N6kArrowFrameDecoder();
	N6kArrowFrameDecoder(const N6kArrowFrameDecoder &) = delete;
	N6kArrowFrameDecoder &operator=(const N6kArrowFrameDecoder &) = delete;

	// Decode every complete IPC message contained in one frame payload (schema and/or record batches).
	// An empty payload (pyarrow's lazy RESP_SCHEMA) is a no-op. An EOS message flips SawEndOfStream(). Returns
	// false and records ErrorMessage() on any nanoarrow failure; the decoder must not be used after that.
	bool PushFrame(const uint8_t *data, size_t len);
	bool PushFrame(const std::string &payload) {
		return PushFrame(reinterpret_cast<const uint8_t *>(payload.data()), payload.size());
	}

	// Move the oldest decoded record batch into `out` (caller then owns and must release it). Returns
	// false when no batch is queued.
	bool TryPopBatch(ArrowArray *out);

	bool HasBatch() const;
	bool HasSchema() const;
	bool SawEndOfStream() const;

	// The decoded stream schema, valid once HasSchema(); nullptr before. Owned by the decoder.
	const ArrowSchema *GetSchema() const;

	// Non-empty after PushFrame() returns false.
	const std::string &ErrorMessage() const;

private:
	struct State;
	std::unique_ptr<State> state_;
};

} // namespace n6k
} // namespace duckdb
