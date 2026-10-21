#include "n6k_arrow_frame_decoder.hpp"

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <cerrno>
#include <deque>

namespace duckdb {
namespace n6k {

struct N6kArrowFrameDecoder::State {
	ArrowIpcDecoder decoder {};
	ArrowSchema schema {}; // owned once schema_set
	// encodings holds pointers into schema; dictionaries holds the decoded dictionary values that
	// record batches reference by id. Popped batches carry refcounted clones of those values, so
	// they stay valid after this State is destroyed.
	ArrowIpcDictionaryEncodings encodings {};
	ArrowIpcDictionaries dictionaries {};
	bool schema_set = false;
	bool decoder_init = false;      // decoder needs ArrowIpcDecoderReset
	bool encodings_init = false;    // encodings needs ArrowIpcDictionaryEncodingsReset
	bool dictionaries_init = false; // dictionaries needs ArrowIpcDictionariesReset
	bool done = false;              // saw the end-of-stream message
	bool failed = false;            // a PushFrame failed; decoder is poisoned
	std::string error_message;      // non-empty only once failed; the nanoarrow error text
	std::deque<ArrowArray> batches; // decoded record batches awaiting TryPopBatch, oldest first

	~State() {
		for (auto &batch : batches) {
			if (batch.release) {
				batch.release(&batch);
			}
		}
		if (dictionaries_init) {
			ArrowIpcDictionariesReset(&dictionaries);
		}
		if (encodings_init) {
			ArrowIpcDictionaryEncodingsReset(&encodings);
		}
		if (schema_set && schema.release) {
			schema.release(&schema);
		}
		if (decoder_init) {
			ArrowIpcDecoderReset(&decoder);
		}
	}
};

N6kArrowFrameDecoder::N6kArrowFrameDecoder() : state_(new State()) {
	if (ArrowIpcDecoderInit(&state_->decoder) != NANOARROW_OK) {
		state_->failed = true;
		state_->error_message = "ArrowIpcDecoderInit failed";
		return;
	}
	state_->decoder_init = true;
}

N6kArrowFrameDecoder::~N6kArrowFrameDecoder() = default;

bool N6kArrowFrameDecoder::PushFrame(const uint8_t *data, size_t len) {
	auto &st = *state_;
	if (st.failed) {
		return false;
	}

	ArrowError err;
	size_t off = 0;
	while (off < len) {
		ArrowBufferView view;
		view.data.as_uint8 = data + off;
		view.size_bytes = static_cast<int64_t>(len - off);

		err.message[0] = '\0';
		int rc = ArrowIpcDecoderDecodeHeader(&st.decoder, view, &err);
		if (rc == ENODATA) {
			// End-of-stream marker: an empty message body signals the writer closed the stream.
			st.done = true;
			return true;
		}
		if (rc != NANOARROW_OK) {
			st.failed = true;
			st.error_message = std::string(err.message);
			return false;
		}
		const size_t msg_total =
		    static_cast<size_t>(st.decoder.header_size_bytes) + static_cast<size_t>(st.decoder.body_size_bytes);

		if (st.decoder.message_type == NANOARROW_IPC_MESSAGE_TYPE_SCHEMA) {
			err.message[0] = '\0';
			if (ArrowIpcDecoderDecodeSchema(&st.decoder, &st.schema, &err) != NANOARROW_OK) {
				st.failed = true;
				st.error_message = std::string(err.message);
				return false;
			}
			if (st.dictionaries_init) {
				ArrowIpcDictionariesReset(&st.dictionaries);
				st.dictionaries_init = false;
			}
			if (st.encodings_init) {
				ArrowIpcDictionaryEncodingsReset(&st.encodings);
				st.encodings_init = false;
			}
			ArrowIpcDictionaryEncodingsInit(&st.encodings);
			st.encodings_init = true;
			if (ArrowIpcDictionaryEncodingsAppendSchema(&st.encodings, &st.schema) != NANOARROW_OK) {
				st.failed = true;
				st.error_message = "could not collect the schema's dictionary encodings";
				return false;
			}
			if (ArrowIpcDecoderSetEndianness(&st.decoder, st.decoder.endianness) != NANOARROW_OK ||
			    ArrowIpcDecoderSetSchemaWithDictionaries(&st.decoder, &st.schema, &st.encodings, &err) !=
			        NANOARROW_OK) {
				st.failed = true;
				st.error_message = std::string(err.message);
				return false;
			}
			err.message[0] = '\0';
			if (ArrowIpcDictionariesInit(&st.dictionaries, &st.encodings, &err) != NANOARROW_OK) {
				st.failed = true;
				st.error_message = std::string(err.message);
				return false;
			}
			st.dictionaries_init = true;
			st.schema_set = true;
		} else if (st.decoder.message_type == NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH) {
			if (!st.schema_set) {
				st.failed = true;
				st.error_message = "dictionary batch arrived before schema";
				return false;
			}
			ArrowBufferView body;
			body.data.as_uint8 = data + off + st.decoder.header_size_bytes;
			body.size_bytes = st.decoder.body_size_bytes;
			err.message[0] = '\0';
			if (ArrowIpcDecoderDecodeDictionary(&st.decoder, body, NANOARROW_VALIDATION_LEVEL_DEFAULT, &st.dictionaries,
			                                    &err) != NANOARROW_OK) {
				st.failed = true;
				st.error_message = std::string(err.message);
				return false;
			}
		} else if (st.decoder.message_type == NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
			if (!st.schema_set) {
				st.failed = true;
				st.error_message = "record batch arrived before schema";
				return false;
			}
			ArrowBufferView body;
			body.data.as_uint8 = data + off + st.decoder.header_size_bytes;
			body.size_bytes = st.decoder.body_size_bytes;
			ArrowArray array {};
			err.message[0] = '\0';
			if (ArrowIpcDecoderDecodeArrayWithDictionaries(&st.decoder, body, -1, &st.dictionaries, &array,
			                                               NANOARROW_VALIDATION_LEVEL_DEFAULT, &err) != NANOARROW_OK) {
				st.failed = true;
				st.error_message = std::string(err.message);
				return false;
			}
			st.batches.push_back(array);
		}
		// Tensor/sparse-tensor messages never reach here: DecodeHeader refuses them with ENOTSUP.
		off += msg_total;
	}
	return true;
}

bool N6kArrowFrameDecoder::TryPopBatch(ArrowArray *out) {
	auto &st = *state_;
	if (st.batches.empty()) {
		return false;
	}
	*out = st.batches.front();            // transfer the release callback to the caller...
	st.batches.front().release = nullptr; // ...so the deque drop below is a no-op.
	st.batches.pop_front();
	return true;
}

bool N6kArrowFrameDecoder::HasBatch() const {
	return !state_->batches.empty();
}

bool N6kArrowFrameDecoder::HasSchema() const {
	return state_->schema_set;
}

bool N6kArrowFrameDecoder::SawEndOfStream() const {
	return state_->done;
}

const ArrowSchema *N6kArrowFrameDecoder::GetSchema() const {
	return state_->schema_set ? &state_->schema : nullptr;
}

const std::string &N6kArrowFrameDecoder::ErrorMessage() const {
	return state_->error_message;
}

} // namespace n6k
} // namespace duckdb
