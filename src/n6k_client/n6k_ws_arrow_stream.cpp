#include "n6k_err_throw.hpp"
#include "n6k_ws_arrow_stream.hpp"
#include "n6k_protocol_generated.hpp"
#include "ws_client.hpp"
#include "ws_json.hpp"

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <string>

namespace duckdb {
namespace n6k {

namespace {

enum class StreamFailureKind { NONE, SERVER_ERROR, SERVER_CANCELLED, ENDED_WITHOUT_RESULT, INTERRUPTED };

struct StreamFailure {
	StreamFailureKind kind = StreamFailureKind::NONE;
	std::string err_payload;
};

struct WsStreamPrivate {
	std::shared_ptr<RequestState> req;
	std::shared_ptr<StreamFailure> failure;
	std::string cur;
	size_t cur_off = 0;
	bool saw_chunk = false;
	bool done = false;
	// Polled between wait slices so an idle stream tears down promptly; empty → wait for terminal.
	std::function<bool()> should_cancel;
};

constexpr std::chrono::seconds kFrameWaitSlice {1};

// An idle gap is never EOF: a slice that expires with the stream still open just waits again.
bool WaitNextFrameUntilTerminalOrCancel(WsStreamPrivate *priv, Frame &f) {
	while (true) {
		if (priv->req->Next(f, kFrameWaitSlice)) {
			return true;
		}
		if (priv->req->Exhausted()) {
			return false;
		}
		if (priv->should_cancel && priv->should_cancel()) {
			return false;
		}
	}
}

[[noreturn]] void ThrowStreamFailure(const std::string &catalog, uint8_t op, const StreamFailure &failure) {
	switch (failure.kind) {
	case StreamFailureKind::SERVER_ERROR:
		ThrowN6kError(catalog, op, failure.err_payload);
	case StreamFailureKind::SERVER_CANCELLED:
		throw IOException("n6k[%s] %s: server cancelled the request", catalog, OpName(op));
	case StreamFailureKind::ENDED_WITHOUT_RESULT:
		throw IOException("n6k[%s] %s: the request ended without a result", catalog, OpName(op));
	case StreamFailureKind::INTERRUPTED:
		throw InterruptException();
	case StreamFailureKind::NONE:
		break;
	}
	throw InternalException("n6k[%s] %s: no stream failure to throw", catalog, OpName(op));
}

// Called when no frame will come. Interrupted leaves `done` false so release still sends CANCEL.
void RecordStreamStopped(WsStreamPrivate *priv) {
	if (priv->req->Exhausted()) {
		priv->done = true;
		priv->failure->kind = StreamFailureKind::ENDED_WITHOUT_RESULT;
	} else {
		priv->failure->kind = StreamFailureKind::INTERRUPTED;
	}
}

void RecordRespEnd(WsStreamPrivate *priv, const Frame &end) {
	priv->done = true;
	JsonDoc doc(end.payload);
	if (doc.Parsed() && JsonGetBool(doc.Root(), "cancelled")) {
		priv->failure->kind = StreamFailureKind::SERVER_CANCELLED;
	}
}

bool ReplenishCreditAndAdvanceToPayload(WsStreamPrivate *priv) {
	if (priv->done) {
		return false;
	}
	if (priv->saw_chunk) {
		priv->req->Credit(1);
		priv->saw_chunk = false;
	}

	while (true) {
		Frame f;
		if (!WaitNextFrameUntilTerminalOrCancel(priv, f)) {
			RecordStreamStopped(priv);
			return false;
		}
		switch (f.type) {
		case FrameType::RESP_SCHEMA:
		case FrameType::RESP_CHUNK:
			priv->cur = std::move(f.payload);
			priv->cur_off = 0;
			priv->saw_chunk = (f.type == FrameType::RESP_CHUNK);
			return true;
		case FrameType::RESP_END:
			RecordRespEnd(priv, f);
			return false;
		case FrameType::ERR:
			priv->done = true;
			priv->failure->kind = StreamFailureKind::SERVER_ERROR;
			priv->failure->err_payload = std::move(f.payload);
			return false;
		default:
			break;
		}
	}
}

// Runs under nanoarrow's C frames, so it must not throw: it records the failure and returns an error,
// because 0 bytes with NANOARROW_OK at a message boundary is nanoarrow's clean end of stream.
ArrowErrorCode WsStreamRead(struct ArrowIpcInputStream *stream, uint8_t *buf, int64_t buf_size_bytes,
                            int64_t *size_read_out, struct ArrowError *error) {
	auto *priv = static_cast<WsStreamPrivate *>(stream->private_data);
	int64_t written = 0;
	while (written < buf_size_bytes && priv->failure->kind == StreamFailureKind::NONE) {
		if (priv->cur_off >= priv->cur.size()) {
			if (!ReplenishCreditAndAdvanceToPayload(priv)) {
				break;
			}
		}
		size_t remaining = priv->cur.size() - priv->cur_off;
		size_t want = static_cast<size_t>(buf_size_bytes - written);
		size_t n = remaining < want ? remaining : want;
		std::memcpy(buf + written, priv->cur.data() + priv->cur_off, n);
		priv->cur_off += n;
		written += static_cast<int64_t>(n);
	}
	*size_read_out = written;
	if (priv->failure->kind != StreamFailureKind::NONE) {
		ArrowErrorSet(error, "n6k: stream failed");
		return EIO;
	}
	return NANOARROW_OK;
}

void WsStreamReleaseAndCancelIfUnfinished(struct ArrowIpcInputStream *stream) {
	auto *priv = static_cast<WsStreamPrivate *>(stream->private_data);
	if (priv) {
		if (priv->req && !priv->done) {
			// Peek for an already-enqueued RESP_END/ERR to avoid a spurious CANCEL on a completed stream.
			Frame f;
			while (priv->req->Next(f, std::chrono::milliseconds(0))) {
				if (f.type == FrameType::RESP_END || f.type == FrameType::ERR) {
					priv->done = true;
					break;
				}
			}
			if (!priv->done) {
				priv->req->Cancel();
			}
		}
		delete priv;
	}
	stream->private_data = nullptr;
	stream->read = nullptr;
	stream->release = nullptr;
}

struct WsArrayStream {
	ArrowArrayStream inner;
	std::shared_ptr<StreamFailure> failure;
	std::string catalog;
	uint8_t op;
};

// The outer callbacks' callers are all C++, so unlike WsStreamRead they can throw, which keeps the typed exception
// that DuckDB's arrow_scan would otherwise flatten to InvalidInputException.
int WsArrayStreamChecked(WsArrayStream &self, int rc) {
	if (rc != 0 && self.failure->kind != StreamFailureKind::NONE) {
		ThrowStreamFailure(self.catalog, self.op, *self.failure);
	}
	return rc;
}

int WsArrayStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	auto &self = *static_cast<WsArrayStream *>(stream->private_data);
	return WsArrayStreamChecked(self, self.inner.get_schema(&self.inner, out));
}

int WsArrayStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	auto &self = *static_cast<WsArrayStream *>(stream->private_data);
	return WsArrayStreamChecked(self, self.inner.get_next(&self.inner, out));
}

const char *WsArrayStreamGetLastError(struct ArrowArrayStream *stream) {
	auto &self = *static_cast<WsArrayStream *>(stream->private_data);
	return self.inner.get_last_error(&self.inner);
}

void WsArrayStreamRelease(struct ArrowArrayStream *stream) {
	auto *self = static_cast<WsArrayStream *>(stream->private_data);
	self->inner.release(&self->inner);
	delete self;
	stream->private_data = nullptr;
	stream->release = nullptr;
}

} // namespace

void StartStreamingArrowFromRequestState(const std::string &catalog, uint8_t op, std::shared_ptr<RequestState> req,
                                         ArrowArrayStream *out, std::function<bool()> should_cancel) {
	auto priv = std::unique_ptr<WsStreamPrivate>(new WsStreamPrivate());
	priv->req = std::move(req);
	priv->failure = std::make_shared<StreamFailure>();
	priv->should_cancel = std::move(should_cancel);

	// Pre-consume the first frame so an early ERR throws the typed DuckDB exception before nanoarrow flattens it to
	// IOException.
	Frame first;
	if (WaitNextFrameUntilTerminalOrCancel(priv.get(), first)) {
		if (first.type == FrameType::ERR) {
			ThrowN6kError(catalog, op, first.payload);
		}
		if (first.type == FrameType::RESP_SCHEMA || first.type == FrameType::RESP_CHUNK) {
			priv->cur = std::move(first.payload);
			priv->cur_off = 0;
			priv->saw_chunk = (first.type == FrameType::RESP_CHUNK);
		} else if (first.type == FrameType::RESP_END) {
			RecordRespEnd(priv.get(), first);
		}
	} else {
		RecordStreamStopped(priv.get());
		priv->req->Cancel();
	}
	if (priv->failure->kind != StreamFailureKind::NONE) {
		ThrowStreamFailure(catalog, op, *priv->failure);
	}

	auto array_stream = std::unique_ptr<WsArrayStream>(new WsArrayStream());
	array_stream->failure = priv->failure;
	array_stream->catalog = catalog;
	array_stream->op = op;

	ArrowIpcInputStream input_stream;
	input_stream.read = WsStreamRead;
	input_stream.release = WsStreamReleaseAndCancelIfUnfinished;
	input_stream.private_data = priv.release();

	auto rc = ArrowIpcArrayStreamReaderInit(&array_stream->inner, &input_stream, nullptr);
	if (rc != NANOARROW_OK) {
		if (input_stream.release) {
			input_stream.release(&input_stream);
		}
		throw IOException("n6k[%s] %s: ArrowIpcArrayStreamReaderInit failed", catalog, OpName(op));
	}

	out->get_schema = WsArrayStreamGetSchema;
	out->get_next = WsArrayStreamGetNext;
	out->get_last_error = WsArrayStreamGetLastError;
	out->release = WsArrayStreamRelease;
	out->private_data = array_stream.release();
}

} // namespace n6k
} // namespace duckdb
