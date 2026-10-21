#include "n6k_err_throw.hpp"
#include "n6k_ws_arrow_stream.hpp"
#include "n6k_protocol_generated.hpp"
#include "ws_client.hpp"
#include "ws_json.hpp"

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <string>

namespace duckdb {
namespace n6k {

namespace {

struct WsStreamPrivate {
	std::shared_ptr<RequestState> req;
	std::string cur;
	size_t cur_off = 0;
	bool saw_chunk = false;
	bool done = false;
	std::string err_msg;
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

bool ReplenishCreditAndAdvanceToPayload(WsStreamPrivate *priv) {
	if (priv->saw_chunk) {
		priv->req->Credit(1);
		priv->saw_chunk = false;
	}

	while (true) {
		Frame f;
		if (!WaitNextFrameUntilTerminalOrCancel(priv, f)) {
			// Terminal → mark done (no spurious CANCEL); client cancel leaves done=false so release sends CANCEL.
			if (priv->req->Exhausted()) {
				priv->done = true;
				if (!priv->req->ErrorMessage().empty()) {
					priv->err_msg = priv->req->ErrorMessage();
				}
			}
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
			priv->done = true;
			return false;
		case FrameType::ERR:
			priv->done = true;
			priv->err_msg = ParseErrFrame(f.payload).message;
			return false;
		default:
			break;
		}
	}
}

ArrowErrorCode WsStreamRead(struct ArrowIpcInputStream *stream, uint8_t *buf, int64_t buf_size_bytes,
                            int64_t *size_read_out, struct ArrowError *error) {
	(void)error;
	auto *priv = static_cast<WsStreamPrivate *>(stream->private_data);
	int64_t written = 0;
	while (written < buf_size_bytes) {
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
	if (written == 0 && !priv->err_msg.empty() && error != nullptr) {
		ArrowErrorSet(error, "n6k ws stream error: %s", priv->err_msg.c_str());
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

} // namespace

void StartStreamingArrowFromRequestState(const std::string &catalog, uint8_t op, std::shared_ptr<RequestState> req,
                                         ArrowArrayStream *out, std::function<bool()> should_cancel) {
	auto priv = std::unique_ptr<WsStreamPrivate>(new WsStreamPrivate());
	priv->req = std::move(req);
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
			priv->done = true;
		}
	} else {
		priv->done = true;
		if (!priv->req->ErrorMessage().empty()) {
			priv->err_msg = priv->req->ErrorMessage();
		}
	}

	ArrowIpcInputStream input_stream;
	input_stream.read = WsStreamRead;
	input_stream.release = WsStreamReleaseAndCancelIfUnfinished;
	input_stream.private_data = priv.release();

	auto rc = ArrowIpcArrayStreamReaderInit(out, &input_stream, nullptr);
	if (rc != NANOARROW_OK) {
		if (input_stream.release) {
			input_stream.release(&input_stream);
		}
		throw IOException("n6k[%s] %s: ArrowIpcArrayStreamReaderInit failed", catalog, OpName(op));
	}
}

} // namespace n6k
} // namespace duckdb
