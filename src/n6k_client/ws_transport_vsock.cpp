#ifdef WASM_LOADABLE_EXTENSIONS

#include "ws_transport_vsock.hpp"

#include "n6k_str_utils.hpp"
#include "n6k_wasm_main_thread.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdlib>
#include <cstring>

namespace duckdb {
namespace n6k {

// MUST equal channelBytes(DEFAULT_CHANNEL_LAYOUT) in packages/npm/src/virtual-socket/channel.ts: both sides
// build views over these exact bytes.
static constexpr size_t N6K_VSOCK_CHANNEL_BYTES = 2 * static_cast<size_t>(N6K_RING_BYTES);

static std::string NextTransportKey() {
	static std::atomic<uint64_t> next {1};
	return "vsock_" + std::to_string(next.fetch_add(1));
}

VsockTransport::VsockTransport(std::string connection_id, std::string ws_url)
    : key_(NextTransportKey()), connection_id_(std::move(connection_id)), ws_url_(std::move(ws_url)) {
}

VsockTransport::~VsockTransport() {
	Stop();
}

void VsockTransport::Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) {
	// In duckdb's shared heap so the pump worker addresses the same bytes. Zeroed so the control words start
	// empty and open. Once attached, JS owns it and frees it after the pump releases the ring: freeing it
	// here would race the pump's async teardown.
	void *ring = std::malloc(N6K_VSOCK_CHANNEL_BYTES);
	if (!ring) {
		throw IOException("n6k: failed to allocate %llu-byte vsock ring region",
		                  static_cast<unsigned long long>(N6K_VSOCK_CHANNEL_BYTES));
	}
	std::memset(ring, 0, N6K_VSOCK_CHANNEL_BYTES);
	const auto ring_ptr = reinterpret_cast<uintptr_t>(ring);
	MakeVsockChannelRings(ring_ptr, ring0_, ring1_);

	N6kIoThread::Registration reg;
	reg.ring0 = ring0_;
	reg.ring1 = ring1_;
	reg.pinned_epoch = ring1_.Epoch();
	reg.on_frame = [on_message](const std::string &frame, bool) {
		on_message(frame, /*binary=*/true);
	};
	reg.on_close = [on_close](const char *reason) {
		on_close(reason ? reason : "vsock channel closed");
	};
	handle_ = N6kIoThread::Instance().Register(std::move(reg));

	const auto doorbell = reinterpret_cast<uintptr_t>(N6kIoThread::Instance().DoorbellAddress());
	const std::string args = "'" + key_ + "', '" + EscapeSingleQuotedJsLiteral(connection_id_) + "', " +
	                         std::to_string(ring_ptr) + ", " + std::to_string(doorbell);
	// Main runtime thread: under threads>1 this can run on a compute pthread, whose JS scope has no
	// globalThis.n6k (see n6k_wasm_main_thread.hpp).
	auto result = RunScriptStringOnMain(ws_url_.empty() ? "n6k.vsockAdopt(" + args + ")"
	                                                    : "n6k.vsockDial(" + args + ", '" +
	                                                          EscapeSingleQuotedJsLiteral(ws_url_) + "')");
	if (result != "OK") {
		N6kIoThread::Instance().UnregisterAndAwaitQuiesce(handle_);
		std::free(ring);
		stopped_.store(true);
		throw IOException("n6k: vsock attach failed: %s", result.empty() ? "no reply" : result);
	}
	attached_ = true;
	// Frames sent before the socket opens wait in the TX ring; the pump flushes them once it is open.
	if (on_open) {
		on_open();
	}
}

bool VsockTransport::Send(const std::string &frame) {
	if (stopped_.load()) {
		return false;
	}
	if (N6K_RING_LEN_BYTES + frame.size() > N6K_RING_CAPACITY) {
		throw IOException("n6k vsock: frame %llu B + prefix exceeds ring capacity %u",
		                  static_cast<unsigned long long>(frame.size()), N6K_RING_CAPACITY);
	}
	N6kIoThread::Instance().Enqueue(handle_, frame);
	return true;
}

bool VsockTransport::SendText(const std::string &) {
	return false;
}

void VsockTransport::Stop() {
	if (stopped_.exchange(true) || !attached_) {
		return;
	}
	// Blocks until the I/O thread acks it will never touch these rings again; only then may JS free them.
	N6kIoThread::Instance().UnregisterAndAwaitQuiesce(handle_);
	ring0_.Close();
	ring1_.Close();
	RunScriptOnMain("n6k.vsockClose('" + key_ + "')");
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
