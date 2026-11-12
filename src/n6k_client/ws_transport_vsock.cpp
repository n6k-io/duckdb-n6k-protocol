#ifdef WASM_LOADABLE_EXTENSIONS

#include "ws_transport_vsock.hpp"

#include "n6k_pump_thread.hpp"
#include "n6k_str_utils.hpp"
#include "n6k_wasm_main_thread.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdlib>
#include <cstring>

namespace duckdb {
namespace n6k {

// Mirrors PUMP_RELEASED in packages/npm/src/glue/pump-status.ts.
static constexpr int32_t N6K_PUMP_RELEASED = 4;

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

	const auto pointers = std::to_string(ring_ptr) + ", " +
	                      std::to_string(reinterpret_cast<uintptr_t>(N6kIoThread::Instance().DoorbellAddress()));
	const std::string args = "'" + key_ + "', '" + EscapeSingleQuotedJsLiteral(connection_id_) + "', " + pointers;
	// n6k.* calls go to the main runtime thread: under threads>1 this can run on a compute pthread, whose JS scope
	// has no globalThis.n6k (see n6k_wasm_main_thread.hpp).
	if (ws_url_.empty()) {
		auto result = RunScriptStringOnMain("n6k.vsockAdopt(" + args + ")");
		if (result != "OK") {
			FailStart(ring, nullptr, result);
		}
	} else {
		StartDial(ring, args, pointers);
	}
	attached_ = true;
	// Frames sent before the socket opens wait in the TX ring; the pump flushes them once it is open.
	if (on_open) {
		on_open();
	}
}

void VsockTransport::StartDial(void *ring, const std::string &args, const std::string &pointers) {
	// The pump reports socket status through this word; the main glue frees it once the pump has released.
	auto *status = static_cast<int32_t *>(std::calloc(1, sizeof(int32_t)));
	if (!status) {
		FailStart(ring, nullptr, "failed to allocate the status word");
	}
	const auto status_ptr = std::to_string(reinterpret_cast<uintptr_t>(status));
	auto result = RunScriptStringOnMain("n6k.vsockDial(" + args + ", " + status_ptr + ")");
	if (result != "OK") {
		FailStart(ring, status, result);
	}
	result = N6kPumpThread::Instance().Call("n6kPump.dial('" + key_ + "', " + pointers + ", " + status_ptr + ", '" +
	                                        EscapeSingleQuotedJsLiteral(ws_url_) + "')");
	if (result != "OK") {
		RunScriptOnMain("n6k.vsockClose('" + key_ + "')");
		// No pump will release it, so release it here: the main glue frees the word on this bit.
		__atomic_or_fetch(status, N6K_PUMP_RELEASED, __ATOMIC_RELEASE);
		__builtin_wasm_memory_atomic_notify(status, static_cast<int32_t>(-1));
		FailStart(ring, nullptr, result);
	}
}

void VsockTransport::FailStart(void *ring, int32_t *status, const std::string &reason) {
	N6kIoThread::Instance().UnregisterAndAwaitQuiesce(handle_);
	std::free(ring);
	std::free(status);
	stopped_.store(true);
	throw IOException("n6k: vsock attach failed: %s", reason.empty() ? "no reply" : reason);
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
	if (!ws_url_.empty()) {
		// The pump drains, closes the socket and frees the ring.
		const bool queued = N6kPumpThread::Instance().Post("n6kPump.close('" + key_ + "')");
		D_ASSERT(queued);
		(void)queued;
	}
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
