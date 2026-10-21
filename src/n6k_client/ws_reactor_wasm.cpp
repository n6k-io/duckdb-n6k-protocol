#ifdef WASM_LOADABLE_EXTENSIONS

// WsClient seam. The reactor registers its heap rings with the single shared n6k I/O thread
// (n6k_io_thread.hpp), which across ALL catalogs is the sole consumer of RX ring1 (drains each frame
// -> on_frame_ -> WsClient::OnMessage -> Push, and wakes a parked WaitUntil) AND the sole producer of
// TX ring0. Send just enqueues (non-blocking) and rings the doorbell; the I/O thread TryWrites, so no
// producer ever blocks the shared thread. No JS on any of these paths, so the I/O thread is safe with
// no globalThis.n6k.

#include "n6k_io_thread.hpp"
#include "n6k_str_utils.hpp"
#include "n6k_vsock_ring.hpp"
#include "n6k_wasm_main_thread.hpp" // RunScriptOnMain
#include "ws_client.hpp"            // WsClientOptions
#include "ws_reactor.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>

namespace duckdb {
namespace n6k {

namespace {

class WasmReactor : public Reactor {
public:
	WasmReactor(std::string catalog, uintptr_t ring_ptr) : catalog_(std::move(catalog)) {
		MakeVsockChannelRings(ring_ptr, ring0_, ring1_); // ring0 = SEND (duckdb->ws), ring1 = RECV
	}
	~WasmReactor() override {
		Stop();
	}

	void Start(FrameFn on_frame, CloseFn on_close) override {
		on_frame_ = std::move(on_frame);
		on_close_ = std::move(on_close);
		// Register ring1 with the shared I/O thread; it becomes ring1's sole consumer. Callbacks
		// are set above first, and Unregister (Stop/RetireKeepingChannelOpen) quiesces the I/O thread off this ring
		// before they're cleared, so it can never call a stale callback.
		N6kIoThread::Registration reg;
		reg.ring0 = ring0_;
		reg.ring1 = ring1_;
		reg.pinned_epoch = ring1_.Epoch();
		reg.on_frame = on_frame_;
		reg.on_progress = [this] {
			NotifyProgress();
		};
		reg.on_close = [this](const char *reason) {
			FireCloseOnce(reason);
		};
		handle_ = N6kIoThread::Instance().Register(std::move(reg));
		registered_ = true;
	}

	void Send(const std::string &frame) override {
		if (stopped_.load()) {
			return;
		}
		// Enqueue for the shared I/O thread (ring0's sole producer); never touches a ring, never
		// blocks. Reject an unfittable frame here on the caller's thread so the oversized case never
		// reaches the shared thread.
		if (N6K_RING_LEN_BYTES + frame.size() > N6K_RING_CAPACITY) {
			throw IOException("n6k vsock: frame %llu B + prefix exceeds ring capacity %u",
			                  static_cast<unsigned long long>(frame.size()), N6K_RING_CAPACITY);
		}
		N6kIoThread::Instance().Enqueue(handle_, frame);
	}

	bool WaitUntil(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) override {
		// The shared I/O thread drains ring1 and NotifyProgress()es; just park until a delivered
		// frame satisfies the predicate, the reactor stops, or the deadline passes. `ready`
		// self-locks client state and is safe under progress_mu_ (the I/O thread never holds
		// progress_mu_ while locking client state — it Push()es, then NotifyProgress separately).
		std::unique_lock<std::mutex> lk(progress_mu_);
		progress_cv_.wait_until(lk, deadline, [&] { return ready() || stopped_.load(); });
		return ready();
	}

	uint64_t Epoch() override {
		return ring0_.Epoch();
	}

	void Stop() override {
		if (torn_down_.exchange(true)) {
			return; // once-only teardown (stopped_ may already be set by the shared I/O thread on a drop)
		}
		stopped_.store(true);
		// Unregister BLOCKS until the shared I/O thread acks it will never touch ring1 again (the
		// quiesce barrier). Only then is it safe to Close + free: the I/O thread is provably off this
		// ring before vsockClose posts n6k-vsock-detach -> pump-ring-released -> capturedWasmFree. Close
		// signals the pump (CLOSED).
		if (registered_) {
			N6kIoThread::Instance().UnregisterAndAwaitQuiesce(handle_);
			registered_ = false;
		}
		ring0_.Close();
		ring1_.Close();
		// JS lifecycle: vsockClose closes the duckdb-worker endpoint and posts n6k-vsock-detach so the
		// driver retires the pump ws-worker and frees the heap ring. No on_close_ here —
		// ShutdownFromOwner already called FailConnectionAndAllInFlightRequests, and the shared I/O
		// thread fires on_close_ for a genuine drop. Matches the native reactor's Stop. Run on the
		// main runtime thread: under threads>1 DETACH can reach Stop on a compute pthread, whose JS
		// scope has no globalThis.n6k (see n6k_wasm_main_thread.hpp).
		std::string js = "n6k.vsockClose('" + EscapeSingleQuotedJsLiteral(catalog_) + "')";
		RunScriptOnMain(js);
	}

	// Reconnect discard: retire this generation but leave the channel open for reuse (must NOT close
	// the ring or vsockClose — Reconnect re-attaches the same ring_ptr). Unregister quiesces the shared
	// I/O thread off ring1 before nulling callbacks, so it can never call a cleared callback and the
	// new generation can re-register the SAME ring with no overlapping consumer.
	void RetireKeepingChannelOpen() override {
		if (torn_down_.exchange(true)) {
			return;
		}
		stopped_.store(true);
		if (registered_) {
			N6kIoThread::Instance().UnregisterAndAwaitQuiesce(handle_);
			registered_ = false;
		}
		on_frame_ = nullptr;
		on_close_ = nullptr;
	}

private:
	void NotifyProgress() {
		std::lock_guard<std::mutex> lk(progress_mu_);
		progress_cv_.notify_all();
	}

	// Fire on_close_ at most once (drop or host reset), mark stopped, and wake a parked WaitUntil.
	void FireCloseOnce(const char *reason) {
		stopped_.store(true);
		if (!close_fired_.exchange(true) && on_close_) {
			on_close_(reason);
		}
		NotifyProgress();
	}

	std::string catalog_;
	FrameFn on_frame_;
	CloseFn on_close_;
	std::atomic<bool> stopped_ {false};
	std::atomic<bool> torn_down_ {false}; // once-only gate for the Stop/RetireKeepingChannelOpen teardown body
	VsockRing ring0_;                // duckdb->ws (SEND); the shared I/O thread is its sole producer while registered
	VsockRing ring1_;                // ws->duckdb (RECV); the shared I/O thread is its sole consumer while registered
	N6kIoThread::Handle handle_ = 0; // registration with the shared I/O thread (valid while registered_)
	bool registered_ = false;
	std::mutex progress_mu_;
	std::condition_variable progress_cv_;   // a parked WaitUntil is notified after each delivered frame
	std::atomic<bool> close_fired_ {false}; // on_close_ fires at most once per generation
};

} // namespace

std::unique_ptr<Reactor> CreateReactor(const WsClientOptions &opts) {
	// Keyed by the LOCAL alias (channel_key).
	return std::unique_ptr<Reactor>(new WasmReactor(opts.channel_key, opts.ring_ptr));
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
