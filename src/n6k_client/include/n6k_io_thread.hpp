#pragma once

// The single shared n6k I/O thread: ONE process-global consumer pthread that drains every
// attached catalog's RX ring (ring1) in duckdb's shared wasm heap. It parks on one standalone
// doorbell int32 (rung by every pump after it publishes an inbound frame) and, when woken, drains
// all registered rings and delivers each frame via that channel's callback. Collapsing to one
// thread keeps n6k's emscripten-pool cost at 1 regardless of catalog count, which is what makes
// threads>1 viable with multiple catalogs.
//
// Lifecycle: a lazily-started singleton (first Instance()), torn down at static destruction. A
// reactor Registers its channel in Start and Unregisters in Stop/RetireKeepingChannelOpen. Unregister
// BLOCKS on a quiesce handshake — the I/O thread acks once it has re-snapshotted without the channel
// and will never touch that ring again — the barrier that gates ring Close + the JS free chain.

#ifdef WASM_LOADABLE_EXTENSIONS

#include "n6k_vsock_ring.hpp"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace duckdb {
namespace n6k {

class N6kIoThread {
public:
	using Handle = uint64_t;
	using FrameFn = std::function<void(const std::string &, bool)>;
	using ProgressFn = std::function<void()>;
	using CloseFn = std::function<void(const char *)>;

	// One registered channel's plumbing. The reactor owns the callbacks' targets and keeps the
	// registration alive (via Unregister's quiesce) until the I/O thread is provably off the rings.
	struct Registration {
		VsockRing ring0;       // TX (duckdb->ws); the I/O thread is its sole producer (SPSC)
		VsockRing ring1;       // RX (ws->duckdb); the I/O thread is its sole consumer (SPSC)
		uint64_t pinned_epoch; // retire this registration if the ring's epoch moves past it
		FrameFn on_frame;
		ProgressFn on_progress; // wake the reactor's HELLO/ready gate after a delivery/close
		CloseFn on_close;       // fired once on -3 (closed), an epoch move (host reset) or a channel fault
	};

	static N6kIoThread &Instance();

	// Address of the single shared doorbell int32 in the wasm heap. Forwarded to every pump, which
	// bumps + notifies it after publishing an inbound frame so the I/O thread wakes to drain.
	int32_t *DoorbellAddress() {
		return &doorbell_;
	}

	// Add a channel; the I/O thread begins draining its ring1. Returns a handle for Unregister.
	Handle Register(Registration reg);

	// Remove a channel and BLOCK until the I/O thread guarantees it will never touch that channel's
	// rings again (the quiesce barrier). Idempotent for an already-removed handle.
	void UnregisterAndAwaitQuiesce(Handle h);

	// Queue one outbound frame for the channel's ring0; the I/O thread (ring0's sole producer)
	// TryWrites it, so this never touches a ring and never blocks. No-op if the handle is gone.
	void Enqueue(Handle h, const std::string &frame);

private:
	N6kIoThread();
	~N6kIoThread();
	N6kIoThread(const N6kIoThread &) = delete;
	N6kIoThread &operator=(const N6kIoThread &) = delete;

	struct Channel {
		Registration reg;
		bool retiring = false;    // Unregister requested; exclude from the next snapshot, then ack
		bool quiesced = false;    // I/O thread's ack that it re-snapshotted without this channel
		bool defunct = false;     // closed/reset; skip draining until the reactor Unregisters
		bool close_fired = false; // on_close fires at most once per registration

		// Outbound frames awaiting ring0. Producers (compute threads / the I/O thread's own PONGs)
		// push under out_mu; the I/O thread swaps this out and TryWrites, so ring0 keeps one producer.
		std::mutex out_mu;
		std::deque<std::string> outbound;
	};

	void Loop();
	void BumpDoorbell();
	void RetireChannelWithClose(Channel &ch, const char *reason);
	// TryWrite queued outbound to ring0; re-queues the tail (order-preserving) if the ring fills.
	// Returns true if it wrote at least one frame. Skips a defunct channel (drops its backlog).
	bool DrainOutbound(Channel &ch);

	// A plain int32 living in the singleton's static storage — an offset into the same shared linear
	// memory JS views, so JS Atomics and the C++ wasm futex interoperate on it. Only ever touched via
	// __atomic_* / __builtin_wasm_* (never a bare member read), so it behaves as an atomic word.
	alignas(4) int32_t doorbell_ = 0;

	std::mutex mu_; // guards channels_, next_handle_, stop_, and the retiring/quiesced flags
	std::condition_variable quiesce_cv_;
	std::unordered_map<Handle, std::shared_ptr<Channel>> channels_;
	Handle next_handle_ = 1;
	bool stop_ = false;
	std::thread thread_;
};

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
