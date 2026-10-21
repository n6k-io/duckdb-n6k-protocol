#pragma once

// Native (C++) accessor for the vsock SPSC byte-rings that live in duckdb's shared wasm heap.
// This is a byte-for-byte mirror of packages/npm/src/virtual-socket/ring.ts and channel.ts:
// both sides build views over the SAME bytes, so any layout/atomic-ordering divergence corrupts
// the rings. Frames are [u32 len (LE)][bytes]; capacity is a power of two so counters mask cleanly.
//
// SPSC precondition: each ring has exactly one producer and one consumer. The accessor is
// thread-agnostic (raw pointers + mask, no per-instance state beyond the views) and shared with the
// N6kIoThread pthread that drains it.
//
// This side is non-blocking: TryWrite/TryRead never park, and waiting is the caller's business --
// N6kIoThread parks on its own doorbell (n6k_io_thread.cpp). Wakeups still go through the wasm
// futex (`memory.atomic.notify`), which shares the engine's wait queue with JS `Atomics.wait`, so a
// C++ notifier wakes a JS waiter. std::atomic must NOT be used for that: libc++ keeps its own
// contention table with no JS interop. `wait32` also traps on the browser main thread, so any
// waiting added here must stay off it.

#ifdef WASM_LOADABLE_EXTENSIONS

#include "duckdb/common/exception.hpp"

#include <cstdint>
#include <cstring>

namespace duckdb {
namespace n6k {

// Control-word indices (Int32Array slots) — must match ring.ts RING_HEAD/TAIL/CLOSED/EPOCH.
constexpr uint32_t N6K_RING_HEAD = 0;
constexpr uint32_t N6K_RING_TAIL = 1;
constexpr uint32_t N6K_RING_CLOSED = 2;
constexpr uint32_t N6K_RING_EPOCH = 3;
constexpr uint32_t N6K_RING_CTRL_I32 = 4;                       // HEAD, TAIL, CLOSED, EPOCH
constexpr uint32_t N6K_RING_CTRL_BYTES = N6K_RING_CTRL_I32 * 4; // 16; keeps data 8-byte aligned
constexpr uint32_t N6K_RING_LEN_BYTES = 4;                      // u32 length prefix per frame
constexpr uint32_t N6K_RING_CAPACITY = 1u << 20;                // DEFAULT_CHANNEL_LAYOUT per-direction capacity
constexpr uint32_t N6K_RING_BYTES = N6K_RING_CTRL_BYTES + N6K_RING_CAPACITY; // one ring's total footprint

// A single SPSC byte-ring backed by shared heap: ctrl = the 4 control words, data = the
// capacity-byte circular area, mask = capacity-1 for wrap indexing.
struct VsockRing {
	int32_t *ctrl = nullptr;
	uint8_t *data = nullptr;
	uint32_t mask = 0;
	uint32_t capacity = 0;

	// ---- control-word atomics (mirror JS Atomics on the same Int32Array) ----
	uint32_t Load(uint32_t idx, int order) const {
		return static_cast<uint32_t>(__atomic_load_n(&ctrl[idx], order));
	}
	void Store(uint32_t idx, uint32_t val, int order) {
		__atomic_store_n(&ctrl[idx], static_cast<int32_t>(val), order);
	}
	void NotifyAll(uint32_t idx) {
		// count = -1 (0xFFFFFFFF) wakes all waiters, matching JS Atomics.notify with no count.
		__builtin_wasm_memory_atomic_notify(&ctrl[idx], static_cast<int32_t>(-1));
	}
	// Free-running byte counters; the unsigned difference is the used byte count even across u32 wrap.
	uint32_t Used() const {
		uint32_t tail = Load(N6K_RING_TAIL, __ATOMIC_ACQUIRE);
		uint32_t head = Load(N6K_RING_HEAD, __ATOMIC_ACQUIRE);
		return tail - head;
	}

	// ---- circular copy helpers (split at the wrap) ----
	void WriteAt(uint32_t pos, const uint8_t *src, uint32_t len) {
		uint32_t start = pos & mask;
		uint32_t first = len < (capacity - start) ? len : (capacity - start);
		std::memcpy(data + start, src, first);
		if (first < len) {
			std::memcpy(data, src + first, len - first);
		}
	}
	void ReadAt(uint32_t pos, uint32_t len, uint8_t *out) const {
		uint32_t start = pos & mask;
		uint32_t first = len < (capacity - start) ? len : (capacity - start);
		std::memcpy(out, data + start, first);
		if (first < len) {
			std::memcpy(out + first, data, len - first);
		}
	}

	static uint32_t DecodeLenLE(const uint8_t *p) {
		return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) | (static_cast<uint32_t>(p[2]) << 16) |
		       (static_cast<uint32_t>(p[3]) << 24);
	}
	static void EncodeLenLE(uint8_t *p, uint32_t v) {
		p[0] = static_cast<uint8_t>(v & 0xff);
		p[1] = static_cast<uint8_t>((v >> 8) & 0xff);
		p[2] = static_cast<uint8_t>((v >> 16) & 0xff);
		p[3] = static_cast<uint8_t>((v >> 24) & 0xff);
	}

	// Write one frame; false if no room now, throws if it can never fit (mirrors ringTryWrite).
	bool TryWrite(const uint8_t *src, uint32_t len) {
		uint32_t need = N6K_RING_LEN_BYTES + len;
		if (need > capacity) {
			throw IOException("n6k vsock: frame %u B + prefix exceeds ring capacity %u", len, capacity);
		}
		if (capacity - Used() < need) {
			return false;
		}
		uint32_t tail = Load(N6K_RING_TAIL, __ATOMIC_RELAXED);
		uint8_t len_buf[N6K_RING_LEN_BYTES];
		EncodeLenLE(len_buf, len);
		WriteAt(tail, len_buf, N6K_RING_LEN_BYTES);
		WriteAt(tail + N6K_RING_LEN_BYTES, src, len);
		Store(N6K_RING_TAIL, tail + need, __ATOMIC_RELEASE);
		NotifyAll(N6K_RING_TAIL);
		return true;
	}

	// Read one frame into out[0..cap); cap >= 3, so a too-large -n never aliases -1/-3. Returns:
	//   >= 0  frame length copied into out (consumed)
	//   -1    ring empty (still open)
	//   -3    ring empty AND closed — fail fast
	//   < -3  next frame (length -n) is larger than cap; nothing consumed (caller grows + retries)
	int64_t TryRead(uint8_t *out, uint32_t cap) {
		if (Used() < N6K_RING_LEN_BYTES) {
			return IsClosed() ? -3 : -1;
		}
		uint32_t head = Load(N6K_RING_HEAD, __ATOMIC_RELAXED);
		uint8_t len_buf[N6K_RING_LEN_BYTES];
		ReadAt(head, N6K_RING_LEN_BYTES, len_buf);
		uint32_t len = DecodeLenLE(len_buf);
		if (len > cap) {
			return -static_cast<int64_t>(len); // too big for caller's buffer; don't consume
		}
		if (Used() < N6K_RING_LEN_BYTES + len) {
			return IsClosed() ? -3 : -1; // partial (rare under SPSC atomic publish)
		}
		ReadAt(head + N6K_RING_LEN_BYTES, len, out);
		Store(N6K_RING_HEAD, head + N6K_RING_LEN_BYTES + len, __ATOMIC_RELEASE);
		NotifyAll(N6K_RING_HEAD);
		return static_cast<int64_t>(len);
	}

	bool IsClosed() const {
		return Load(N6K_RING_CLOSED, __ATOMIC_ACQUIRE) == 1;
	}

	// Mark closed and wake any blocked reader (TAIL) and writer (HEAD) so they can exit.
	void Close() {
		Store(N6K_RING_CLOSED, 1, __ATOMIC_RELEASE);
		NotifyAll(N6K_RING_TAIL);
		NotifyAll(N6K_RING_HEAD);
	}

	uint64_t Epoch() const {
		return Load(N6K_RING_EPOCH, __ATOMIC_ACQUIRE);
	}
};

// `ring_base` is the ring's control-word base.
inline VsockRing MakeVsockRing(uint8_t *ring_base) {
	VsockRing r;
	r.ctrl = reinterpret_cast<int32_t *>(ring_base);
	r.data = ring_base + N6K_RING_CTRL_BYTES;
	r.capacity = N6K_RING_CAPACITY;
	r.mask = N6K_RING_CAPACITY - 1;
	return r;
}

// Split a channel base (the C++ malloc pointer JS also views) into its two rings:
// ring0 = duckdb->ws (SEND), ring1 = ws->duckdb (RECV). Mirrors channel.ts `rings()`.
inline void MakeVsockChannelRings(uintptr_t channel_base, VsockRing &ring0, VsockRing &ring1) {
	auto *base = reinterpret_cast<uint8_t *>(channel_base);
	ring0 = MakeVsockRing(base);
	ring1 = MakeVsockRing(base + N6K_RING_BYTES);
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
