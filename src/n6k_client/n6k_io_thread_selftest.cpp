#include "n6k_io_thread_selftest.hpp"
#include "duckdb/function/table_function.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#ifndef WASM_LOADABLE_EXTENSIONS
#include <condition_variable>
#include <deque>
#include <mutex>
#endif
#ifdef WASM_LOADABLE_EXTENSIONS
#include "n6k_vsock_ring.hpp"
#include <cstdlib>
#include <cstring>
#endif

namespace duckdb {

namespace {

struct IoSelftestResult {
	std::string mode;
	bool ok = false;
	bool spawned = false;
	bool joined = false;
	bool doorbell_woke = false; // the consumer genuinely parked on the doorbell and a bump woke it
	int32_t frames_expected = 0;
	int32_t frames_drained = 0;
	int64_t elapsed_ms = 0;
};

// Two producers × this many frames each; small fixed payload well under the ring capacity.
constexpr int32_t kProducers = 2;
constexpr int32_t kFramesPerProducer = 256;
constexpr int32_t kPayloadBytes = 48;

#ifdef WASM_LOADABLE_EXTENSIONS

// ---- doorbell = one shared-heap int32 driven by the wasm futex (interoperates with JS Atomics) ----
int32_t DoorbellLoad(int32_t *db) {
	return __atomic_load_n(db, __ATOMIC_ACQUIRE);
}
void DoorbellBump(int32_t *db) {
	__atomic_add_fetch(db, 1, __ATOMIC_RELEASE);
	__builtin_wasm_memory_atomic_notify(db, static_cast<int32_t>(-1));
}
int DoorbellWait(int32_t *db, int32_t expected, int64_t timeout_ns) {
	return __builtin_wasm_memory_atomic_wait32(db, expected, timeout_ns);
}

// Sole consumer: sample the doorbell, drain BOTH rings fully, and only park (on the sampled value, so
// a bump landing in the sampling gap makes wait32 return not-equal immediately) when a whole pass
// drained nothing. `parked` is raised just before wait32 so the driver can prove a bump wakes a
// genuinely-parked consumer (woke). Exits once a fully-empty pass coincides with stop — by then all
// producers have joined, so both rings are provably drained.
void IoConsumer(int32_t *doorbell, n6k::VsockRing r0, n6k::VsockRing r1, std::atomic<bool> &stop,
                std::atomic<int32_t> &frames, std::atomic<bool> &woke, std::atomic<int32_t> &parked) {
	std::vector<uint8_t> buf(4096);
	n6k::VsockRing rings[2] = {r0, r1};
	for (;;) {
		int32_t seen = DoorbellLoad(doorbell);
		int drained_this_pass = 0;
		for (auto &r : rings) {
			for (;;) {
				int64_t n = r.TryRead(buf.data(), static_cast<uint32_t>(buf.size()));
				if (n == -1 || n == -3) {
					break; // empty (rings are never closed here)
				}
				if (n < -1) {
					buf.resize(static_cast<size_t>(-n)); // frame > buffer; grow and retry
					continue;
				}
				++drained_this_pass;
				frames.fetch_add(1, std::memory_order_relaxed);
			}
		}
		if (drained_this_pass == 0) {
			if (stop.load(std::memory_order_acquire)) {
				return;
			}
			parked.store(1, std::memory_order_release);
			int wr = DoorbellWait(doorbell, seen, static_cast<int64_t>(250) * 1000000);
			parked.store(0, std::memory_order_release);
			if (wr == 0) {
				woke.store(true, std::memory_order_release);
			}
		}
	}
}

void Producer(int32_t *doorbell, n6k::VsockRing ring, int32_t count) {
	std::vector<uint8_t> frame(kPayloadBytes, 0xAB);
	for (int32_t i = 0; i < count; ++i) {
		while (!ring.TryWrite(frame.data(), static_cast<uint32_t>(frame.size()))) {
			std::this_thread::yield(); // ring full: the consumer will free space shortly
		}
		DoorbellBump(doorbell);
	}
}

IoSelftestResult RunIoSelftest() {
	IoSelftestResult r;
	r.mode = "wasm_threads";
	r.frames_expected = kProducers * kFramesPerProducer;
	const auto t0 = std::chrono::steady_clock::now();

	auto *doorbell = static_cast<int32_t *>(std::calloc(1, sizeof(int32_t)));
	auto *base0 = static_cast<uint8_t *>(std::calloc(1, n6k::N6K_RING_BYTES));
	auto *base1 = static_cast<uint8_t *>(std::calloc(1, n6k::N6K_RING_BYTES));
	if (!doorbell || !base0 || !base1) {
		std::free(doorbell);
		std::free(base0);
		std::free(base1);
		r.elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
		return r;
	}
	n6k::VsockRing r0 = n6k::MakeVsockRing(base0);
	n6k::VsockRing r1 = n6k::MakeVsockRing(base1);

	std::atomic<int32_t> frames {0};
	std::atomic<bool> stop {false};
	std::atomic<bool> woke {false};
	std::atomic<int32_t> parked {0};

	try {
		std::thread io([&] { IoConsumer(doorbell, r0, r1, stop, frames, woke, parked); });
		r.spawned = true;

		// Deterministic doorbell-wake proof: with the rings still empty the consumer parks; wait until
		// it is genuinely inside wait32, then bump once so its wait returns "woken" (sets woke). Only
		// after that do the producers start — so woke never depends on the drain/stop race.
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (parked.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		DoorbellBump(doorbell);

		std::thread p0([&] { Producer(doorbell, r0, kFramesPerProducer); });
		std::thread p1([&] { Producer(doorbell, r1, kFramesPerProducer); });

		p0.join();
		p1.join();
		// All frames are now enqueued; signal stop and kick the doorbell so the consumer drains the
		// tail and then observes stop on its next fully-empty pass.
		stop.store(true, std::memory_order_release);
		DoorbellBump(doorbell);
		io.join();
		r.joined = true;
	} catch (...) {
	}

	r.frames_drained = frames.load(std::memory_order_acquire);
	r.doorbell_woke = woke.load(std::memory_order_acquire);
	r.ok = r.spawned && r.joined && r.frames_drained == r.frames_expected;

	std::free(doorbell);
	std::free(base0);
	std::free(base1);
	r.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	return r;
}

#else

IoSelftestResult RunIoSelftest() {
	IoSelftestResult r;
	r.mode = "native";
	r.frames_expected = kProducers * kFramesPerProducer;
	const auto t0 = std::chrono::steady_clock::now();

	struct Ring {
		std::mutex m;
		std::deque<int> q;
	};
	Ring rings[2];

	std::mutex dm;
	std::condition_variable dcv;
	int32_t doorbell = 0;
	std::atomic<int32_t> frames {0};
	std::atomic<bool> stop {false};
	std::atomic<bool> woke {false};
	std::atomic<int32_t> parked {0};

	auto bump = [&] {
		{
			std::lock_guard<std::mutex> lk(dm);
			++doorbell;
		}
		dcv.notify_all();
	};

	try {
		std::thread io([&] {
			for (;;) {
				int32_t seen;
				{
					std::lock_guard<std::mutex> lk(dm);
					seen = doorbell;
				}
				int drained_this_pass = 0;
				for (auto &ring : rings) {
					std::lock_guard<std::mutex> lk(ring.m);
					while (!ring.q.empty()) {
						ring.q.pop_front();
						++drained_this_pass;
						frames.fetch_add(1, std::memory_order_relaxed);
					}
				}
				if (drained_this_pass == 0) {
					if (stop.load(std::memory_order_acquire)) {
						return;
					}
					std::unique_lock<std::mutex> dl(dm);
					parked.store(1, std::memory_order_release);
					bool w = dcv.wait_for(dl, std::chrono::milliseconds(250),
					                      [&] { return doorbell != seen || stop.load(std::memory_order_acquire); });
					parked.store(0, std::memory_order_release);
					if (w) {
						woke.store(true, std::memory_order_release);
					}
				}
			}
		});
		r.spawned = true;

		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (parked.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		bump();

		auto producer = [&](Ring &ring) {
			for (int32_t i = 0; i < kFramesPerProducer; ++i) {
				{
					std::lock_guard<std::mutex> lk(ring.m);
					ring.q.push_back(i);
				}
				bump();
			}
		};
		std::thread p0([&] { producer(rings[0]); });
		std::thread p1([&] { producer(rings[1]); });

		p0.join();
		p1.join();
		stop.store(true, std::memory_order_release);
		bump();
		io.join();
		r.joined = true;
	} catch (...) {
	}

	r.frames_drained = frames.load(std::memory_order_acquire);
	r.doorbell_woke = woke.load(std::memory_order_acquire);
	r.ok = r.spawned && r.joined && r.frames_drained == r.frames_expected;
	r.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	return r;
}

#endif

struct N6kIoThreadSelftestBind : public TableFunctionData {};

struct N6kIoThreadSelftestState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"mode", "ok", "spawned", "joined", "doorbell_woke", "frames_expected", "frames_drained", "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::BOOLEAN, LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER};
	return make_uniq<N6kIoThreadSelftestBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kIoThreadSelftestState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kIoThreadSelftestState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	const IoSelftestResult r = RunIoSelftest();
	output.SetValue(0, 0, Value(r.mode));
	output.SetValue(1, 0, Value::BOOLEAN(r.ok));
	output.SetValue(2, 0, Value::BOOLEAN(r.spawned));
	output.SetValue(3, 0, Value::BOOLEAN(r.joined));
	output.SetValue(4, 0, Value::BOOLEAN(r.doorbell_woke));
	output.SetValue(5, 0, Value::INTEGER(r.frames_expected));
	output.SetValue(6, 0, Value::INTEGER(r.frames_drained));
	output.SetValue(7, 0, Value::INTEGER(static_cast<int32_t>(r.elapsed_ms)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kSelftestIoThreadDoorbell(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_io_thread_doorbell", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
