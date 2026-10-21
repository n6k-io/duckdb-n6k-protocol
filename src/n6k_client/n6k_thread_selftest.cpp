#include "n6k_thread_selftest.hpp"
#include "duckdb/function/table_function.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#ifndef WASM_LOADABLE_EXTENSIONS
#include <condition_variable>
#include <mutex>
#endif
#ifdef WASM_LOADABLE_EXTENSIONS
#include <cstdlib>
#include <emscripten.h>
#include <emscripten/threading.h> // emscripten_sync_run_in_main_runtime_thread
#endif

namespace duckdb {

namespace {

struct SelftestResult {
	std::string mode;
	bool ok = false;
	bool spawned = false;
	bool joined = false;
	bool woke_from_park = false; // wasm: true only when the notify woke a genuinely-parked waiter
	int32_t wait_result = -99;   // futex return: 0 woken / 1 not-equal / 2 timed-out; -99 = n/a
	bool main_proxy_sees_n6k = false;
	int64_t elapsed_ms = 0;
};

#ifdef WASM_LOADABLE_EXTENSIONS

int ProbeN6kOnMain() {
	const char *r = emscripten_run_script_string("(typeof n6k === 'object') ? 1 : 0");
	return r ? std::atoi(r) : -1;
}

SelftestResult RunSelftest() {
	SelftestResult r;
	r.mode = "wasm_threads";
	const auto t0 = std::chrono::steady_clock::now();

	alignas(4) int32_t word = 0; // futex word — in shared linear memory under -sSHARED_MEMORY
	std::atomic<int32_t> parked {0};
	int32_t child_r = -99;
	std::atomic<int32_t> proxy_seen {-99};

	try {
		std::thread th([&word, &parked, &child_r, &proxy_seen]() {
			parked.store(1, std::memory_order_release);
			// No JS, no globalThis: the n6k I/O thread's constraint.
			child_r = __builtin_wasm_memory_atomic_wait32(&word, 0, static_cast<int64_t>(5000000000LL));
			proxy_seen.store(emscripten_sync_run_in_main_runtime_thread(EM_FUNC_SIG_I, &ProbeN6kOnMain),
			                 std::memory_order_release);
		});
		r.spawned = true;

		// Short sleep after the park flag so the child is actually inside wait32 before we notify —
		// makes the WAKE path (child_r==0) reliable.
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (parked.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));

		__atomic_store_n(&word, 1, __ATOMIC_RELEASE);
		__builtin_wasm_memory_atomic_notify(&word, static_cast<int32_t>(-1));

		th.join();
		r.joined = true;
		r.wait_result = child_r;
		r.woke_from_park = (child_r == 0);
		r.main_proxy_sees_n6k = (proxy_seen.load(std::memory_order_acquire) == 1);
	} catch (...) {
		// leave spawned/joined reflecting how far we got
	}

	r.ok = r.spawned && r.joined;
	r.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	return r;
}

#else

SelftestResult RunSelftest() {
	SelftestResult r;
	r.mode = "native";
	const auto t0 = std::chrono::steady_clock::now();

	std::mutex m;
	std::condition_variable cv;
	bool flag = false;
	std::atomic<int32_t> parked {0};

	try {
		std::thread th([&]() {
			std::unique_lock<std::mutex> lk(m);
			parked.store(1, std::memory_order_release);
			cv.wait(lk, [&] { return flag; });
		});
		r.spawned = true;

		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		while (parked.load(std::memory_order_acquire) == 0 && std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		{
			std::lock_guard<std::mutex> lk(m);
			flag = true;
		}
		cv.notify_all();

		th.join();
		r.joined = true;
		r.woke_from_park = true;
		r.wait_result = 0;
	} catch (...) {
	}

	r.ok = r.spawned && r.joined;
	r.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	return r;
}

#endif

struct N6kThreadSelftestBind : public TableFunctionData {};

struct N6kThreadSelftestState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"mode", "ok", "spawned", "joined", "woke_from_park", "wait_result", "main_proxy_sees_n6k", "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::BOOLEAN, LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::INTEGER};
	return make_uniq<N6kThreadSelftestBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kThreadSelftestState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kThreadSelftestState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	const SelftestResult r = RunSelftest();
	output.SetValue(0, 0, Value(r.mode));
	output.SetValue(1, 0, Value::BOOLEAN(r.ok));
	output.SetValue(2, 0, Value::BOOLEAN(r.spawned));
	output.SetValue(3, 0, Value::BOOLEAN(r.joined));
	output.SetValue(4, 0, Value::BOOLEAN(r.woke_from_park));
	output.SetValue(5, 0, Value::INTEGER(r.wait_result));
	output.SetValue(6, 0, Value::BOOLEAN(r.main_proxy_sees_n6k));
	output.SetValue(7, 0, Value::INTEGER(static_cast<int32_t>(r.elapsed_ms)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kSelftestThreadParkWake(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_thread_park_wake", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
