#include "n6k_pump_thread.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS

#include "n6k_glue_generated.hpp"

#include "duckdb/common/exception.hpp"

#include <condition_variable>
#include <emscripten.h>
#include <emscripten/proxying.h>
#include <mutex>
#include <new>

namespace duckdb {
namespace n6k {

namespace {

struct StartSignal {
	std::mutex mu;
	std::condition_variable cv;
	bool ready = false;
};

struct ScriptCall {
	const char *js;
	std::string out;
};

void *PumpThreadMain(void *arg) {
	emscripten_run_script(N6K_PUMP_GLUE_JS);
	auto *signal = static_cast<StartSignal *>(arg);
	{
		// Notify under the lock: the waiter owns `signal` and destroys it as soon as it wakes.
		std::lock_guard<std::mutex> lk(signal->mu);
		signal->ready = true;
		signal->cv.notify_all();
	}
	// Not emscripten_unwind_to_js_event_loop: without a live runtime the thread exits and its worker returns to
	// the pool, dropping proxied calls.
	emscripten_exit_with_live_runtime();
	return nullptr;
}

void RunScriptString(void *arg) {
	auto *call = static_cast<ScriptCall *>(arg);
	const char *r = emscripten_run_script_string(call->js);
	if (r) {
		call->out = r;
	}
}

void RunScriptOwned(void *arg) {
	auto *js = static_cast<std::string *>(arg);
	emscripten_run_script(js->c_str());
	delete js;
}

} // namespace

N6kPumpThread &N6kPumpThread::Instance() {
	static auto *instance = new N6kPumpThread();
	return *instance;
}

N6kPumpThread::N6kPumpThread() {
	StartSignal signal;
	// Not std::thread (emscripten#17091).
	if (pthread_create(&tid_, nullptr, PumpThreadMain, &signal) != 0) {
		throw IOException("n6k: failed to start the pump thread");
	}
	pthread_detach(tid_);
	std::unique_lock<std::mutex> lk(signal.mu);
	signal.cv.wait(lk, [&] { return signal.ready; });
}

std::string N6kPumpThread::Call(const std::string &js) {
	ScriptCall call {js.c_str(), std::string()};
	if (!emscripten_proxy_sync(emscripten_proxy_get_system_queue(), tid_, RunScriptString, &call)) {
		throw IOException("n6k: pump thread is not accepting calls");
	}
	return std::move(call.out);
}

bool N6kPumpThread::Post(const std::string &js) noexcept {
	auto *copy = new (std::nothrow) std::string(js);
	if (!copy) {
		return false;
	}
	if (!emscripten_proxy_async(emscripten_proxy_get_system_queue(), tid_, RunScriptOwned, copy)) {
		delete copy;
		return false;
	}
	return true;
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
