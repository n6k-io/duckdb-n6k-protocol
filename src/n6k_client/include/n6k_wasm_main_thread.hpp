#pragma once

// Run n6k's JS glue (globalThis.n6k.*) on the main runtime thread, callable from any thread.
//
// The glue is registered only on the main duckdb worker's JS global scope. Each emscripten pthread
// has its OWN JS global scope with no `n6k`, so a direct emscripten_run_script("n6k.*") on a compute
// pthread throws "n6k is not defined". Under threads>1 duckdb dispatches n6k control-plane work
// (a dial -> VsockTransport::Start's vsockDial/vsockAdopt, a close -> its Stop's vsockClose) onto
// arbitrary compute pthreads, so those calls must run on the main runtime thread where the glue
// lives. emscripten_sync_run_in_main_runtime_thread runs the function inline when already on the
// main thread (the threads=1 case — no round-trip) and proxies it there otherwise.

#ifdef WASM_LOADABLE_EXTENSIONS

#include <cstring>
#include <emscripten.h>
#include <emscripten/threading.h>
#include <string>
#include <utility>

namespace duckdb {
namespace n6k {

namespace detail {

struct N6kScriptStringCall {
	const char *js;
	std::string out;
};

inline void N6kRunScriptStringOnMain(void *arg) {
	auto *call = static_cast<N6kScriptStringCall *>(arg);
	const char *r = emscripten_run_script_string(call->js);
	if (r) {
		call->out = r;
	}
}

inline void N6kRunScriptVoidOnMain(void *arg) {
	emscripten_run_script(static_cast<const char *>(arg));
}

inline void N6kRunScriptOwnedOnMain(void *arg) {
	emscripten_run_script(static_cast<const char *>(arg));
	std::free(arg);
}

} // namespace detail

inline std::string RunScriptStringOnMain(const std::string &js) {
	detail::N6kScriptStringCall call {js.c_str(), std::string()};
	emscripten_sync_run_in_main_runtime_thread(EM_FUNC_SIG_VI, &detail::N6kRunScriptStringOnMain, &call);
	return std::move(call.out);
}

inline void RunScriptOnMain(const std::string &js) {
	emscripten_sync_run_in_main_runtime_thread(EM_FUNC_SIG_VI, &detail::N6kRunScriptVoidOnMain,
	                                           const_cast<char *>(js.c_str()));
}

// Queues the script and returns at once: safe from the I/O thread, which the main thread may be waiting on.
inline void RunScriptOnMainAsync(const std::string &js) {
	auto *copy = static_cast<char *>(std::malloc(js.size() + 1));
	if (!copy) {
		return;
	}
	std::memcpy(copy, js.c_str(), js.size() + 1);
	emscripten_async_run_in_main_runtime_thread(EM_FUNC_SIG_VI, &detail::N6kRunScriptOwnedOnMain, copy);
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
