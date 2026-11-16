#pragma once

// The pthread that owns every dialed WebSocket (wasm). It evals the pump glue, then returns to its JS event loop
// with a live runtime, so its sockets keep flowing while duckdb's main worker is blocked in a query. C++ reaches
// its JS through the system proxying queue. Never torn down: it lives as long as the wasm runtime.

#ifdef WASM_LOADABLE_EXTENSIONS

#include <pthread.h>
#include <string>

namespace duckdb {
namespace n6k {

class N6kPumpThread {
public:
	// Starts the thread on first use and waits until its glue is installed.
	static N6kPumpThread &Instance();

	// Runs `js` on the pump thread and returns its string result.
	std::string Call(const std::string &js);
	// Queues `js` on the pump thread and returns at once; false if the thread no longer accepts calls.
	bool Post(const std::string &js) noexcept;

private:
	N6kPumpThread();
	pthread_t tid_ {};
};

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
