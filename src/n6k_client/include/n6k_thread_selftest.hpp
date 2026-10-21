#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_selftest_thread_park_wake()`, a network-free diagnostic table function. On the
// coi/wasm_threads bundle it spawns and joins ONE worker pthread from inside the loaded
// extension and proves the threading primitives the wasm data path relies on: that the
// toolchain links std::thread into the SIDE_MODULE, that pthread_create/join work from the
// duckdb worker without deadlock, that a `__builtin_wasm` futex wait/notify round-trips on a
// spawned pthread, and that a pthread can reach the main-worker-only `n6k` JS glue via
// emscripten main-thread proxying. Emits exactly one diagnostic row.
void RegisterN6kSelftestThreadParkWake(ExtensionLoader &loader);
} // namespace duckdb
