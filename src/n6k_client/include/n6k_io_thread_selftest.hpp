#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_selftest_io_thread_doorbell()`, a network-free diagnostic table function for the
// shared I/O thread: on the coi/wasm_threads bundle it stands up ONE consumer pthread that
// parks on a single standalone doorbell int32 in shared heap and drains TWO independent vsock rings
// when woken, while two producer pthreads write frames into their own ring and ring the doorbell
// after each write. It proves the doorbell futex wake + multi-ring drain loop (the core of the
// single-shared-I/O-thread reactor). Emits exactly one diagnostic row.
void RegisterN6kSelftestIoThreadDoorbell(ExtensionLoader &loader);
} // namespace duckdb
