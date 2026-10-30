#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_testing_wire_codec_bench(sql, reps := ...)` (default in BenchBind): runs `sql` once, then
// times encoding its chunks and decoding them back to DuckDB vectors, as Arrow IPC (the server's encoder,
// the client's decoder + ArrowToDuckDB) and as DuckDB-serialized DataChunks. One row per codec/stage:
// median seconds over `reps`, and total encoded payload bytes. Network-free.
void RegisterN6kTestingWireCodecBench(ExtensionLoader &loader);
} // namespace duckdb
