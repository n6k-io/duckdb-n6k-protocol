#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_testing_arrow_ipc_decode()`, a network-free diagnostic table function that gates
// the push-based N6kArrowFrameDecoder. It encodes the exact pyarrow WebSocket framing — an EMPTY
// RESP_SCHEMA frame plus a first RESP_CHUNK that carries the schema message PREPENDED to the first
// record batch (int32 `id` + utf8 `name`, two batches) — pushes those frames through the decoder, and
// returns the decoded rows. A green run proves the message-loop handles the empty-schema-frame and
// two-messages-in-one-frame cases that crashed the earlier assume-one-batch-per-chunk decoder.
void RegisterN6kTestingArrowIpcDecode(ExtensionLoader &loader);
} // namespace duckdb
