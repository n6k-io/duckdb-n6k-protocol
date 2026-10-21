#pragma once

#include <cstddef>
#include <cstdint>

namespace duckdb {

class ExtensionLoader;

namespace n6k {

// Private table function that scans an in-memory Arrow IPC buffer (OP_INSERT and OP_RPC_TABLE).
constexpr const char *N6K_INSERT_SCAN_FN = "n6k_serve_arrow_scan";

// Borrowed view of Arrow IPC bytes; the pointer must outlive the INSERT.
struct InsertArrowBytes {
	const uint8_t *data;
	size_t len;
};

void RegisterInsertScanFunction(ExtensionLoader &loader);

// Decodes its own copy.
int64_t CountArrowIpcRows(const uint8_t *data, size_t len);

} // namespace n6k
} // namespace duckdb
