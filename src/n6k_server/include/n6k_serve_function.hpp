#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterN6kServeFunction(ExtensionLoader &loader);

// Registers n6k_serve_fd, which serves an already-connected stream fd, and the functions that control
// a running serve.
void RegisterN6kServeFdFunction(ExtensionLoader &loader);

void RegisterN6kHandshakeFunctions(ExtensionLoader &loader);

// Registers n6k_serve_push_invalidate(catalog, schema...), which tells attached clients to drop
// their cached view of those schemas.
void RegisterN6kServePushInvalidateFunction(ExtensionLoader &loader);

// Registers n6k_serve_stats(), reporting flow-control counters across live connections.
void RegisterN6kServeStatsFunction(ExtensionLoader &loader);

} // namespace duckdb
