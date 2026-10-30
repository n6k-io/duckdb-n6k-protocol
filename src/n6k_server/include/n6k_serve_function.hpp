#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterN6kServeFunction(ExtensionLoader &loader);

// Registers n6k_serve_fd and its companion session/control functions.
void RegisterN6kServeFdFunction(ExtensionLoader &loader);

// Registers n6k_serve_push_invalidate(catalog, schema...), which tells attached clients to drop
// their cached view of those schemas.
void RegisterN6kServePushInvalidateFunction(ExtensionLoader &loader);

// Registers n6k_serve_stats(), reporting flow-control counters across live connections.
void RegisterN6kServeStatsFunction(ExtensionLoader &loader);

} // namespace duckdb
