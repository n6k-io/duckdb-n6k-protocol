#pragma once
#include "duckdb.hpp"

namespace duckdb {
// n6k_disconnect, n6k_reconnect, n6k_send_text, n6k_connections.
void RegisterN6kConnectionFunctions(ExtensionLoader &loader);
} // namespace duckdb
