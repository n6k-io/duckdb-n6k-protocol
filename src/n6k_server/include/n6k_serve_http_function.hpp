#pragma once

namespace duckdb {

class ExtensionLoader;

// Registers the n6k_serve_http(host, port, catalog...) table function.
void RegisterN6kServeHttpFunction(ExtensionLoader &loader);

} // namespace duckdb
