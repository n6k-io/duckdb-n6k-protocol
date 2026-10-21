#pragma once

namespace duckdb {

class ExtensionLoader;

// Registers n6k_login(...) — interactive OAuth device flow (RFC 8628). Native-only; bind throws on WASM.
void RegisterN6kLogin(ExtensionLoader &loader);

} // namespace duckdb
