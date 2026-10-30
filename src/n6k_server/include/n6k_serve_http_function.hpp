#pragma once

namespace duckdb {

class ExtensionLoader;

// Registers n6k_serve_http, which listens for WebSocket clients and serves each one.
void RegisterN6kServeHttpFunction(ExtensionLoader &loader);

// Registers n6k_serve_ws, which serves one WebSocket on a session socket the host accepted.
void RegisterN6kServeWsFunction(ExtensionLoader &loader);

} // namespace duckdb
