#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_selftest_send_after_start()`, a loopback-only diagnostic table function that
// gates the Reactor's connect ordering: a frame handed to Reactor::Send immediately after
// Reactor::Start (exactly what WsClient::StartReactorAndHandshake does with HELLO) MUST reach the wire. ixwebsocket's
// WebSocket::sendBinary returns false and silently discards when the socket has not yet reached
// ReadyState::Open, so a writer thread that fires before the 101 arrives drops the frame and the
// connect can only ever time out. Stands up an in-process ix::WebSocketServer on loopback and emits
// exactly one diagnostic row.
void RegisterN6kSelftestSendAfterStart(ExtensionLoader &loader);
} // namespace duckdb
