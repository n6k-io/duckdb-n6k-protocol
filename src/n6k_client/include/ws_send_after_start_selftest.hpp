#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_selftest_send_after_start()`, a loopback-only diagnostic table function that
// gates the ConnectionHub's connect ordering: the HELLO it queues once the socket opens, and the first frame
// after it, MUST reach the wire. ixwebsocket's WebSocket::sendBinary returns false and silently discards when the
// socket has not yet reached ReadyState::Open, so a frame written before the 101 arrives is lost and the
// connect can only ever time out. Stands up an in-process ix::WebSocketServer on loopback and emits
// exactly one diagnostic row.
void RegisterN6kSelftestSendAfterStart(ExtensionLoader &loader);
} // namespace duckdb
