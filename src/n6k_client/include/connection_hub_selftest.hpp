#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_selftest_hub_routing()`: one ConnectionHub over a RawSocketTransport on one socketpair() end,
// HubSessions and a connection-scoped request on it. Pins that each inbound frame reaches the session named by
// its `ns`, that the hub answers PING itself, that one reader owns the socket — two would split the byte stream
// between them — and that a session's Close() waits out its running callback and puts CANCEL {ns, id: 0} on the
// wire. Emits one row.
void RegisterN6kSelftestHubRouting(ExtensionLoader &loader);
} // namespace duckdb
