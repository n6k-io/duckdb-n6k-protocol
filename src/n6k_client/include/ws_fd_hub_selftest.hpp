#pragma once
#include "duckdb.hpp"

namespace duckdb {
// Registers `n6k_selftest_fd_hub_routing()`, a diagnostic table function that pins the demux guarantee two
// ATTACHes sharing one `wsFd` depend on: exactly ONE reader owns the socket and routes each inbound
// frame to the session named by its `ns`. Two independent readers would split the byte stream between
// them at arbitrary points — invisible with one session, silent corruption with two. Drives two real
// FdTransports over one socketpair() end and emits one row.
void RegisterN6kSelftestFdHubRouting(ExtensionLoader &loader);
} // namespace duckdb
