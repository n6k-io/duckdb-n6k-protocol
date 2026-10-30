#pragma once

#include "n6k_protocol_generated.hpp"

#include <string>

namespace duckdb {

class TableFunction;
struct TableFunctionBindInput;

namespace n6k {

// Host callbacks are names of scalar UDFs, called through HostCallback; each takes the connection's session id first.
// An empty name means the host supplied none.
struct ServeOptions {
	// (session UUID, token VARCHAR, capabilities VARCHAR[], request VARCHAR) -> VARCHAR, parsed by ParseAuthResult.
	// None: every token is acked and the catalogs are the serve's own.
	std::string authenticate;
	// (session UUID, text VARCHAR) -> VARCHAR reply, or NULL / empty for none. One thread per connection calls it,
	// in arrival order. Past MAX_QUEUED_TEXT waiting, a request is answered -32000 instead; text from a client not
	// granted an app capability is dropped. None: a JSON-RPC request is answered -32601.
	std::string on_text;
	// (session UUID, catalog VARCHAR) -> VARCHAR or NULL. Set, an OPEN of an unknown catalog parks until it returns,
	// unless MAX_CONCURRENT_REQS catalogs are already resolving (OPEN_ERR at once). OPENs still parked then get
	// OPEN_ERR: "not found" when it returned NULL or empty, else "failed to resolve" with the returned text, or
	// with its error when it threw.
	std::string resolve;
	// authenticate's `request`: RequestJson (ws_serve_connection.cpp) on a WebSocket path; on n6k_serve_fd, the
	// host's `request =>`.
	std::string request;
	bool has_request = false;
	std::string hello_pending_message = "authenticating";
	int hello_pending_first_ms = 1000;
	// < 0: ResolvePingIntervalMs; 0: off.
	int ping_interval_ms = -1;
	int open_pending_interval_ms = OPEN_PENDING_INTERVAL_MS;
	// Requests that run at once on this connection; the rest of the MAX_CONCURRENT_REQS window queues.
	int workers = 4;
	// 0: off. A stream parked for credit longer than this is ERRed and ended.
	int idle_stream_timeout_ms = 0;
	// 0: off. A pinned transaction no request has used for this long is rolled back.
	int idle_txn_timeout_ms = 0;
};

void AddServeOptionParameters(TableFunction &fn);
ServeOptions BindServeOptions(const TableFunctionBindInput &input, const char *fn_name);

} // namespace n6k
} // namespace duckdb
