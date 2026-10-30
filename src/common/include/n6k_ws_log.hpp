#pragma once

#include "duckdb/logging/log_type.hpp"

#include <string>

namespace duckdb {

class DatabaseInstance;

namespace n6k {

// One row per frame on the client WsClient and the server ServeReactor's downstream connection; upstream
// connections and the pre-reactor HELLO_ERR are not logged. `bytes` is the frame as the transport hands it, without
// transport framing; on the server, the same measure as SessionStats.
class WsLogType : public LogType {
public:
	static constexpr const char *NAME = "WS";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	WsLogType();

	static LogicalType GetLogType();

	// An n6k frame; only its msgpack header is decoded.
	static string ConstructLogMessage(const char *direction, const std::string &frame);
	// A frame that is not n6k msgpack; `frame_type` is `kind`.
	static string ConstructLogMessage(const char *direction, const char *kind, const std::string &frame);
};

// Idempotent: the client and server extensions may both load into one database.
void RegisterWsLogType(DatabaseInstance &db);

} // namespace n6k
} // namespace duckdb
