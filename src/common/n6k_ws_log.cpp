#include "n6k_ws_log.hpp"

#include "n6k_msgpack.hpp"

#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace n6k {

constexpr LogLevel WsLogType::LEVEL;

namespace {

string WsLogMessage(const char *direction, const char *frame_type, const string &req_id, const string &ns,
                    idx_t bytes) {
	const string type_json = frame_type ? StringUtil::Format("\"%s\"", frame_type) : "null";
	return StringUtil::Format("{\"direction\":\"%s\",\"frame_type\":%s,\"req_id\":%s,\"ns\":%s,\"bytes\":%d}",
	                          direction, type_json, req_id, ns, bytes);
}

} // namespace

WsLogType::WsLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType WsLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"direction", LogicalType::VARCHAR}, {"frame_type", LogicalType::VARCHAR}, {"req_id", LogicalType::BIGINT},
	    {"ns", LogicalType::BIGINT},         {"bytes", LogicalType::BIGINT},
	};
	return LogicalType::STRUCT(child_list);
}

string WsLogType::ConstructLogMessage(const char *direction, const std::string &frame) {
	msgpack::object_handle oh;
	size_t off = 0;
	try {
		oh = msgpack::unpack(frame.data(), frame.size(), off);
	} catch (const std::exception &) {
		return WsLogMessage(direction, nullptr, "null", "null", frame.size());
	}
	const msgpack::object &h = oh.get();
	if (h.type != msgpack::type::MAP) {
		return WsLogMessage(direction, nullptr, "null", "null", frame.size());
	}
	return WsLogMessage(direction, FrameTypeName(static_cast<int>(GetInt(h, "t", -1))),
	                    std::to_string(GetInt(h, "id", 0)), std::to_string(GetInt(h, "ns", 0)), frame.size());
}

string WsLogType::ConstructLogMessage(const char *direction, const char *kind, const std::string &frame) {
	return WsLogMessage(direction, kind, "null", "null", frame.size());
}

void RegisterWsLogType(DatabaseInstance &db) {
	auto &log_manager = db.GetLogManager();
	if (!log_manager.LookupLogType(WsLogType::NAME)) {
		log_manager.RegisterLogType(make_uniq<WsLogType>());
	}
}

} // namespace n6k
} // namespace duckdb
