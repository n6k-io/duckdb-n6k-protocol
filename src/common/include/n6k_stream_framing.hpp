#pragma once

#include "n6k_protocol_generated.hpp"
#include "n6k_yyjson_util.hpp"
#include "ws_json.hpp"

#include <cstdint>
#include <string>

namespace duckdb {
namespace n6k {

// n6k stream framing; see protocol.py "Stream framing".
static constexpr size_t STREAM_HEADER_BYTES = 5;

inline void AppendStreamHeader(std::string &buf, StreamFrameType type, size_t payload_len) {
	auto len = static_cast<uint32_t>(payload_len);
	buf.push_back(static_cast<char>((len >> 24) & 0xff));
	buf.push_back(static_cast<char>((len >> 16) & 0xff));
	buf.push_back(static_cast<char>((len >> 8) & 0xff));
	buf.push_back(static_cast<char>(len & 0xff));
	buf.push_back(static_cast<char>(type));
}

inline uint32_t StreamPayloadLength(const char *header) {
	auto *p = reinterpret_cast<const unsigned char *>(header);
	return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
	       (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

inline StreamFrameType StreamFrameTypeOf(const char *header, const char *who) {
	auto raw = static_cast<unsigned char>(header[4]);
	if (raw != static_cast<unsigned char>(StreamFrameType::JSONRPC) &&
	    raw != static_cast<unsigned char>(StreamFrameType::N6K)) {
		throw IOException("%s: unknown stream frame type 0x%02x", who, static_cast<unsigned>(raw));
	}
	return static_cast<StreamFrameType>(raw);
}

// The error answering `message`, or empty when it is a notification or not a request at all.
inline std::string JsonRpcErrorReply(const std::string &message, int code, const char *error) {
	JsonDoc doc(message);
	auto *root = doc.Root();
	if (!root || !duckdb_yyjson::yyjson_is_obj(root) || !duckdb_yyjson::yyjson_obj_get(root, "method")) {
		return {};
	}
	auto *id = duckdb_yyjson::yyjson_obj_get(root, "id");
	if (!id) {
		return {};
	}
	auto *out = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
	auto *reply = duckdb_yyjson::yyjson_mut_obj(out);
	duckdb_yyjson::yyjson_mut_doc_set_root(out, reply);
	duckdb_yyjson::yyjson_mut_obj_add_str(out, reply, "jsonrpc", "2.0");
	duckdb_yyjson::yyjson_mut_obj_add_val(out, reply, "id", duckdb_yyjson::yyjson_val_mut_copy(out, id));
	auto *err = duckdb_yyjson::yyjson_mut_obj_add_obj(out, reply, "error");
	duckdb_yyjson::yyjson_mut_obj_add_int(out, err, "code", code);
	duckdb_yyjson::yyjson_mut_obj_add_strcpy(out, err, "message", error);
	return SerializeJsonDocAndFree(out);
}

inline std::string JsonRpcMethodNotFoundReply(const std::string &message) {
	return JsonRpcErrorReply(message, -32601, "Method not found");
}

} // namespace n6k
} // namespace duckdb
