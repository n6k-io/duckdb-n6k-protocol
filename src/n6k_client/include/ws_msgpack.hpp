#pragma once

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "yyjson.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdint>
#include <string>

namespace duckdb {
namespace n6k {

inline void PackJsonValue(msgpack::packer<msgpack::sbuffer> &pk, duckdb_yyjson::yyjson_val *v) {
	using namespace duckdb_yyjson; // NOLINT(google-build-using-namespace): scoped; yyjson foreach macros need the names
	if (!v || yyjson_is_null(v)) {
		pk.pack_nil();
		return;
	}
	if (yyjson_is_str(v)) {
		pk.pack(std::string(yyjson_get_str(v), yyjson_get_len(v)));
		return;
	}
	if (yyjson_is_bool(v)) {
		pk.pack(yyjson_get_bool(v));
		return;
	}
	if (yyjson_is_uint(v)) {
		pk.pack(static_cast<uint64_t>(yyjson_get_uint(v)));
		return;
	}
	if (yyjson_is_sint(v)) {
		pk.pack(static_cast<int64_t>(yyjson_get_sint(v)));
		return;
	}
	if (yyjson_is_real(v)) {
		pk.pack(yyjson_get_real(v));
		return;
	}
	if (yyjson_is_arr(v)) {
		pk.pack_array(static_cast<uint32_t>(yyjson_arr_size(v)));
		size_t idx, n;
		yyjson_val *item;
		yyjson_arr_foreach(v, idx, n, item) {
			PackJsonValue(pk, item);
		}
		return;
	}
	if (yyjson_is_obj(v)) {
		pk.pack_map(static_cast<uint32_t>(yyjson_obj_size(v)));
		size_t idx, n;
		yyjson_val *key, *val;
		yyjson_obj_foreach(v, idx, n, key, val) {
			pk.pack(std::string(yyjson_get_str(key), yyjson_get_len(key)));
			PackJsonValue(pk, val);
		}
		return;
	}
	pk.pack_nil();
}

// REQ frame: {t, id, op, ns, ...merged args} + optional raw Arrow body. args_json may be empty.
inline std::string PackReqFrame(int t_req, uint32_t req_id, int op, const char *args_json, size_t args_len,
                                const char *body, size_t body_len, uint64_t ns) {
	using namespace duckdb_yyjson; // NOLINT(google-build-using-namespace): scoped; yyjson foreach macros need the names
	yyjson_doc *doc = (args_len > 0) ? yyjson_read(args_json, args_len, 0) : nullptr;
	yyjson_val *root = doc ? yyjson_doc_get_root(doc) : nullptr;
	if (args_len > 0 && !(root && yyjson_is_obj(root))) {
		if (doc) {
			yyjson_doc_free(doc);
		}
		throw InternalException("n6k: request args are not a JSON object: %s", std::string(args_json, args_len));
	}
	const uint32_t nargs = root ? static_cast<uint32_t>(yyjson_obj_size(root)) : 0;

	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(4 + nargs);
	PackString(pk, "t");
	pk.pack(t_req);
	PackString(pk, "id");
	pk.pack(req_id);
	PackString(pk, "op");
	pk.pack(op);
	PackString(pk, "ns");
	pk.pack(ns);
	if (nargs > 0) {
		size_t idx, n;
		yyjson_val *key, *val;
		yyjson_obj_foreach(root, idx, n, key, val) {
			pk.pack(std::string(yyjson_get_str(key), yyjson_get_len(key)));
			PackJsonValue(pk, val);
		}
	}
	if (doc) {
		yyjson_doc_free(doc);
	}

	std::string frame(sbuf.data(), sbuf.size());
	if (body_len > 0 && body != nullptr) {
		frame.append(body, body_len);
	}
	return frame;
}

inline std::string PackIdOnlyFrame(int t, uint32_t req_id, uint64_t ns = 0) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(ns != 0 ? 3 : 2);
	PackString(pk, "t");
	pk.pack(t);
	PackString(pk, "id");
	pk.pack(req_id);
	if (ns != 0) {
		PackString(pk, "ns");
		pk.pack(ns);
	}
	return std::string(sbuf.data(), sbuf.size());
}

inline std::string PackCredit(int t, uint32_t req_id, uint32_t n, uint64_t ns) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(4);
	PackString(pk, "t");
	pk.pack(t);
	PackString(pk, "id");
	pk.pack(req_id);
	PackString(pk, "n");
	pk.pack(n);
	PackString(pk, "ns");
	pk.pack(ns);
	return std::string(sbuf.data(), sbuf.size());
}

inline std::string PackOpen(int t, uint64_t ns, const std::string &catalog) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(4);
	PackString(pk, "t");
	pk.pack(t);
	PackString(pk, "ns");
	pk.pack(ns);
	PackString(pk, "catalog");
	pk.pack(catalog);
	PackCapabilities(pk, OPEN_CAPS);
	return std::string(sbuf.data(), sbuf.size());
}

inline std::string PackPong(int t, bool has_id, uint32_t req_id) {
	if (has_id) {
		return PackIdOnlyFrame(t, req_id);
	}
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(1);
	PackString(pk, "t");
	pk.pack(t);
	return std::string(sbuf.data(), sbuf.size());
}

} // namespace n6k
} // namespace duckdb
