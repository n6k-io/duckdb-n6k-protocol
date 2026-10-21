#pragma once

#include "yyjson.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
namespace n6k {

class JsonDoc {
public:
	JsonDoc(const char *data, size_t size) {
		doc_ = duckdb_yyjson::yyjson_read(data, size, 0);
	}
	explicit JsonDoc(const std::string &s) : JsonDoc(s.data(), s.size()) {
	}
	~JsonDoc() {
		if (doc_) {
			duckdb_yyjson::yyjson_doc_free(doc_);
		}
	}
	JsonDoc(const JsonDoc &) = delete;
	JsonDoc &operator=(const JsonDoc &) = delete;

	bool Parsed() const {
		return doc_ != nullptr;
	}
	duckdb_yyjson::yyjson_val *Root() const {
		return doc_ ? duckdb_yyjson::yyjson_doc_get_root(doc_) : nullptr;
	}

private:
	duckdb_yyjson::yyjson_doc *doc_ = nullptr;
};

inline std::string JsonGetStr(duckdb_yyjson::yyjson_val *obj, const char *key) {
	if (!obj) {
		return {};
	}
	auto *v = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!v || !duckdb_yyjson::yyjson_is_str(v)) {
		return {};
	}
	const char *s = duckdb_yyjson::yyjson_get_str(v);
	return s ? std::string(s) : std::string();
}

inline uint64_t JsonGetUint(duckdb_yyjson::yyjson_val *obj, const char *key, uint64_t fallback = 0) {
	if (!obj) {
		return fallback;
	}
	auto *v = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!v || !duckdb_yyjson::yyjson_is_int(v)) {
		return fallback;
	}
	return duckdb_yyjson::yyjson_get_uint(v);
}

inline bool JsonGetBool(duckdb_yyjson::yyjson_val *obj, const char *key, bool fallback = false) {
	if (!obj) {
		return fallback;
	}
	auto *v = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!v || !duckdb_yyjson::yyjson_is_bool(v)) {
		return fallback;
	}
	return duckdb_yyjson::yyjson_get_bool(v);
}

inline std::vector<std::string> JsonGetStrArray(duckdb_yyjson::yyjson_val *obj, const char *key) {
	std::vector<std::string> out;
	if (!obj) {
		return out;
	}
	auto *arr = duckdb_yyjson::yyjson_obj_get(obj, key);
	if (!arr) {
		return out;
	}
	if (!duckdb_yyjson::yyjson_is_arr(arr)) {
		throw InvalidInputException("n6k: \"%s\" must be an array of strings", key);
	}
	size_t idx, n;
	duckdb_yyjson::yyjson_val *item;
	yyjson_arr_foreach(arr, idx, n, item) {
		if (!duckdb_yyjson::yyjson_is_str(item)) {
			throw InvalidInputException("n6k: \"%s\" must be an array of strings", key);
		}
		out.emplace_back(duckdb_yyjson::yyjson_get_str(item), duckdb_yyjson::yyjson_get_len(item));
	}
	return out;
}

} // namespace n6k
} // namespace duckdb
