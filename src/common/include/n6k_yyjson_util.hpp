#pragma once

#include "yyjson.hpp"

#include <cstdlib>
#include <string>

namespace duckdb {
namespace n6k {

// Caller must set the document root first.
inline std::string SerializeJsonDocAndFree(duckdb_yyjson::yyjson_mut_doc *doc) {
	size_t len = 0;
	auto *json = duckdb_yyjson::yyjson_mut_write(doc, 0, &len);
	std::string result(json, len);
	free(json);
	duckdb_yyjson::yyjson_mut_doc_free(doc);
	return result;
}

} // namespace n6k
} // namespace duckdb
