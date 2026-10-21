#pragma once

#include "duckdb/common/exception.hpp"
#include "yyjson.hpp"
#include "n6k_yyjson_util.hpp"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace duckdb {
namespace n6k_body {

using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;
using duckdb_yyjson::yyjson_mut_obj_add_val;
using duckdb_yyjson::yyjson_mut_val;
using duckdb_yyjson::yyjson_mut_write;
using duckdb_yyjson::yyjson_read;
using duckdb_yyjson::yyjson_val_mut_copy;

// Attaches pre-serialized JSON `src` under `key`, or `fallback` when `src` is empty (the key is absent).
//
// Malformed `src` throws rather than being dropped: every `src` here is produced by our own serializer, so a
// parse failure means a bug upstream, not bad user input.
inline void AddParsedJsonOrThrowFreeingDoc(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *key,
                                           const std::string &src, const char *fallback) {
	const char *data = src.empty() ? fallback : src.c_str();
	size_t len = src.empty() ? std::strlen(fallback) : src.size();
	auto *idoc = yyjson_read(data, len, 0);
	if (!idoc) {
		yyjson_mut_doc_free(doc);
		throw InvalidInputException("n6k: request body field '%s' is not valid JSON: %s", key, src);
	}
	yyjson_mut_obj_add_val(doc, root, key, yyjson_val_mut_copy(doc, yyjson_doc_get_root(idoc)));
	yyjson_doc_free(idoc);
}

inline std::string SchemaTable(const std::string &schema, const std::string &table, int64_t txn = -1) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "table", table.c_str(), table.size());
	if (txn >= 0) {
		duckdb_yyjson::yyjson_mut_obj_add_sint(doc, root, "txn", txn);
	}
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string TablesList(int64_t txn) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	if (txn >= 0) {
		duckdb_yyjson::yyjson_mut_obj_add_sint(doc, root, "txn", txn);
	}
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string Sql(const std::string &sql) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "sql", sql.c_str(), sql.size());
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string Rpc(const std::string &function, const std::string &args_json) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "function", function.c_str(), function.size());
	AddParsedJsonOrThrowFreeingDoc(doc, root, "args", args_json, "[]");
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string ReadPlan(const std::string &plan_json, int64_t txn) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "plan", plan_json.c_str(), plan_json.size());
	if (txn >= 0) {
		duckdb_yyjson::yyjson_mut_obj_add_sint(doc, root, "txn", txn);
	}
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string WritePlan(const std::string &verb, const std::string &schema, const std::string &table,
                             const std::vector<std::string> &key_columns, const std::vector<std::string> &set_columns,
                             const std::string &plan_json, int64_t txn) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "verb", verb.c_str(), verb.size());
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "table", table.c_str(), table.size());
	auto *keys = duckdb_yyjson::yyjson_mut_arr(doc);
	for (auto &k : key_columns) {
		duckdb_yyjson::yyjson_mut_arr_add_strncpy(doc, keys, k.c_str(), k.size());
	}
	yyjson_mut_obj_add_val(doc, root, "key_columns", keys);
	auto *sets = duckdb_yyjson::yyjson_mut_arr(doc);
	for (auto &s : set_columns) {
		duckdb_yyjson::yyjson_mut_arr_add_strncpy(doc, sets, s.c_str(), s.size());
	}
	yyjson_mut_obj_add_val(doc, root, "set_columns", sets);
	yyjson_mut_obj_add_strncpy(doc, root, "plan", plan_json.c_str(), plan_json.size());
	if (txn >= 0) {
		duckdb_yyjson::yyjson_mut_obj_add_sint(doc, root, "txn", txn);
	}
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string Txn(int64_t txn) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	duckdb_yyjson::yyjson_mut_obj_add_sint(doc, root, "txn", txn);
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string CreateTable(const std::string &schema, const std::string &name, const std::string &columns_json) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "name", name.c_str(), name.size());
	AddParsedJsonOrThrowFreeingDoc(doc, root, "columns", columns_json, "[]");
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string CreateView(const std::string &schema, const std::string &name, const std::string &on_conflict,
                              const std::vector<std::string> &names, const std::string &plan_json, int64_t txn) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "name", name.c_str(), name.size());
	yyjson_mut_obj_add_strncpy(doc, root, "on_conflict", on_conflict.c_str(), on_conflict.size());
	auto *columns = duckdb_yyjson::yyjson_mut_arr(doc);
	for (auto &column : names) {
		duckdb_yyjson::yyjson_mut_arr_add_strncpy(doc, columns, column.c_str(), column.size());
	}
	yyjson_mut_obj_add_val(doc, root, "names", columns);
	yyjson_mut_obj_add_strncpy(doc, root, "plan", plan_json.c_str(), plan_json.size());
	if (txn >= 0) {
		duckdb_yyjson::yyjson_mut_obj_add_sint(doc, root, "txn", txn);
	}
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string DropView(const std::string &schema, const std::string &name, bool if_exists, int64_t txn) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "name", name.c_str(), name.size());
	duckdb_yyjson::yyjson_mut_obj_add_bool(doc, root, "if_exists", if_exists);
	if (txn >= 0) {
		duckdb_yyjson::yyjson_mut_obj_add_sint(doc, root, "txn", txn);
	}
	return n6k::SerializeJsonDocAndFree(doc);
}

inline std::string AlterTable(const std::string &schema, const std::string &table, const std::string &kind,
                              const std::string &details_json) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "schema", schema.c_str(), schema.size());
	yyjson_mut_obj_add_strncpy(doc, root, "table", table.c_str(), table.size());
	yyjson_mut_obj_add_strncpy(doc, root, "kind", kind.c_str(), kind.size());
	AddParsedJsonOrThrowFreeingDoc(doc, root, "details", details_json, "{}");
	return n6k::SerializeJsonDocAndFree(doc);
}

} // namespace n6k_body
} // namespace duckdb
