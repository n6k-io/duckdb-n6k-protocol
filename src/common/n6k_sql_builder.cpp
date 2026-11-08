#include "n6k_sql_builder.hpp"

#include "ws_json.hpp"
#include "n6k_sql_escape.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdlib>

namespace duckdb {
namespace n6k {

std::string QuoteQualifiedTable(const std::string &catalog, const std::string &schema, const std::string &table) {
	return QuoteIdent(catalog) + "." + QuoteIdent(schema) + "." + QuoteIdent(table);
}

std::string JsonValueToSqlLiteral(duckdb_yyjson::yyjson_val *v) {
	using namespace duckdb_yyjson; // NOLINT
	if (!v || yyjson_is_null(v)) {
		return "NULL";
	}
	if (yyjson_is_bool(v)) {
		return yyjson_get_bool(v) ? "TRUE" : "FALSE";
	}
	if (yyjson_is_str(v)) {
		return QuoteSqlLiteral(yyjson_get_str(v));
	}
	if (yyjson_is_num(v)) {
		size_t len = 0;
		char *txt = yyjson_val_write(v, 0, &len);
		if (!txt) {
			throw InternalException("n6k: failed to serialize a numeric literal");
		}
		std::string out(txt, len);
		free(txt);
		return out;
	}
	throw InvalidInputException("n6k: unsupported scalar value type");
}

std::string BuildTableSchemaSql(const std::string &catalog, const std::string &schema, const std::string &table) {
	return "SELECT * FROM " + QuoteQualifiedTable(catalog, schema, table) + " LIMIT 0";
}

std::string BuildCreateSchemaSql(const std::string &catalog, const std::string &schema,
                                 const std::string &on_conflict) {
	const auto qualified = QuoteIdent(catalog) + "." + QuoteIdent(schema);
	if (on_conflict == "error") {
		return "CREATE SCHEMA " + qualified;
	}
	if (on_conflict == "ignore") {
		return "CREATE SCHEMA IF NOT EXISTS " + qualified;
	}
	throw InvalidInputException("n6k: create_schema \"on_conflict\" must be error or ignore");
}

std::string RenderRpcArgValue(duckdb_yyjson::yyjson_val *v) {
	using namespace duckdb_yyjson; // NOLINT
	if (v && yyjson_is_obj(v)) {
		std::string out = "{";
		size_t idx, max;
		yyjson_val *key;
		yyjson_val *val;
		yyjson_obj_foreach(v, idx, max, key, val) {
			if (idx > 0) {
				out += ", ";
			}
			const char *kstr = yyjson_get_str(key);
			out += QuoteSqlLiteral(kstr ? std::string(kstr) : std::string()) + ": " + RenderRpcArgValue(val);
		}
		out += "}";
		return out;
	}
	if (v && yyjson_is_arr(v)) {
		std::string out = "[";
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(v, idx, max, item) {
			if (idx > 0) {
				out += ", ";
			}
			out += RenderRpcArgValue(item);
		}
		out += "]";
		return out;
	}
	return JsonValueToSqlLiteral(v);
}

std::string BuildRpcCallSql(const std::string &catalog, const std::string &schema, const std::string &function,
                            duckdb_yyjson::yyjson_val *args, const std::string &table_arg) {
	using namespace duckdb_yyjson; // NOLINT
	if (function.empty()) {
		throw InvalidInputException("n6k: rpc requires a function name");
	}

	std::string target = QuoteIdent(function);
	if (!catalog.empty()) {
		target = QuoteIdent(catalog) + "." + QuoteIdent(schema) + "." + target;
	}

	std::string rendered;
	if (!table_arg.empty()) {
		rendered = table_arg;
	}
	if (args && !yyjson_is_arr(args)) {
		throw InvalidInputException("n6k: rpc \"args\" must be an array");
	}
	if (args) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(args, idx, max, item) {
			if (!rendered.empty()) {
				rendered += ", ";
			}
			rendered += RenderRpcArgValue(item);
		}
	}
	return "SELECT * FROM " + target + "(" + rendered + ")";
}

} // namespace n6k
} // namespace duckdb
