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

std::string BuildCreateTableSql(const std::string &catalog, const std::string &schema, const std::string &name,
                                duckdb_yyjson::yyjson_val *columns) {
	using namespace duckdb_yyjson; // NOLINT
	if (!columns || !yyjson_is_arr(columns)) {
		throw InvalidInputException("n6k: create_table requires a \"columns\" array");
	}
	std::string col_defs;
	size_t idx, max;
	yyjson_val *col;
	yyjson_arr_foreach(columns, idx, max, col) {
		auto cname = JsonGetStr(col, "name");
		auto ctype = JsonGetStr(col, "type");
		if (cname.empty() || ctype.empty()) {
			throw InvalidInputException("n6k: each column requires \"name\" and \"type\"");
		}
		if (!col_defs.empty()) {
			col_defs += ", ";
		}
		col_defs += QuoteIdent(cname) + " " + ctype;
	}
	return "CREATE TABLE " + QuoteQualifiedTable(catalog, schema, name) + " (" + col_defs + ")";
}

std::string BuildAlterSql(const std::string &catalog, const std::string &schema, const std::string &table,
                          const std::string &kind, duckdb_yyjson::yyjson_val *details) {
	std::string qualified = QuoteQualifiedTable(catalog, schema, table);
	if (kind == "add_column") {
		auto cname = JsonGetStr(details, "name");
		auto ctype = JsonGetStr(details, "type");
		if (cname.empty() || ctype.empty()) {
			throw InvalidInputException("n6k: add_column requires \"name\" and \"type\"");
		}
		return "ALTER TABLE " + qualified + " ADD COLUMN " + QuoteIdent(cname) + " " + ctype;
	}
	if (kind == "drop_column") {
		auto cname = JsonGetStr(details, "name");
		if (cname.empty()) {
			throw InvalidInputException("n6k: drop_column requires \"name\"");
		}
		return "ALTER TABLE " + qualified + " DROP COLUMN " + QuoteIdent(cname);
	}
	if (kind == "rename_column") {
		auto old_name = JsonGetStr(details, "old_name");
		auto new_name = JsonGetStr(details, "new_name");
		if (old_name.empty() || new_name.empty()) {
			throw InvalidInputException("n6k: rename_column requires \"old_name\" and \"new_name\"");
		}
		return "ALTER TABLE " + qualified + " RENAME COLUMN " + QuoteIdent(old_name) + " TO " + QuoteIdent(new_name);
	}
	throw InvalidInputException("n6k: unsupported alter kind '%s' (add_column, drop_column, rename_column)", kind);
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
