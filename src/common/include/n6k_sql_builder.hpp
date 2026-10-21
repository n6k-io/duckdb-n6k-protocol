#pragma once

#include "n6k_sql_escape.hpp"
#include "yyjson.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace n6k {

// Structure -> SQL for the n6k request ops. This is the trust boundary: a client sends column
// names, RPC arguments and alter kinds, and NEVER SQL text, so everything a request can influence
// passes through here to be whitelisted (alter kinds), quoted (identifiers) or escaped (literals)
// before reaching a statement.
//
// One implementation, deliberately: a second is a place for the two to disagree, and disagreement
// here is a security bug. The reactor (src/n6k_server/request_handlers.cpp) is the ONLY caller on
// the serving path; the n6k_testing_build_*_sql scalars expose these to SQL so tests can assert on
// the exact statement, and live in the unpublished n6k_testing extension so that keyhole is not
// part of a shipped binary.

// QuoteIdent lives in n6k_sql_escape.hpp, included above.

std::string QuoteQualifiedTable(const std::string &catalog, const std::string &schema, const std::string &table);

// One JSON scalar -> a SQL literal. Strings are single-quoted with '' escaping; numbers are
// re-serialized from the JSON token so a 64-bit integer keeps its exact form. Throws on a value
// type the wire has no encoding for.
std::string JsonValueToSqlLiteral(duckdb_yyjson::yyjson_val *v);

// OP_TABLE_SCHEMA: a zero-row select, executed only for its result types.
std::string BuildTableSchemaSql(const std::string &catalog, const std::string &schema, const std::string &table);

// OP_CREATE_TABLE. `columns` is an array of {"name": ..., "type": ...}; the type is a type
// EXPRESSION, not a value, and is interpolated verbatim -- only the name is quoted.
std::string BuildCreateTableSql(const std::string &catalog, const std::string &schema, const std::string &name,
                                duckdb_yyjson::yyjson_val *columns);

// OP_ALTER_TABLE. `kind` is whitelisted: add_column, drop_column, rename_column.
std::string BuildAlterSql(const std::string &catalog, const std::string &schema, const std::string &table,
                          const std::string &kind, duckdb_yyjson::yyjson_val *details);

// One RPC argument -> a SQL literal. A superset of JsonValueToSqlLiteral: the wire encodes a STRUCT
// as a JSON object and a LIST as a JSON array, which JsonValueToSqlLiteral rejects on purpose.
// Nested values recurse, so a struct of lists round-trips.
std::string RenderRpcArgValue(duckdb_yyjson::yyjson_val *v);

// OP_RPC_SCALAR / OP_RPC_TABLE: `SELECT * FROM fn(args...)`.
//
// Empty `catalog` calls the bare name, reaching a table function the host registered on the
// DatabaseInstance (those live in the system catalog); non-empty qualifies it as
// "catalog"."schema"."fn", reaching a table macro inside the served catalog. Choosing between the
// two needs a ClientContext and is the caller's job.
//
// `table_arg`, when non-empty, is emitted verbatim as the FIRST argument: the sub-select an in-out
// table function reads its input relation from, built by the server, not taken from the request.
std::string BuildRpcCallSql(const std::string &catalog, const std::string &schema, const std::string &function,
                            duckdb_yyjson::yyjson_val *args, const std::string &table_arg = "");

} // namespace n6k
} // namespace duckdb
