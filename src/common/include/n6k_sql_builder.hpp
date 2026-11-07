#pragma once

#include "n6k_sql_escape.hpp"
#include "yyjson.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace n6k {

// Structure -> SQL for the n6k request ops: everything these requests can influence passes through here to be
// whitelisted, quoted (identifiers) or escaped (literals) before reaching a statement.
//
// One implementation, deliberately: a second is a place for the two to disagree, and disagreement
// here is a security bug. The reactor (src/n6k_server/request_handlers.cpp) is the ONLY caller on
// the serving path.

// QuoteIdent lives in n6k_sql_escape.hpp, included above.

std::string QuoteQualifiedTable(const std::string &catalog, const std::string &schema, const std::string &table);

// One JSON scalar -> a SQL literal. Strings are single-quoted with '' escaping; numbers are
// re-serialized from the JSON token so a 64-bit integer keeps its exact form. Throws on a value
// type the wire has no encoding for.
std::string JsonValueToSqlLiteral(duckdb_yyjson::yyjson_val *v);

// OP_TABLE_SCHEMA: a zero-row select, executed only for its result types.
std::string BuildTableSchemaSql(const std::string &catalog, const std::string &schema, const std::string &table);

// OP_CREATE_SCHEMA. `on_conflict` is whitelisted: error, ignore.
std::string BuildCreateSchemaSql(const std::string &catalog, const std::string &schema, const std::string &on_conflict);

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
