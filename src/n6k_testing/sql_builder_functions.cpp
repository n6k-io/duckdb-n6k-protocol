#include "sql_builder_functions.hpp"

#include "n6k_sql_builder.hpp"
#include "ws_json.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <utility>

namespace duckdb {

namespace {

// The n6k_testing_build_*_sql scalars expose src/common/include/n6k_sql_builder.hpp to any host that
// speaks SQL, so the trust boundary -- operator/function whitelists, identifier quoting, literal
// escaping -- can be exercised from SQL without reimplementing it. The serving path does not go
// through here: the C++ reactor (src/n6k_server/request_handlers.cpp) calls the builders directly.
// That is the whole reason these live in an unpublished extension: they are a keyhole onto shipped
// code, not shipped code themselves. The only caller is test/sql/n6k_sql_builder.test.
//
// Structured arguments (columns, details) arrive as JSON text rather than as LIST/STRUCT: it is the shape the request
// body already has on the wire, so the same bytes reach the same yyjson parser either way.

// Parse one JSON argument and hand its root to `fn`. An empty/NULL argument yields a null root,
// which every builder already treats as "absent".
template <class FN>
std::string WithJson(const std::string &json, FN fn) {
	if (json.empty()) {
		return fn(nullptr);
	}
	n6k::JsonDoc doc(json);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k: expected JSON, got '%s'", json);
	}
	return fn(doc.Root());
}

// Row-wise driver for a scalar whose arguments are all VARCHAR: flattens every input, then calls
// `build(row_values)` once per row. A NULL argument reaches `build` as an empty string, matching
// the "absent" convention the builders already use.
template <class BUILD>
void RunRowWise(DataChunk &args, Vector &result, BUILD build) {
	auto count = args.size();
	auto arity = args.ColumnCount();
	for (idx_t c = 0; c < arity; c++) {
		args.data[c].Flatten(count);
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);

	std::vector<std::string> row(arity);
	for (idx_t i = 0; i < count; i++) {
		for (idx_t c = 0; c < arity; c++) {
			auto &vec = args.data[c];
			if (FlatVector::IsNull(vec, i)) {
				row[c].clear();
			} else {
				row[c] = FlatVector::GetData<string_t>(vec)[i].GetString();
			}
		}
		result_data[i] = StringVector::AddString(result, build(row));
	}
}

void BuildCreateTableSqlFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RunRowWise(args, result, [](const std::vector<std::string> &r) {
		return WithJson(r[3], [&](duckdb_yyjson::yyjson_val *columns) {
			return n6k::BuildCreateTableSql(r[0], r[1], r[2], columns);
		});
	});
}

void BuildAlterSqlFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RunRowWise(args, result, [](const std::vector<std::string> &r) {
		return WithJson(r[4], [&](duckdb_yyjson::yyjson_val *details) {
			return n6k::BuildAlterSql(r[0], r[1], r[2], r[3], details);
		});
	});
}

void BuildTableSchemaSqlFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RunRowWise(args, result,
	           [](const std::vector<std::string> &r) { return n6k::BuildTableSchemaSql(r[0], r[1], r[2]); });
}

// The one op that composes its own statement rather than taking a whole one from a builder:
// OP_INSERT names its target inline (INSERT INTO <target> SELECT * FROM <registered view>), so it
// needs the quoting rule without the SELECT around it.
void QuoteQualifiedTableFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	RunRowWise(args, result,
	           [](const std::vector<std::string> &r) { return n6k::QuoteQualifiedTable(r[0], r[1], r[2]); });
}

ScalarFunction MakeVarcharFn(const char *name, idx_t arity, scalar_function_t fn) {
	duckdb::vector<LogicalType> arg_types;
	for (idx_t i = 0; i < arity; i++) {
		arg_types.push_back(LogicalType::VARCHAR);
	}
	return ScalarFunction(name, arg_types, LogicalType::VARCHAR, std::move(fn));
}

} // namespace

void RegisterN6kTestingSqlBuilders(ExtensionLoader &loader) {
	// (catalog, schema, name, columns_json)
	loader.RegisterFunction(MakeVarcharFn("n6k_testing_build_create_table_sql", 4, BuildCreateTableSqlFunction));
	// (catalog, schema, table, kind, details_json)
	loader.RegisterFunction(MakeVarcharFn("n6k_testing_build_alter_sql", 5, BuildAlterSqlFunction));
	// (catalog, schema, table)
	loader.RegisterFunction(MakeVarcharFn("n6k_testing_build_table_schema_sql", 3, BuildTableSchemaSqlFunction));
	// (catalog, schema, table)
	loader.RegisterFunction(MakeVarcharFn("n6k_testing_qualified_table", 3, QuoteQualifiedTableFunction));
}

} // namespace duckdb
