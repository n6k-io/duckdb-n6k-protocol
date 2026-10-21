#include "n6k_split_statements.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

struct StatementRow {
	string statement;
	string type;
};

struct SplitStatementsBindData : public TableFunctionData {
	vector<StatementRow> rows;
};

struct SplitStatementsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

static unique_ptr<FunctionData> SplitStatementsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("ordinality");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("statement");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("statement_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto sql = input.inputs[0].GetValue<string>();

	Parser parser;
	parser.ParseQuery(sql);

	auto result = make_uniq<SplitStatementsBindData>();
	for (auto &stmt : parser.statements) {
		// stmt->query is this statement's verbatim text; re-slicing sql is wrong (ParseQuery zeroes stmt_location).
		string text = stmt->query;
		StringUtil::Trim(text);
		if (!text.empty() && text.back() == ';') {
			text.pop_back();
			StringUtil::RTrim(text);
		}
		result->rows.push_back({std::move(text), StatementTypeToString(stmt->type)});
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SplitStatementsInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<SplitStatementsState>();
}

static void SplitStatementsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<SplitStatementsBindData>();
	auto &state = data_p.global_state->Cast<SplitStatementsState>();
	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[state.offset];
		output.SetValue(0, count, Value::BIGINT(NumericCast<int64_t>(state.offset + 1)));
		output.SetValue(1, count, Value(row.statement));
		output.SetValue(2, count, Value(row.type));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void RegisterN6kSplitStatements(ExtensionLoader &loader) {
	TableFunction func("n6k_split_statements", {LogicalType::VARCHAR}, SplitStatementsScan, SplitStatementsBind,
	                   SplitStatementsInitGlobal);
	loader.RegisterFunction(func);
}

} // namespace duckdb
