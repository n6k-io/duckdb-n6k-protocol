#include "n6k_version_function.hpp"
#include "n6k_protocol_generated.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

namespace duckdb {

struct N6kVersionBindData : public TableFunctionData {
	vector<pair<string, string>> rows;
};

struct N6kVersionState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

static unique_ptr<FunctionData> N6kVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("component");
	names.push_back("version");
	return_types.push_back(LogicalType::VARCHAR);
	return_types.push_back(LogicalType::VARCHAR);

	auto result = make_uniq<N6kVersionBindData>();

	string ext_version = "";
#ifdef EXT_VERSION_N6K_CLIENT
	ext_version = EXT_VERSION_N6K_CLIENT;
#endif
	result->rows.emplace_back("n6k", ext_version);

	result->rows.emplace_back("duckdb", DuckDB::LibraryVersion());

	result->rows.emplace_back("n6k_protocol", std::to_string(n6k::N6K_PROTOCOL_VERSION));

#ifdef WASM_LOADABLE_EXTENSIONS
	const char *npm_ver =
	    emscripten_run_script_string("typeof n6k === 'object' && n6k !== null ? String(n6k.npmVersion || '') : ''");
	result->rows.emplace_back("n6k_npm", npm_ver && npm_ver[0] != '\0' ? npm_ver : "n/a");
#else
	result->rows.emplace_back("n6k_npm", "n/a");
#endif

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> N6kVersionInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_uniq<N6kVersionState>();
}

static void N6kVersionScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kVersionBindData>();
	auto &state = data_p.global_state->Cast<N6kVersionState>();
	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[state.offset];
		output.SetValue(0, count, Value(row.first));
		output.SetValue(1, count, Value(row.second));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void RegisterN6kVersion(ExtensionLoader &loader) {
	TableFunction func("n6k_version", {}, N6kVersionScan, N6kVersionBind, N6kVersionInitGlobal);
	loader.RegisterFunction(func);
}

} // namespace duckdb
