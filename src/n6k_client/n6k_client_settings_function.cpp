#include "n6k_client_settings_function.hpp"
#include "n6k_catalog.hpp"
#include "n6k_source.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

namespace {

constexpr const char *CROSSING_SETTING = "crossing";

struct N6kClientSettingsBind : public TableFunctionData {
	vector<string> rows;
};

struct N6kClientSettingsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

N6kSource &SourceOf(ClientContext &context, const string &db_name, const string &setting, const char *fn_name) {
	if (!StringUtil::CIEquals(setting, CROSSING_SETTING)) {
		throw InvalidInputException("%s: unknown setting \"%s\"; expected crossing", fn_name, setting);
	}
	auto &catalog = Catalog::GetCatalog(context, db_name);
	if (catalog.GetCatalogType() != "n6k") {
		throw InvalidInputException("%s: \"%s\" is not an n6k database", fn_name, db_name);
	}
	return catalog.Cast<N6kCatalog>().Source();
}

unique_ptr<FunctionData> GetBind(ClientContext &context, TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto &source =
	    SourceOf(context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(), "n6k_client_get");
	auto result = make_uniq<N6kClientSettingsBind>();
	result->rows = source.CrossingOperators();
	names = {"operator"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<FunctionData> SetBind(ClientContext &context, TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto &source =
	    SourceOf(context, input.inputs[0].GetValue<string>(), input.inputs[1].GetValue<string>(), "n6k_client_set");
	if (input.inputs[2].IsNull() || input.inputs[3].IsNull()) {
		throw InvalidInputException("n6k_client_set: action and operators must not be NULL");
	}
	vector<string> operators;
	for (auto &value : ListValue::GetChildren(input.inputs[3])) {
		operators.push_back(value.GetValue<string>());
	}
	auto result = make_uniq<N6kClientSettingsBind>();
	result->rows = source.SetCrossingOperators(input.inputs[2].GetValue<string>(), operators);
	names = {"operator"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kClientSettingsState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kClientSettingsBind>();
	auto &state = data_p.global_state->Cast<N6kClientSettingsState>();

	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(bind_data.rows[state.offset]));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterN6kClientSettings(ExtensionLoader &loader) {
	TableFunction get_fn("n6k_client_get", {LogicalType::VARCHAR, LogicalType::VARCHAR}, Scan, GetBind, InitGlobal);
	loader.RegisterFunction(get_fn);
	TableFunction set_fn(
	    "n6k_client_set",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    Scan, SetBind, InitGlobal);
	loader.RegisterFunction(set_fn);
}

} // namespace duckdb
