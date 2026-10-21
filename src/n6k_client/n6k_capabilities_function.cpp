#include "n6k_capabilities_function.hpp"
#include "n6k_catalog.hpp"
#include "n6k_protocol_generated.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

namespace {

struct N6kCapabilitiesBind : public TableFunctionData {
	vector<std::pair<string, string>> rows;
};

struct N6kCapabilitiesState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                              vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto &catalog = Catalog::GetCatalog(context, db_name);
	auto &n6k_catalog = catalog.Cast<N6kCatalog>();

	auto result = make_uniq<N6kCapabilitiesBind>();
	auto caps = n6k_catalog.session->GetCapabilities();
	for (auto &cap : caps.hello) {
		result->rows.emplace_back(cap, "connect");
	}
	for (auto &cap : caps.open) {
		result->rows.emplace_back(cap, "open");
	}

	names = {"capability", "level"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kCapabilitiesState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kCapabilitiesBind>();
	auto &state = data_p.global_state->Cast<N6kCapabilitiesState>();

	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(bind_data.rows[state.offset].first));
		output.SetValue(1, count, Value(bind_data.rows[state.offset].second));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterN6kCapabilities(ExtensionLoader &loader) {
	TableFunction fn("n6k_capabilities", {LogicalType::VARCHAR}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
