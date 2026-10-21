#include "n6k_catalog_list.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <algorithm>

namespace duckdb {

namespace {
bool IsSystemSchema(const string &name) {
	return name == "information_schema" || name == "pg_catalog";
}
} // namespace

void ListCatalogSchemas(ClientContext &context, Catalog &catalog, vector<string> &out) {
	const idx_t first = out.size();
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) {
		if (!IsSystemSchema(s.name)) {
			out.push_back(s.name);
		}
	});
	// ScanSchemas has no defined order; sort so both servers answer the op identically.
	std::sort(out.begin() + NumericCast<int64_t>(first), out.end());
}

namespace {

struct CatalogListState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<GlobalTableFunctionState> CatalogListInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CatalogListState>();
}

unique_ptr<FunctionData> CatalogListBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	names = {"schema_name"};
	return_types = {LogicalType::VARCHAR};

	auto catalog_name = input.inputs[0].GetValue<string>();
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	auto result = make_uniq<CatalogListBindData>();
	ListCatalogSchemas(context, catalog, result->schemas);
	return std::move(result);
}

void CatalogListScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CatalogListBindData>();
	auto &state = data_p.global_state->Cast<CatalogListState>();
	idx_t count = 0;
	while (state.offset < bind_data.schemas.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(bind_data.schemas[state.offset]));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterCatalogListFunction(ExtensionLoader &loader) {
	TableFunction func("n6k_catalog_list", {LogicalType::VARCHAR}, CatalogListScan, CatalogListBind,
	                   CatalogListInitGlobal);
	CreateTableFunctionInfo info(std::move(func));
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
