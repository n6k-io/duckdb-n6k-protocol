#include "n6k_describe.hpp"

#include "n6k_permissions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <set>

namespace duckdb {

struct DescribeColumn {
	string name;
	string type;
	bool nullable = true;
	bool has_default = false;
	string default_value;
	vector<string> enum_values;
};

struct TableDescribeBindData : public TableFunctionData {
	vector<DescribeColumn> columns;
	vector<string> primary_key;
	vector<string> verbs;
};

namespace {
struct TableDescribeState : public GlobalTableFunctionState {
	bool done = false;
};
} // namespace

// Kept in one place so bind-time return schema and scan-time construction stay in sync.
static LogicalType ColumnStructType() {
	child_list_t<LogicalType> children;
	children.push_back(make_pair("name", LogicalType::VARCHAR));
	children.push_back(make_pair("type", LogicalType::VARCHAR));
	children.push_back(make_pair("nullable", LogicalType::BOOLEAN));
	children.push_back(make_pair("default", LogicalType::VARCHAR));
	return LogicalType::STRUCT(std::move(children));
}

static void SetTableDescribeReturnSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"columns", "primary_key", "verbs", "enums"};
	return_types = {LogicalType::LIST(ColumnStructType()), LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR))};
}

static vector<string> EnumLabels(const LogicalType &type) {
	vector<string> out;
	if (type.id() != LogicalTypeId::ENUM) {
		return out;
	}
	idx_t size = EnumType::GetSize(type);
	for (idx_t i = 0; i < size; i++) {
		out.push_back(EnumType::GetString(type, i).GetString());
	}
	return out;
}

static void DescribeTableColumns(TableCatalogEntry &table, vector<DescribeColumn> &out) {
	std::set<idx_t> not_null;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			not_null.insert(constraint->Cast<NotNullConstraint>().index.index);
		}
	}
	for (auto &col : table.GetColumns().Logical()) {
		DescribeColumn dc;
		dc.name = col.Name();
		dc.type = col.Type().ToString();
		dc.nullable = not_null.find(col.Logical().index) == not_null.end();
		dc.has_default = col.HasDefaultValue();
		if (dc.has_default) {
			dc.default_value = col.DefaultValue().ToString();
		}
		dc.enum_values = EnumLabels(col.Type());
		out.push_back(std::move(dc));
	}
}

static void BindViewAndCollectColumns(ClientContext &context, ViewCatalogEntry &view, vector<DescribeColumn> &out) {
	// A view must be bound before its columns are known, and binding fails if an underlying object is
	// gone. Let that propagate: swallowing it describes a broken view as having zero columns, which the
	// caller cannot tell apart from a legitimately empty one. A missing *table* already throws here, so
	// reporting a broken view as healthy-but-empty was the odd one out.
	view.BindView(context);
	auto info = view.GetColumnInfo();
	if (!info) {
		throw InvalidInputException("n6k: view \"%s\" has no column info after binding", view.name);
	}
	for (idx_t i = 0; i < info->types.size(); i++) {
		DescribeColumn dc;
		dc.name = i < info->names.size() ? info->names[i] : "col" + std::to_string(i);
		dc.type = info->types[i].ToString();
		dc.enum_values = EnumLabels(info->types[i]);
		out.push_back(std::move(dc));
	}
}

static unique_ptr<FunctionData> TableDescribeBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	SetTableDescribeReturnSchema(return_types, names);

	auto args = ParsePermissionsArgs(input);
	if (!args.has_table) {
		throw BinderException("n6k_table_describe requires a \"table\" argument: "
		                      "n6k_table_describe('catalog', schema := 'main', \"table\" := 'foo')");
	}
	string schema_name = args.has_schema ? args.schema : string(DEFAULT_SCHEMA);

	auto &catalog = Catalog::GetCatalog(context, args.catalog);
	auto entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema_name, args.table, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		throw CatalogException("n6k_table_describe: table \"%s.%s.%s\" not found", args.catalog, schema_name,
		                       args.table);
	}

	auto result = make_uniq<TableDescribeBindData>();
	if (entry->type == CatalogType::VIEW_ENTRY) {
		BindViewAndCollectColumns(context, entry->Cast<ViewCatalogEntry>(), result->columns);
	} else {
		DescribeTableColumns(entry->Cast<TableCatalogEntry>(), result->columns);
	}

	// PK and verbs reuse the n6k_table_permissions collectors, filtered to this table.
	vector<TablePermissionRow> perms;
	const string schema_filter = schema_name;
	const string table_filter = args.table;
	CollectCatalogPermissions(context, catalog, &schema_filter, &table_filter, perms);
	for (auto &row : perms) {
		if (StringUtil::CIEquals(row.name, args.table)) {
			result->verbs = row.verbs;
			result->primary_key = row.primary_key;
			break;
		}
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> TableDescribeInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_uniq<TableDescribeState>();
}

static void TableDescribeScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TableDescribeBindData>();
	auto &state = data_p.global_state->Cast<TableDescribeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	vector<Value> column_vals;
	for (auto &c : bind_data.columns) {
		child_list_t<Value> fields;
		fields.push_back(make_pair("name", Value(c.name)));
		fields.push_back(make_pair("type", Value(c.type)));
		fields.push_back(make_pair("nullable", Value::BOOLEAN(c.nullable)));
		fields.push_back(make_pair("default", c.has_default ? Value(c.default_value) : Value(LogicalType::VARCHAR)));
		column_vals.push_back(Value::STRUCT(std::move(fields)));
	}
	output.SetValue(0, 0, Value::LIST(ColumnStructType(), std::move(column_vals)));

	output.SetValue(1, 0, VarcharList(bind_data.primary_key));
	output.SetValue(2, 0, VarcharList(bind_data.verbs));

	vector<Value> enum_keys;
	vector<Value> enum_values;
	for (auto &c : bind_data.columns) {
		if (c.enum_values.empty()) {
			continue;
		}
		enum_keys.push_back(Value(c.name));
		vector<Value> labels;
		for (auto &label : c.enum_values) {
			labels.push_back(Value(label));
		}
		enum_values.push_back(Value::LIST(LogicalType::VARCHAR, std::move(labels)));
	}
	output.SetValue(3, 0,
	                Value::MAP(LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR), std::move(enum_keys),
	                           std::move(enum_values)));

	state.done = true;
	output.SetCardinality(1);
}

void RegisterTableDescribeFunction(ExtensionLoader &loader) {
	TableFunction func("n6k_table_describe", {LogicalType::VARCHAR}, TableDescribeScan, TableDescribeBind,
	                   TableDescribeInitGlobal);
	func.named_parameters["schema"] = LogicalType::VARCHAR;
	func.named_parameters["table"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
