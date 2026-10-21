#include "n6k_permissions.hpp"
#include "n6k_catalog_list.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// A PK is a UNIQUE+IsPrimaryKey constraint; single-column is stored as an index, multi-column by name.
static vector<string> NativeTablePrimaryKey(TableCatalogEntry &table) {
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		if (unique.HasIndex()) {
			return {table.GetColumn(unique.GetIndex()).GetName()};
		}
		return unique.GetColumnNames();
	}
	return {};
}

static vector<string> NativeTableVerbs() {
	return {"select", "insert", "update", "delete", "alter", "drop"};
}

static vector<string> NativeViewVerbs() {
	return {"select", "drop"};
}

static void CollectNativeSchemaPermissions(ClientContext &context, SchemaCatalogEntry &schema,
                                           optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	const auto &schema_name = schema.name;
	case_insensitive_set_t seen;
	const auto matches = [&](const string &n) {
		return !table_filter || StringUtil::CIEquals(n, *table_filter);
	};
	// Tables and views share one catalog set, so classify on actual type and dedup via `seen`.
	auto scan_type = [&](CatalogType type) {
		schema.Scan(context, type, [&](CatalogEntry &e) {
			if (!matches(e.name) || seen.count(e.name)) {
				return;
			}
			if (e.type == CatalogType::VIEW_ENTRY) {
				seen.insert(e.name);
				out.push_back({schema_name, e.name, "native_view", NativeViewVerbs(), {}});
			} else if (e.type == CatalogType::TABLE_ENTRY) {
				seen.insert(e.name);
				out.push_back({schema_name, e.name, "native_table", NativeTableVerbs(),
				               NativeTablePrimaryKey(e.Cast<TableCatalogEntry>())});
			}
		});
	};
	scan_type(CatalogType::TABLE_ENTRY);
	scan_type(CatalogType::VIEW_ENTRY);
}

static void CollectCatalogPermissionsHonoringReadOnlyAttach(ClientContext &context, Catalog &catalog,
                                                            optional_ptr<const string> schema_filter,
                                                            optional_ptr<const string> table_filter,
                                                            vector<TablePermissionRow> &out) {
	// Two-pass: gather schema refs under the catalog's schema lock, then scan each after it releases.
	vector<reference<SchemaCatalogEntry>> schemas;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) {
		if (schema_filter && !StringUtil::CIEquals(s.name, *schema_filter)) {
			return;
		}
		schemas.push_back(s);
	});
	const idx_t first = out.size();
	for (auto &sref : schemas) {
		CollectNativeSchemaPermissions(context, sref.get(), table_filter, out);
	}
	// A READ_ONLY attach makes every table in the catalog unwritable no matter what its own type
	// says. Reporting more here would have the client discover otherwise only when its write is
	// rejected -- and ATTACH ... (READ_ONLY) is the documented way for a host to expose a read-only
	// surface.
	if (catalog.GetAttached().IsReadOnly()) {
		for (idx_t i = first; i < out.size(); i++) {
			out[i].verbs = {"select"};
		}
	}
}

PermissionsArgs ParsePermissionsArgs(TableFunctionBindInput &input) {
	PermissionsArgs args;
	args.catalog = input.inputs[0].GetValue<string>();
	auto sit = input.named_parameters.find("schema");
	if (sit != input.named_parameters.end() && !sit->second.IsNull()) {
		args.schema = sit->second.GetValue<string>();
		args.has_schema = true;
	}
	auto tit = input.named_parameters.find("table");
	if (tit != input.named_parameters.end() && !tit->second.IsNull()) {
		args.table = tit->second.GetValue<string>();
		args.has_table = true;
	}
	return args;
}

static void SetTablePermissionsReturnSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"schema", "name", "kind", "verbs", "primary_key"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR)};
}

namespace {
struct TablePermissionsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};
} // namespace

Value VarcharList(const vector<string> &strings) {
	vector<Value> values;
	for (auto &s : strings) {
		values.push_back(Value(s));
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(values));
}

static unique_ptr<GlobalTableFunctionState> TablePermissionsInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	return make_uniq<TablePermissionsState>();
}

static void TablePermissionsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TablePermissionsBindData>();
	auto &state = data_p.global_state->Cast<TablePermissionsState>();
	idx_t count = 0;
	while (state.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[state.offset];
		output.SetValue(0, count, Value(row.schema));
		output.SetValue(1, count, Value(row.name));
		output.SetValue(2, count, Value(row.kind));
		output.SetValue(3, count, VarcharList(row.verbs));
		output.SetValue(4, count, VarcharList(row.primary_key));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

namespace {
// Shared in the per-DatabaseInstance ObjectCache so both unlinked extensions reach one map; non-evictable.
struct PermissionCollectorRegistry : public ObjectCacheEntry {
	static string ObjectType() {
		return "n6k_permission_collectors";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

	mutex registry_lock;
	case_insensitive_map_t<PermissionCollectorFn> collectors;
};
} // namespace

void RegisterPermissionCollector(DatabaseInstance &db, const string &catalog_type, PermissionCollectorFn collector) {
	auto registry =
	    db.GetObjectCache().GetOrCreate<PermissionCollectorRegistry>(PermissionCollectorRegistry::ObjectType());
	lock_guard<mutex> guard(registry->registry_lock);
	registry->collectors[catalog_type] = collector;
}

static PermissionCollectorFn LookupPermissionCollector(ClientContext &context, const string &catalog_type) {
	auto registry = ObjectCache::GetObjectCache(context).Get<PermissionCollectorRegistry>(
	    PermissionCollectorRegistry::ObjectType());
	if (!registry) {
		return nullptr;
	}
	lock_guard<mutex> guard(registry->registry_lock);
	auto it = registry->collectors.find(catalog_type);
	return it == registry->collectors.end() ? nullptr : it->second;
}

// The virtual_catalog extensions are separate binaries; their `<prefix>_table_permissions` table
// function is the contract. Its `verbs` list passes through (minus `alter` on bridged kinds), and its open `kind` set
// folds into the kinds n6k clients already know.
static const char *VirtualCatalogPermissionsFunction(const string &catalog_type) {
	if (catalog_type == "virtual_catalog_bridge") {
		return "bridge_table_permissions";
	}
	if (catalog_type == "virtual_catalog_provider") {
		return "provider_table_permissions";
	}
	return nullptr;
}

static bool HasVerb(const vector<Value> &verbs, const char *verb) {
	for (auto &v : verbs) {
		if (StringValue::Get(v) == verb) {
			return true;
		}
	}
	return false;
}

static void CollectVirtualCatalogPermissions(ClientContext &context, Catalog &catalog, const char *function,
                                             optional_ptr<const string> schema_filter,
                                             optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	string sql = "SELECT schema, name, kind, primary_key, verbs FROM " + string(function) + "(" +
	             KeywordHelper::WriteQuoted(catalog.GetName(), '\'');
	if (schema_filter) {
		sql += ", schema := " + KeywordHelper::WriteQuoted(*schema_filter, '\'');
	}
	if (table_filter) {
		sql += ", \"table\" := " + KeywordHelper::WriteQuoted(*table_filter, '\'');
	}
	sql += ") ORDER BY schema, name";

	// A connection of its own: this runs from inside a bind on `context`, which cannot re-enter.
	Connection conn(*context.db);
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError();
	}
	while (auto chunk = result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			TablePermissionRow row;
			row.schema = StringValue::Get(chunk->GetValue(0, i));
			row.name = StringValue::Get(chunk->GetValue(1, i));
			auto kind = StringValue::Get(chunk->GetValue(2, i));
			const Value primary_key = chunk->GetValue(3, i);
			const Value verb_list = chunk->GetValue(4, i);
			for (auto &col : ListValue::GetChildren(primary_key)) {
				row.primary_key.push_back(StringValue::Get(col));
			}
			auto &verbs = ListValue::GetChildren(verb_list);
			const bool bridged = kind != "native_table" && kind != "native_view" && kind != "provider";
			for (auto &verb : verbs) {
				auto name = StringValue::Get(verb);
				// Bridge ALTER is reported by the grant but not implemented.
				if (!(bridged && name == "alter")) {
					row.verbs.push_back(std::move(name));
				}
			}
			if (!bridged) {
				row.kind = kind;
			} else {
				const bool writes = HasVerb(verbs, "insert") || HasVerb(verbs, "update") || HasVerb(verbs, "delete");
				row.kind = writes ? "bridge_readwrite" : "bridge_read";
			}
			out.push_back(std::move(row));
		}
	}
}

void CollectCatalogPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                               optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	auto collector = LookupPermissionCollector(context, catalog.GetCatalogType());
	if (collector) {
		collector(context, catalog, schema_filter, table_filter, out);
		return;
	}
	if (auto function = VirtualCatalogPermissionsFunction(catalog.GetCatalogType())) {
		CollectVirtualCatalogPermissions(context, catalog, function, schema_filter, table_filter, out);
		return;
	}
	CollectCatalogPermissionsHonoringReadOnlyAttach(context, catalog, schema_filter, table_filter, out);
}

void CollectSchemaPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                              vector<SchemaPermissionRow> &out) {
	const bool plain = !LookupPermissionCollector(context, catalog.GetCatalogType()) &&
	                   !VirtualCatalogPermissionsFunction(catalog.GetCatalogType());
	const bool creates = plain && !catalog.GetAttached().IsReadOnly();
	vector<string> names;
	ListCatalogSchemas(context, catalog, names);
	for (auto &name : names) {
		if (schema_filter && !StringUtil::CIEquals(name, *schema_filter)) {
			continue;
		}
		out.push_back({name, creates ? vector<string> {"create"} : vector<string> {}});
	}
}

static unique_ptr<FunctionData> TablePermissionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	SetTablePermissionsReturnSchema(return_types, names);

	auto args = ParsePermissionsArgs(input);
	auto &catalog = Catalog::GetCatalog(context, args.catalog);
	auto result = make_uniq<TablePermissionsBindData>();
	optional_ptr<const string> table_arg = args.has_table ? optional_ptr<const string>(&args.table) : nullptr;
	optional_ptr<const string> schema_arg = args.has_schema ? optional_ptr<const string>(&args.schema) : nullptr;
	CollectCatalogPermissions(context, catalog, schema_arg, table_arg, result->rows);
	return std::move(result);
}

void RegisterTablePermissionsFunction(ExtensionLoader &loader) {
	TableFunction func("n6k_table_permissions", {LogicalType::VARCHAR}, TablePermissionsScan, TablePermissionsBind,
	                   TablePermissionsInitGlobal);
	func.named_parameters["schema"] = LogicalType::VARCHAR;
	func.named_parameters["table"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
