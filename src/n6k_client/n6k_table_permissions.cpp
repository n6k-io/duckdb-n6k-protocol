#include "n6k_table_permissions.hpp"
#include "n6k_catalog_list.hpp"
#include "n6k_describe.hpp"
#include "n6k_exception_types.hpp"
#include "n6k_permissions.hpp"
#include "n6k_catalog.hpp"
#include "n6k_schema_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

static void CollectRemoteTablePermissionRows(ClientContext &context, N6kCatalog &catalog,
                                             optional_ptr<const string> schema_filter,
                                             optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	vector<reference<SchemaCatalogEntry>> schemas;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &s) {
		if (schema_filter && !StringUtil::CIEquals(s.name, *schema_filter)) {
			return;
		}
		schemas.push_back(s);
	});
	auto &attach = catalog.Attach();
	auto &source = catalog.Source();
	auto &transaction = Transaction::Get(context, catalog);
	for (auto &sref : schemas) {
		auto &s = sref.get();
		for (auto &table_name : attach.Tables(s.name, &transaction)) {
			if (table_filter && !StringUtil::CIEquals(table_name, *table_filter)) {
				continue;
			}
			auto info = source.Info(s.name, table_name);
			out.push_back({s.name, table_name, "n6k_remote", info.verbs, info.primary_keys});
		}
	}
}

static void CollectN6kPermissions(ClientContext &context, Catalog &catalog, optional_ptr<const string> schema_filter,
                                  optional_ptr<const string> table_filter, vector<TablePermissionRow> &out) {
	CollectRemoteTablePermissionRows(context, catalog.Cast<N6kCatalog>(), schema_filter, table_filter, out);
}

void RegisterN6kTablePermissions(ExtensionLoader &loader) {
	RegisterPermissionCollector(loader.GetDatabaseInstance(), "n6k", CollectN6kPermissions);
	RegisterTablePermissionsFunction(loader);
	RegisterTableDescribeFunction(loader);
	RegisterCatalogListFunction(loader);
	n6k::RegisterExceptionTypesFunction(loader);
}

} // namespace duckdb
