#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ExtensionLoader;

// OP_SCHEMA_LIST: the schemas of one catalog.
//
// One implementation and one ordering, rather than copies that drift apart. The C++ reactor
// (src/n6k_server/request_handlers.cpp) calls ListCatalogSchemas directly; the n6k_catalog_list
// table function exposes the same result to SQL callers.
//
// Catalog metadata is read directly rather than through information_schema, so a caller in any
// language needs no SQL of its own. Results are sorted by name; the system schemas
// (information_schema, pg_catalog) are excluded.
void ListCatalogSchemas(ClientContext &context, Catalog &catalog, vector<string> &out);

struct CatalogListBindData : public TableFunctionData {
	vector<string> schemas;
};

// Registers the shared n6k_catalog_list table function; idempotent across extensions.
void RegisterCatalogListFunction(ExtensionLoader &loader);

} // namespace duckdb
