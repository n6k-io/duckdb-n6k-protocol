#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

#include <memory>

namespace duckdb {

class CatalogSession;

namespace n6k {
struct WsTableFunctionRow;
} // namespace n6k

void RegisterN6kRpc(ExtensionLoader &loader);

TableFunction MakeN6kCatalogRpcFunction(const string &function_name, std::shared_ptr<CatalogSession> session);
TableFunction MakeN6kCatalogExecFunction(std::shared_ptr<CatalogSession> session);
TableFunction MakeN6kCatalogQueryFunction(std::shared_ptr<CatalogSession> session);
//! Calls go by RPC to `row.schema`. Listed overloads that become identical once parameters are typed ANY are kept once.
TableFunctionSet MakeN6kListedFunctionSet(DatabaseInstance &db, const n6k::WsTableFunctionRow &row,
                                          std::shared_ptr<CatalogSession> session);

} // namespace duckdb
