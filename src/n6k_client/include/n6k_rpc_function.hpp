#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

#include <memory>

namespace duckdb {

class CatalogSession;

void RegisterN6kRpc(ExtensionLoader &loader);

TableFunction MakeN6kCatalogRpcFunction(const string &function_name, std::shared_ptr<CatalogSession> session);
TableFunction MakeN6kCatalogExecFunction(std::shared_ptr<CatalogSession> session);
TableFunction MakeN6kCatalogQueryFunction(std::shared_ptr<CatalogSession> session);

} // namespace duckdb
