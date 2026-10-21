#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

void RegisterN6kTablePermissions(ExtensionLoader &loader);

} // namespace duckdb
