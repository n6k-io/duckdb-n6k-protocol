#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

// Registers n6k_table_describe: a single-row typed descriptor for one table. Idempotent across extensions.
void RegisterTableDescribeFunction(ExtensionLoader &loader);

} // namespace duckdb
