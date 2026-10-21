#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterN6kTestingSqlBuilders(ExtensionLoader &loader);

} // namespace duckdb
