#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

shared_ptr<StorageExtension> CreateN6kStorageExtension();

} // namespace duckdb
