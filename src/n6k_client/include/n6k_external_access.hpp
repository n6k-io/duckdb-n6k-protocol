#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {
namespace n6k {

inline void ThrowIfExternalAccessDisabled(DatabaseInstance &db) {
	if (!Settings::Get<EnableExternalAccessSetting>(db)) {
		throw PermissionException("n6k: cannot open a new connection: external access is disabled through "
		                          "configuration");
	}
}

} // namespace n6k
} // namespace duckdb
