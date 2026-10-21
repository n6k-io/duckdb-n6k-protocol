#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

#include <string>

namespace duckdb {

class ClientContext;

namespace n6k {

constexpr const char *ALL_CATALOGS = "*";

vector<std::string> ResolveServedCatalogs(ClientContext &context, const vector<Value> &inputs, idx_t first_catalog,
                                          const char *fn_name);

// A `pending` MAP(VARCHAR, VARCHAR) named parameter (catalog -> initial status message), as
// (catalog, message) pairs. Rejects a NULL or repeated name and one already in `served`. The
// catalogs need not be attached yet: that is what pending means.
vector<std::pair<std::string, std::string>>
ResolvePendingCatalogs(const Value &pending, const vector<std::string> &served, const char *fn_name);

} // namespace n6k
} // namespace duckdb
