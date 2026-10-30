#pragma once

#include "serve_reactor.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement.hpp"

#include <string>
#include <utility>
#include <vector>

namespace duckdb {
namespace n6k {

// One host UDF, prepared once on a served Connection of its own. Not thread-safe: one per calling thread.
class HostCallback {
public:
	HostCallback(DatabaseInstance &db, std::string udf, idx_t arity);

	// The UDF's result, NULL included. Throws whatever the UDF or the statement raised, or IOException when it
	// returns no row.
	Value Call(vector<Value> args);

private:
	Connection conn_;
	std::string udf_;
	unique_ptr<PreparedStatement> stmt_;
};

// What authenticate returned:
//   {"reject": reason, "code": n}
//   {"catalogs": [...], "pending": {name: message}, "lazy": {name: rank|null}, "app_capabilities": [...]}
// every key optional; "catalogs" absent keeps the serve's own.
struct AuthResult {
	bool reject = false;
	std::string reason;
	int code = 4400;
	bool has_catalogs = false;
	std::vector<std::string> catalogs;
	std::vector<PendingCatalogSpec> pending;
	std::vector<std::pair<std::string, int64_t>> lazy;
	std::vector<std::string> app_capabilities;
};

// Throws InvalidInputException on a non-object root, non-object "pending" or "lazy", "catalogs" or
// "app_capabilities" not a string array, a repeated catalog name, or an app capability outside APP_HELLO_CAPS.
// Other mistyped values fall back to their defaults; unknown keys are ignored.
AuthResult ParseAuthResult(const std::string &json);

} // namespace n6k
} // namespace duckdb
