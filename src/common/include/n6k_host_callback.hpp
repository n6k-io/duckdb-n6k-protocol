#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement.hpp"

#include <string>

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

} // namespace n6k
} // namespace duckdb
