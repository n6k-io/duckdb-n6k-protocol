#include "n6k_host_callback.hpp"

#include "n6k_served.hpp"
#include "n6k_sql_escape.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace n6k {

HostCallback::HostCallback(DatabaseInstance &db, std::string udf, idx_t arity) : conn_(db), udf_(std::move(udf)) {
	MarkServed(*conn_.context);
	std::string sql = "SELECT " + QuoteIdent(udf_) + "(";
	for (idx_t i = 0; i < arity; i++) {
		sql += i == 0 ? "?" : ", ?";
	}
	sql += ")";
	stmt_ = conn_.Prepare(sql);
	if (stmt_->HasError()) {
		stmt_->GetErrorObject().Throw("n6k: host callback \"" + udf_ + "\" is unusable: ");
	}
}

Value HostCallback::Call(vector<Value> args) {
	// vector<Value>, not the variadic Execute: that overload binds std::string as a BLOB.
	auto result = stmt_->Execute(args);
	if (result->HasError()) {
		result->ThrowError("n6k: host callback \"" + udf_ + "\" failed: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("n6k: host callback \"%s\" returned no row", udf_);
	}
	return chunk->GetValue(0, 0);
}

} // namespace n6k
} // namespace duckdb
