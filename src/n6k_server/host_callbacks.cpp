#include "host_callbacks.hpp"

#include "frame_codec.hpp"
#include "n6k_served.hpp"
#include "n6k_sql_escape.hpp"
#include "ws_json.hpp"

#include "duckdb/common/exception.hpp"

#include <unordered_set>

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
		stmt_->GetErrorObject().Throw("n6k_server: host callback \"" + udf_ + "\" is unusable: ");
	}
}

Value HostCallback::Call(vector<Value> args) {
	// vector<Value>, not the variadic Execute: that overload binds std::string as a BLOB.
	auto result = stmt_->Execute(args);
	if (result->HasError()) {
		result->ThrowError("n6k_server: host callback \"" + udf_ + "\" failed: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("n6k_server: host callback \"%s\" returned no row", udf_);
	}
	return chunk->GetValue(0, 0);
}

static duckdb_yyjson::yyjson_val *ObjectMember(duckdb_yyjson::yyjson_val *root, const char *key) {
	auto *value = duckdb_yyjson::yyjson_obj_get(root, key);
	if (value && !duckdb_yyjson::yyjson_is_obj(value) && !duckdb_yyjson::yyjson_is_null(value)) {
		throw InvalidInputException("n6k_server: authenticate: \"%s\" must be an object", key);
	}
	return value && duckdb_yyjson::yyjson_is_obj(value) ? value : nullptr;
}

AuthResult ParseAuthResult(const std::string &json) {
	JsonDoc doc(json);
	auto *root = doc.Root();
	if (!root || !duckdb_yyjson::yyjson_is_obj(root)) {
		throw InvalidInputException("n6k_server: authenticate must return a JSON object");
	}
	AuthResult result;
	if (duckdb_yyjson::yyjson_obj_get(root, "reject")) {
		result.reject = true;
		result.reason = JsonGetStr(root, "reject");
		if (result.reason.empty()) {
			result.reason = "unauthorized";
		}
		auto *code = duckdb_yyjson::yyjson_obj_get(root, "code");
		if (code && duckdb_yyjson::yyjson_is_int(code)) {
			result.code = static_cast<int>(duckdb_yyjson::yyjson_get_sint(code));
		}
		return result;
	}

	std::unordered_set<std::string> taken;
	auto claim = [&](const std::string &name) {
		if (!taken.insert(name).second) {
			throw InvalidInputException("n6k_server: authenticate: catalog \"%s\" is named twice", name);
		}
	};
	if (duckdb_yyjson::yyjson_obj_get(root, "catalogs")) {
		result.has_catalogs = true;
		result.catalogs = JsonGetStrArray(root, "catalogs");
		for (auto &name : result.catalogs) {
			claim(name);
		}
	}
	if (auto *pending = ObjectMember(root, "pending")) {
		duckdb_yyjson::yyjson_obj_iter it;
		duckdb_yyjson::yyjson_obj_iter_init(pending, &it);
		while (auto *key = duckdb_yyjson::yyjson_obj_iter_next(&it)) {
			auto *message = duckdb_yyjson::yyjson_obj_iter_get_val(key);
			std::string name(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key));
			claim(name);
			result.pending.push_back(PendingCatalogSpec {
			    name, duckdb_yyjson::yyjson_is_str(message) ? duckdb_yyjson::yyjson_get_str(message) : ""});
		}
	}
	if (auto *lazy = ObjectMember(root, "lazy")) {
		duckdb_yyjson::yyjson_obj_iter it;
		duckdb_yyjson::yyjson_obj_iter_init(lazy, &it);
		while (auto *key = duckdb_yyjson::yyjson_obj_iter_next(&it)) {
			auto *rank = duckdb_yyjson::yyjson_obj_iter_get_val(key);
			std::string name(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key));
			claim(name);
			result.lazy.emplace_back(name, duckdb_yyjson::yyjson_is_int(rank) ? duckdb_yyjson::yyjson_get_sint(rank)
			                                                                  : NO_RANK);
		}
	}
	for (auto &cap : JsonGetStrArray(root, "app_capabilities")) {
		if (IntersectCaps(std::vector<std::string> {cap}, APP_HELLO_CAPS).empty()) {
			throw InvalidInputException("n6k_server: authenticate: unknown app capability \"%s\"", cap);
		}
		result.app_capabilities.push_back(cap);
	}
	return result;
}

} // namespace n6k
} // namespace duckdb
