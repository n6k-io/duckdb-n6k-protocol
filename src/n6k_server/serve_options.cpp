#include "serve_options.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

#include <limits>

namespace duckdb {
namespace n6k {

static const Value *FindNamed(const TableFunctionBindInput &input, const char *name) {
	auto it = input.named_parameters.find(name);
	if (it == input.named_parameters.end() || it->second.IsNull()) {
		return nullptr;
	}
	return &it->second;
}

static std::string BindUdfName(const TableFunctionBindInput &input, const char *name, const char *fn_name) {
	auto value = FindNamed(input, name);
	if (!value) {
		return {};
	}
	auto udf = value->GetValue<string>();
	if (udf.empty()) {
		throw BinderException("%s: %s must name a scalar function", fn_name, name);
	}
	return udf;
}

static int BindMs(const TableFunctionBindInput &input, const char *name, const char *fn_name, int64_t min,
                  int fallback) {
	auto value = FindNamed(input, name);
	if (!value) {
		return fallback;
	}
	auto ms = value->GetValue<int64_t>();
	if (ms < min || ms > std::numeric_limits<int>::max()) {
		throw BinderException("%s: %s must be %s", fn_name, name, min == 0 ? "0 (disabled) or positive" : "positive");
	}
	return static_cast<int>(ms);
}

static int BindWorkers(const TableFunctionBindInput &input, const char *fn_name, int fallback) {
	auto value = FindNamed(input, "workers");
	if (!value) {
		return fallback;
	}
	auto workers = value->GetValue<int64_t>();
	if (workers < 1 || workers > MAX_CONCURRENT_REQS) {
		throw BinderException("%s: workers must be between 1 and %d", fn_name, MAX_CONCURRENT_REQS);
	}
	return static_cast<int>(workers);
}

void AddServeOptionParameters(TableFunction &fn) {
	fn.named_parameters["authenticate"] = LogicalType::VARCHAR;
	fn.named_parameters["on_text"] = LogicalType::VARCHAR;
	fn.named_parameters["resolve"] = LogicalType::VARCHAR;
	fn.named_parameters["hello_pending_message"] = LogicalType::VARCHAR;
	fn.named_parameters["hello_pending_first_ms"] = LogicalType::BIGINT;
	fn.named_parameters["ping_interval_ms"] = LogicalType::BIGINT;
	fn.named_parameters["open_pending_interval_ms"] = LogicalType::BIGINT;
	fn.named_parameters["workers"] = LogicalType::BIGINT;
	fn.named_parameters["idle_stream_timeout_ms"] = LogicalType::BIGINT;
	fn.named_parameters["idle_txn_timeout_ms"] = LogicalType::BIGINT;
}

ServeOptions BindServeOptions(const TableFunctionBindInput &input, const char *fn_name) {
	ServeOptions options;
	options.authenticate = BindUdfName(input, "authenticate", fn_name);
	options.on_text = BindUdfName(input, "on_text", fn_name);
	options.resolve = BindUdfName(input, "resolve", fn_name);
	if (auto message = FindNamed(input, "hello_pending_message")) {
		options.hello_pending_message = message->GetValue<string>();
	}
	options.hello_pending_first_ms =
	    BindMs(input, "hello_pending_first_ms", fn_name, 1, options.hello_pending_first_ms);
	options.ping_interval_ms = BindMs(input, "ping_interval_ms", fn_name, 0, options.ping_interval_ms);
	options.open_pending_interval_ms =
	    BindMs(input, "open_pending_interval_ms", fn_name, 1, options.open_pending_interval_ms);
	options.workers = BindWorkers(input, fn_name, options.workers);
	options.idle_stream_timeout_ms =
	    BindMs(input, "idle_stream_timeout_ms", fn_name, 0, options.idle_stream_timeout_ms);
	options.idle_txn_timeout_ms = BindMs(input, "idle_txn_timeout_ms", fn_name, 0, options.idle_txn_timeout_ms);
	return options;
}

} // namespace n6k
} // namespace duckdb
