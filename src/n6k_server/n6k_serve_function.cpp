#include "n6k_serve_function.hpp"

#include "frame_codec.hpp"
#include "n6k_fd_hello.hpp"
#include "n6k_served.hpp"
#include "serve_bind_common.hpp"
#include "serve_clients.hpp"
#include "serve_options.hpp"
#include "serve_reactor.hpp"
#include "uds_transport.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <atomic>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

static constexpr const char *LISTEN_ENV = "N6K_LISTEN";
static constexpr int ACCEPT_POLL_MS = 250;
static constexpr const char *FN_NAME = "n6k_serve_socket";
static constexpr const char *FD_FN_NAME = "n6k_serve_fd";
static constexpr const char *CREATE_SESSION_FN_NAME = "n6k_serve_create_session";
static constexpr const char *PUSH_FN_NAME = "n6k_serve_push_invalidate";
static constexpr const char *STATS_FN_NAME = "n6k_serve_stats";
static constexpr const char *OPEN_READY_FN_NAME = "n6k_serve_open_ready";
static constexpr const char *OPEN_FAILED_FN_NAME = "n6k_serve_open_failed";
static constexpr const char *SEND_TEXT_FN_NAME = "n6k_serve_send_text";
static constexpr const char *CLOSE_FN_NAME = "n6k_serve_close";
static constexpr const char *SESSION_STATS_FN_NAME = "n6k_serve_session_stats";
static constexpr const char *UPSTREAM_HELLO_FN_NAME = "n6k_upstream_hello";
static constexpr const char *ADD_UPSTREAM_FN_NAME = "n6k_serve_add_upstream";
static constexpr const char *REMOVE_UPSTREAM_FN_NAME = "n6k_serve_remove_upstream";
static constexpr const char *UPSTREAM_STATUS_FN_NAME = "n6k_serve_upstream_status";
static constexpr const char *SET_UPSTREAM_RANK_FN_NAME = "n6k_serve_set_upstream_rank";
static constexpr const char *ADD_CATALOG_FN_NAME = "n6k_serve_add_catalog";
static constexpr const char *ADD_LAZY_CATALOG_FN_NAME = "n6k_serve_add_lazy_catalog";
static constexpr const char *REMOVE_CATALOG_FN_NAME = "n6k_serve_remove_catalog";
static constexpr const char *PAUSE_FN_NAME = "n6k_serve_pause";
static constexpr const char *RESUME_FN_NAME = "n6k_serve_resume";
static constexpr const char *INFLIGHT_FN_NAME = "n6k_serve_inflight";
static constexpr const char *CANCEL_INFLIGHT_FN_NAME = "n6k_serve_cancel_inflight";

namespace {

struct N6kServeBindData : public TableFunctionData {
	vector<string> catalogs;
	// `socket_path` doubles as the label reported in the result row.
	string socket_path;
	bool listen = false;
	hugeint_t session_id;
	n6k::ServeOptions options;
};

struct N6kServeState : public GlobalTableFunctionState {
	bool done = false;
};

struct N6kResolveOpenBindData : public TableFunctionData {
	hugeint_t session_id;
	string catalog;
	bool ok = true;
	string message;
};

enum class ServedCatalogOp { ADD, ADD_LAZY, REMOVE, PAUSE, RESUME, INFLIGHT, CANCEL_INFLIGHT };

struct N6kServedCatalogBindData : public TableFunctionData {
	hugeint_t session_id;
	string catalog;
	ServedCatalogOp op = ServedCatalogOp::ADD;
	int64_t rank = n6k::NO_RANK;
};

struct N6kSendTextBindData : public TableFunctionData {
	hugeint_t session_id;
	string text;
};

struct N6kSessionStatsBindData : public TableFunctionData {
	hugeint_t session_id;
};

struct N6kCloseBindData : public TableFunctionData {
	hugeint_t session_id;
	int code = 0;
	string reason;
};

struct N6kUpstreamHelloBindData : public TableFunctionData {
	int fd = -1;
	string token;
};

enum class UpstreamOp { ADD, SET_RANK, REMOVE, STATUS };

struct N6kUpstreamBindData : public TableFunctionData {
	hugeint_t session_id;
	int up_fd = -1;
	vector<string> catalogs;
	int64_t rank = 0;
	UpstreamOp op = UpstreamOp::ADD;
};

struct N6kCreateSessionBindData : public TableFunctionData {
	int fd = -1;
};

struct N6kPushInvalidateBindData : public TableFunctionData {
	string catalog;
	vector<string> schemas;
};

} // namespace

static void N6kServeReturnSchema(vector<LogicalType> &return_types, vector<string> &names) {
	names = {"connected", "requests_handled", "socket"};
	return_types = {LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::VARCHAR};
}

static int BindFdArgument(const TableFunctionBindInput &input, const char *fn_name) {
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s requires a file descriptor: CALL %s(7)", fn_name, fn_name);
	}
	auto fd = input.inputs[0].GetValue<int64_t>();
	if (fd < 0 || fd > static_cast<int64_t>(std::numeric_limits<int>::max())) {
		throw BinderException("%s: %s is not a valid file descriptor", fn_name, std::to_string(fd));
	}
	return static_cast<int>(fd);
}

static hugeint_t BindSessionIdArgument(const TableFunctionBindInput &input, const char *fn_name) {
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s requires a session id from %s", fn_name, CREATE_SESSION_FN_NAME);
	}
	return input.inputs[0].GetValue<hugeint_t>();
}

static unique_ptr<FunctionData> N6kServeBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	N6kServeReturnSchema(return_types, names);
	n6k::ThrowIfServed(context, FN_NAME);

	auto catalogs = n6k::ResolveServedCatalogs(context, input.inputs, 0, FN_NAME);

	string listen_path;
	auto listen = input.named_parameters.find("listen");
	if (listen != input.named_parameters.end() && !listen->second.IsNull()) {
		listen_path = listen->second.GetValue<string>();
		if (listen_path.empty()) {
			throw BinderException("%s: listen must name a Unix domain socket path", FN_NAME);
		}
	} else if (const char *env = std::getenv(LISTEN_ENV)) {
		listen_path = env;
	}
	if (listen_path.empty()) {
		throw BinderException("%s: neither listen => nor %s is set; one must name the Unix domain socket to listen on",
		                      FN_NAME, LISTEN_ENV);
	}
	if (!n6k::UdsPathFits(listen_path)) {
		throw BinderException("%s: listen path too long (%lu bytes, max %lu): %s", FN_NAME,
		                      static_cast<uint64_t>(listen_path.size()), n6k::UdsPathMax(), listen_path);
	}

	auto result = make_uniq<N6kServeBindData>();
	result->catalogs = std::move(catalogs);
	result->socket_path = std::move(listen_path);
	result->listen = true;
	return std::move(result);
}

// Same server, same result row; the fd arrives through a session from n6k_serve_create_session.
static unique_ptr<FunctionData> N6kServeFdBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	N6kServeReturnSchema(return_types, names);
	n6k::ThrowIfServed(context, FD_FN_NAME);

	auto result = make_uniq<N6kServeBindData>();
	result->session_id = BindSessionIdArgument(input, FD_FN_NAME);
	result->catalogs = n6k::ResolveServedCatalogs(context, input.inputs, 1, FD_FN_NAME);
	result->socket_path = "session:" + UUID::ToString(result->session_id);
	result->options = n6k::BindServeOptions(input, FD_FN_NAME);
	auto request = input.named_parameters.find("request");
	if (request != input.named_parameters.end() && !request->second.IsNull()) {
		result->options.request = request->second.GetValue<string>();
		result->options.has_request = true;
	}
	return std::move(result);
}

static void RequireAttached(ClientContext &context, const string &catalog, const char *fn_name) {
	try {
		(void)Catalog::GetCatalog(context, catalog);
	} catch (const std::exception &) {
		throw BinderException("%s: catalog \"%s\" is not attached; ATTACH it before %s", fn_name, catalog, fn_name);
	}
}

static unique_ptr<FunctionData> N6kResolveOpenBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names,
                                                   const char *fn_name, bool ok) {
	names = {"resolved"};
	return_types = {LogicalType::BOOLEAN};
	// A served client reaching this through OP_EXEC could settle another socket's pending open.
	n6k::ThrowIfServed(context, fn_name);
	auto result = make_uniq<N6kResolveOpenBindData>();
	result->session_id = BindSessionIdArgument(input, fn_name);
	if (input.inputs.size() < 2 || input.inputs[1].IsNull()) {
		throw BinderException("%s requires a catalog name", fn_name);
	}
	result->catalog = input.inputs[1].GetValue<string>();
	result->ok = ok;
	if (ok) {
		RequireAttached(context, result->catalog, fn_name);
	} else {
		if (input.inputs.size() < 3 || input.inputs[2].IsNull()) {
			throw BinderException("%s requires a message", fn_name);
		}
		result->message = input.inputs[2].GetValue<string>();
	}
	return std::move(result);
}

static unique_ptr<FunctionData> N6kOpenReadyBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	return N6kResolveOpenBind(context, input, return_types, names, OPEN_READY_FN_NAME, true);
}

static unique_ptr<FunctionData> N6kOpenFailedBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return N6kResolveOpenBind(context, input, return_types, names, OPEN_FAILED_FN_NAME, false);
}

static void N6kResolveOpenScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kResolveOpenBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	n6k::ResolvePendingOpen(DatabaseInstance::GetDatabase(context), bind_data.session_id, bind_data.catalog,
	                        bind_data.ok, bind_data.message);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kSendTextBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names = {"sent"};
	return_types = {LogicalType::BOOLEAN};
	n6k::ThrowIfServed(context, SEND_TEXT_FN_NAME);
	auto result = make_uniq<N6kSendTextBindData>();
	result->session_id = BindSessionIdArgument(input, SEND_TEXT_FN_NAME);
	if (input.inputs.size() < 2 || input.inputs[1].IsNull()) {
		throw BinderException("%s requires the text to send", SEND_TEXT_FN_NAME);
	}
	result->text = input.inputs[1].GetValue<string>();
	return std::move(result);
}

static void N6kSendTextScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kSendTextBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	n6k::SendServedText(DatabaseInstance::GetDatabase(context), bind_data.session_id, bind_data.text);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kCloseBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	names = {"closed"};
	return_types = {LogicalType::BOOLEAN};
	n6k::ThrowIfServed(context, CLOSE_FN_NAME);
	auto result = make_uniq<N6kCloseBindData>();
	result->session_id = BindSessionIdArgument(input, CLOSE_FN_NAME);
	if (input.inputs.size() < 2 || input.inputs[1].IsNull()) {
		throw BinderException("%s requires a close code", CLOSE_FN_NAME);
	}
	result->code = input.inputs[1].GetValue<int32_t>();
	if (result->code < 1000 || result->code > 4999) {
		throw BinderException("%s: close code %d is out of range (1000-4999)", CLOSE_FN_NAME, result->code);
	}
	if (input.inputs.size() >= 3 && !input.inputs[2].IsNull()) {
		result->reason = input.inputs[2].GetValue<string>();
	}
	return std::move(result);
}

static void N6kCloseScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kCloseBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	n6k::CloseServed(DatabaseInstance::GetDatabase(context), bind_data.session_id, bind_data.code, bind_data.reason);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kSessionStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names = {"frames_in",        "bytes_in",    "frames_out",      "bytes_out",
	         "requests_handled", "duration_ms", "authenticate_ms", "peak_workers"};
	return_types = vector<LogicalType>(names.size(), LogicalType::BIGINT);
	n6k::ThrowIfServed(context, SESSION_STATS_FN_NAME);
	auto result = make_uniq<N6kSessionStatsBindData>();
	result->session_id = BindSessionIdArgument(input, SESSION_STATS_FN_NAME);
	return std::move(result);
}

static void N6kSessionStatsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kSessionStatsBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto stats = n6k::ServedSessionStats(DatabaseInstance::GetDatabase(context), bind_data.session_id);
	const vector<int64_t> values {stats.frames_in,        stats.bytes_in,    stats.frames_out,      stats.bytes_out,
	                              stats.requests_handled, stats.duration_ms, stats.authenticate_ms, stats.peak_workers};
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		output.SetValue(i, 0, Value::BIGINT(values[i]));
	}
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kUpstreamHelloBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names = {"catalogs", "capabilities"};
	return_types = {LogicalType::LIST(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR)};
	n6k::ThrowIfServed(context, UPSTREAM_HELLO_FN_NAME);
	auto result = make_uniq<N6kUpstreamHelloBindData>();
	result->fd = BindFdArgument(input, UPSTREAM_HELLO_FN_NAME);
	if (input.inputs.size() >= 2 && !input.inputs[1].IsNull()) {
		result->token = input.inputs[1].GetValue<string>();
	}
	return std::move(result);
}

static vector<Value> VarcharValues(const std::vector<std::string> &items) {
	vector<Value> values;
	for (auto &item : items) {
		values.emplace_back(item);
	}
	return values;
}

// The HELLO, then CATALOG_LIST: HELLO_ACK names no catalogs. The upstream may PING once it has acked.
static void N6kUpstreamHelloScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kUpstreamHelloBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto capabilities = n6k::HelloOnFd(context, bind_data.fd, bind_data.token, UPSTREAM_HELLO_FN_NAME);
	n6k::FdReplyWait wait {context, UPSTREAM_HELLO_FN_NAME,
	                       std::chrono::steady_clock::now() + std::chrono::milliseconds(n6k::OPEN_PENDING_STALL_MS),
	                       "no reply to CATALOG_LIST within " + std::to_string(n6k::OPEN_PENDING_STALL_MS) + "ms"};
	static constexpr uint32_t LIST_REQ_ID = 1;
	n6k::WriteN6kFrameOnFd(
	    bind_data.fd, n6k::PackConnectionReq(static_cast<int>(n6k::FrameType::REQ), LIST_REQ_ID, n6k::OP_CATALOG_LIST),
	    wait);
	vector<string> catalogs;
	for (;;) {
		auto oh = n6k::ReadN6kFrameOnFd(bind_data.fd, wait);
		const auto &h = oh.get();
		const auto type = static_cast<n6k::FrameType>(n6k::GetInt(h, "t", -1));
		if (type == n6k::FrameType::PING) {
			n6k::WriteN6kFrameOnFd(bind_data.fd, n6k::PackPong(static_cast<uint32_t>(n6k::GetInt(h, "id", 0))), wait);
			continue;
		}
		if (n6k::GetInt(h, "id", 0) != LIST_REQ_ID || n6k::HasKey(h, "ns")) {
			continue;
		}
		if (type == n6k::FrameType::RESP_CHUNK) {
			auto chunk = n6k::GetStrArray(h, "catalogs");
			catalogs.insert(catalogs.end(), chunk.begin(), chunk.end());
		} else if (type == n6k::FrameType::RESP_END) {
			break;
		} else if (type == n6k::FrameType::ERR) {
			throw IOException("%s: CATALOG_LIST failed: %s", UPSTREAM_HELLO_FN_NAME,
			                  n6k::GetStr(h, "exception_message"));
		}
	}
	output.SetValue(0, 0, Value::LIST(LogicalType::VARCHAR, VarcharValues(catalogs)));
	output.SetValue(1, 0, Value::LIST(LogicalType::VARCHAR, VarcharValues(capabilities)));
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kUpstreamBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names,
                                                const char *fn_name, UpstreamOp op) {
	switch (op) {
	case UpstreamOp::ADD:
	case UpstreamOp::SET_RANK:
		names = {"skipped"};
		return_types = {LogicalType::LIST(LogicalType::VARCHAR)};
		break;
	case UpstreamOp::REMOVE:
		names = {"removed"};
		return_types = {LogicalType::BOOLEAN};
		break;
	case UpstreamOp::STATUS:
		names = {"open", "reason"};
		return_types = {LogicalType::BOOLEAN, LogicalType::VARCHAR};
		break;
	}
	// A served client reaching this through OP_EXEC could reroute another socket's sessions.
	n6k::ThrowIfServed(context, fn_name);
	auto result = make_uniq<N6kUpstreamBindData>();
	result->session_id = BindSessionIdArgument(input, fn_name);
	if (input.inputs.size() < 2 || input.inputs[1].IsNull()) {
		throw BinderException("%s requires the upstream's file descriptor", fn_name);
	}
	auto up_fd = input.inputs[1].GetValue<int64_t>();
	if (up_fd < 0 || up_fd > static_cast<int64_t>(std::numeric_limits<int>::max())) {
		throw BinderException("%s: %s is not a valid file descriptor", fn_name, std::to_string(up_fd));
	}
	result->up_fd = static_cast<int>(up_fd);
	if (op == UpstreamOp::ADD) {
		if (input.inputs.size() < 3 || input.inputs[2].IsNull()) {
			throw BinderException("%s requires a list of catalogs", fn_name);
		}
		for (auto &name : ListValue::GetChildren(input.inputs[2])) {
			if (!name.IsNull()) {
				result->catalogs.push_back(name.GetValue<string>());
			}
		}
		auto rank = input.named_parameters.find("rank");
		if (rank != input.named_parameters.end() && !rank->second.IsNull()) {
			result->rank = rank->second.GetValue<int64_t>();
		}
	}
	if (op == UpstreamOp::SET_RANK) {
		if (input.inputs.size() < 3 || input.inputs[2].IsNull()) {
			throw BinderException("%s requires a rank", fn_name);
		}
		result->rank = input.inputs[2].GetValue<int64_t>();
	}
	result->op = op;
	return std::move(result);
}

static unique_ptr<FunctionData> N6kAddUpstreamBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	return N6kUpstreamBind(context, input, return_types, names, ADD_UPSTREAM_FN_NAME, UpstreamOp::ADD);
}

static unique_ptr<FunctionData> N6kSetUpstreamRankBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	return N6kUpstreamBind(context, input, return_types, names, SET_UPSTREAM_RANK_FN_NAME, UpstreamOp::SET_RANK);
}

static unique_ptr<FunctionData> N6kRemoveUpstreamBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return N6kUpstreamBind(context, input, return_types, names, REMOVE_UPSTREAM_FN_NAME, UpstreamOp::REMOVE);
}

static unique_ptr<FunctionData> N6kUpstreamStatusBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return N6kUpstreamBind(context, input, return_types, names, UPSTREAM_STATUS_FN_NAME, UpstreamOp::STATUS);
}

static void N6kUpstreamScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kUpstreamBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	switch (bind_data.op) {
	case UpstreamOp::ADD:
		output.SetValue(0, 0,
		                Value::LIST(LogicalType::VARCHAR,
		                            VarcharValues(n6k::AddServedUpstream(db, bind_data.session_id, bind_data.up_fd,
		                                                                 bind_data.catalogs, bind_data.rank))));
		break;
	case UpstreamOp::SET_RANK:
		output.SetValue(0, 0,
		                Value::LIST(LogicalType::VARCHAR,
		                            VarcharValues(n6k::SetServedUpstreamRank(db, bind_data.session_id, bind_data.up_fd,
		                                                                     bind_data.rank))));
		break;
	case UpstreamOp::REMOVE:
		n6k::RemoveServedUpstream(db, bind_data.session_id, bind_data.up_fd);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	case UpstreamOp::STATUS: {
		auto status = n6k::ServedUpstreamStatus(db, bind_data.session_id, bind_data.up_fd);
		output.SetValue(0, 0, Value::BOOLEAN(status.open));
		output.SetValue(1, 0, status.open ? Value() : Value(status.reason));
		break;
	}
	}
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kServedCatalogBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names,
                                                     const char *fn_name, ServedCatalogOp op) {
	switch (op) {
	case ServedCatalogOp::ADD:
	case ServedCatalogOp::ADD_LAZY:
		names = {"added"};
		break;
	case ServedCatalogOp::REMOVE:
		names = {"removed"};
		break;
	case ServedCatalogOp::PAUSE:
		names = {"paused"};
		break;
	case ServedCatalogOp::RESUME:
		names = {"resumed"};
		break;
	case ServedCatalogOp::INFLIGHT:
		names = {"holding", "running"};
		break;
	case ServedCatalogOp::CANCEL_INFLIGHT:
		names = {"cancelled"};
		break;
	}
	if (op == ServedCatalogOp::INFLIGHT) {
		return_types = {LogicalType::BOOLEAN, LogicalType::BIGINT};
	} else {
		return_types = {op == ServedCatalogOp::CANCEL_INFLIGHT ? LogicalType::BIGINT : LogicalType::BOOLEAN};
	}
	// A served client reaching this through OP_EXEC could change another socket's catalogs.
	n6k::ThrowIfServed(context, fn_name);
	auto result = make_uniq<N6kServedCatalogBindData>();
	result->session_id = BindSessionIdArgument(input, fn_name);
	if (input.inputs.size() < 2 || input.inputs[1].IsNull()) {
		throw BinderException("%s requires a catalog name", fn_name);
	}
	result->catalog = input.inputs[1].GetValue<string>();
	result->op = op;
	if (op == ServedCatalogOp::ADD) {
		RequireAttached(context, result->catalog, fn_name);
	}
	auto rank = input.named_parameters.find("rank");
	if (op == ServedCatalogOp::ADD_LAZY && rank != input.named_parameters.end() && !rank->second.IsNull()) {
		result->rank = rank->second.GetValue<int64_t>();
	}
	return std::move(result);
}

static unique_ptr<FunctionData> N6kAddCatalogBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return N6kServedCatalogBind(context, input, return_types, names, ADD_CATALOG_FN_NAME, ServedCatalogOp::ADD);
}

static unique_ptr<FunctionData> N6kAddLazyCatalogBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return N6kServedCatalogBind(context, input, return_types, names, ADD_LAZY_CATALOG_FN_NAME,
	                            ServedCatalogOp::ADD_LAZY);
}

static unique_ptr<FunctionData> N6kRemoveCatalogBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return N6kServedCatalogBind(context, input, return_types, names, REMOVE_CATALOG_FN_NAME, ServedCatalogOp::REMOVE);
}

static unique_ptr<FunctionData> N6kPauseBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	return N6kServedCatalogBind(context, input, return_types, names, PAUSE_FN_NAME, ServedCatalogOp::PAUSE);
}

static unique_ptr<FunctionData> N6kResumeBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	return N6kServedCatalogBind(context, input, return_types, names, RESUME_FN_NAME, ServedCatalogOp::RESUME);
}

static unique_ptr<FunctionData> N6kInflightBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return N6kServedCatalogBind(context, input, return_types, names, INFLIGHT_FN_NAME, ServedCatalogOp::INFLIGHT);
}

static unique_ptr<FunctionData> N6kCancelInflightBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return N6kServedCatalogBind(context, input, return_types, names, CANCEL_INFLIGHT_FN_NAME,
	                            ServedCatalogOp::CANCEL_INFLIGHT);
}

static void N6kServedCatalogScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kServedCatalogBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	switch (bind_data.op) {
	case ServedCatalogOp::ADD:
		n6k::AddServedCatalog(db, bind_data.session_id, bind_data.catalog);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	case ServedCatalogOp::ADD_LAZY:
		n6k::AddServedLazyCatalog(db, bind_data.session_id, bind_data.catalog, bind_data.rank);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	case ServedCatalogOp::REMOVE:
		n6k::RemoveServedCatalog(db, bind_data.session_id, bind_data.catalog);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	case ServedCatalogOp::PAUSE:
		n6k::PauseServedCatalog(db, bind_data.session_id, bind_data.catalog);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	case ServedCatalogOp::RESUME:
		n6k::ResumeServedCatalog(db, bind_data.session_id, bind_data.catalog);
		output.SetValue(0, 0, Value::BOOLEAN(true));
		break;
	case ServedCatalogOp::INFLIGHT: {
		const auto inflight = n6k::ServedCatalogInflight(db, bind_data.session_id, bind_data.catalog);
		output.SetValue(0, 0, Value::BOOLEAN(inflight.holding));
		output.SetValue(1, 0, Value::BIGINT(inflight.running));
		break;
	}
	case ServedCatalogOp::CANCEL_INFLIGHT:
		output.SetValue(0, 0,
		                Value::BIGINT(n6k::CancelServedCatalogInflight(db, bind_data.session_id, bind_data.catalog)));
		break;
	}
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kCreateSessionBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names = {"session_id"};
	return_types = {LogicalType::UUID};
	n6k::ThrowIfServed(context, CREATE_SESSION_FN_NAME);
	auto result = make_uniq<N6kCreateSessionBindData>();
	result->fd = BindFdArgument(input, CREATE_SESSION_FN_NAME);
	return std::move(result);
}

static void N6kCreateSessionScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kCreateSessionBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	output.SetValue(0, 0, Value::UUID(n6k::CreateServeSession(DatabaseInstance::GetDatabase(context), bind_data.fd)));
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<GlobalTableFunctionState> N6kServeInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<N6kServeState>();
}

// On interrupt the accept loop exits and CloseAndJoin wakes every reactor with EOF.
static int64_t ServeListening(ClientContext &context, const N6kServeBindData &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	n6k::UdsListener listener(bind_data.socket_path);
	std::atomic<int64_t> handled {0};
	n6k::ServeClients<n6k::UdsConnection> clients;

	while (!context.IsInterrupted()) {
		clients.ReapFinished();
		int fd = listener.Accept(ACCEPT_POLL_MS);
		if (fd < 0) {
			continue;
		}
		auto accepted = std::make_shared<n6k::UdsConnection>();
		try {
			accepted->Adopt(fd);
		} catch (const std::exception &e) {
			DUCKDB_LOG_WARNING(context, "n6k_serve_socket: adopting an accepted connection failed: %s", e.what());
			continue;
		}
		const auto *catalogs = &bind_data.catalogs;
		clients.Spawn(accepted, [&db, &context, &handled, catalogs](n6k::UdsConnection &conn) {
			try {
				n6k::ServeReactor reactor(conn, db, context, *catalogs);
				handled.fetch_add(reactor.Run());
			} catch (const std::exception &e) {
				DUCKDB_LOG_WARNING(context, "n6k_serve_socket: connection reactor failed: %s", e.what());
			}
		});
	}
	clients.CloseAndJoin();
	return handled.load();
}

static void N6kServeScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kServeBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	if (bind_data.listen) {
		// This row is only declared: the loop ends on interrupt, which aborts the statement.
		output.SetValue(1, 0, Value::BIGINT(ServeListening(context, bind_data)));
		output.SetValue(0, 0, Value::BOOLEAN(true));
		output.SetValue(2, 0, Value(bind_data.socket_path));
		state.done = true;
		output.SetCardinality(1);
		return;
	}

	// n6k_serve_fd: blocks the CALLING thread for the connection lifetime — the CALL is the server. Not a
	// `SET threads` slot: a synchronous CALL runs its own root task inline, so the cost is one host thread.
	auto &db = DatabaseInstance::GetDatabase(context);
	n6k::ServeSessionClaim session(db, bind_data.session_id);
	n6k::UdsConnection conn;
	conn.AdoptBlockingDupOfFd(session.Fd());
	n6k::ServeReactor reactor(conn, db, context, bind_data.catalogs, bind_data.options, bind_data.session_id);
	int64_t handled = reactor.Run();

	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value::BIGINT(handled));
	output.SetValue(2, 0, Value(bind_data.socket_path));

	state.done = true;
	output.SetCardinality(1);
}

void RegisterN6kServeFunction(ExtensionLoader &loader) {
	TableFunction func(FN_NAME, {}, N6kServeScan, N6kServeBind, N6kServeInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	func.named_parameters["listen"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

// Tells every client currently attached to `catalog` that those schemas changed, so the next lookup
// refetches instead of serving a stale cache. The server never decides on its own that a catalog has
// changed — the host says so.
static unique_ptr<FunctionData> N6kPushInvalidateBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	n6k::ThrowIfServed(context, PUSH_FN_NAME);
	names = {"notified"};
	return_types = {LogicalType::BIGINT};

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s requires a catalog: CALL %s('mydata', 'main')", PUSH_FN_NAME, PUSH_FN_NAME);
	}
	auto result = make_uniq<N6kPushInvalidateBindData>();
	result->catalog = input.inputs[0].GetValue<string>();
	for (idx_t i = 1; i < input.inputs.size(); i++) {
		if (input.inputs[i].IsNull()) {
			throw BinderException("%s: schema names must not be NULL", PUSH_FN_NAME);
		}
		result->schemas.push_back(input.inputs[i].GetValue<string>());
	}
	if (result->schemas.empty()) {
		throw BinderException("%s requires at least one schema name: CALL %s('mydata', 'main')", PUSH_FN_NAME,
		                      PUSH_FN_NAME);
	}
	return std::move(result);
}

static void N6kPushInvalidateScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kPushInvalidateBindData>();
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	const int notified = n6k::BroadcastInvalidate(db, bind_data.catalog, bind_data.schemas);

	output.SetValue(0, 0, Value::BIGINT(notified));
	state.done = true;
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> N6kServeStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	n6k::ThrowIfServed(context, STATS_FN_NAME);
	names = {"connections", "requests_handled", "credit_pauses", "cancels"};
	return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
	return make_uniq<N6kServeBindData>();
}

static void N6kServeStatsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto stats = n6k::CollectServeStats(DatabaseInstance::GetDatabase(context));
	output.SetValue(0, 0, Value::BIGINT(stats.connections));
	output.SetValue(1, 0, Value::BIGINT(stats.requests_handled));
	output.SetValue(2, 0, Value::BIGINT(stats.credit_pauses));
	output.SetValue(3, 0, Value::BIGINT(stats.cancels));
	state.done = true;
	output.SetCardinality(1);
}

void RegisterN6kServeStatsFunction(ExtensionLoader &loader) {
	TableFunction func(STATS_FN_NAME, {}, N6kServeStatsScan, N6kServeStatsBind, N6kServeInitGlobal);
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

void RegisterN6kServePushInvalidateFunction(ExtensionLoader &loader) {
	TableFunction func(PUSH_FN_NAME, {LogicalType::VARCHAR}, N6kPushInvalidateScan, N6kPushInvalidateBind,
	                   N6kServeInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));
}

void RegisterN6kServeFdFunction(ExtensionLoader &loader) {
	TableFunction func(FD_FN_NAME, {LogicalType::UUID}, N6kServeScan, N6kServeFdBind, N6kServeInitGlobal);
	func.varargs = LogicalType::VARCHAR;
	n6k::AddServeOptionParameters(func);
	func.named_parameters["request"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo info(std::move(func));
	loader.RegisterFunction(std::move(info));

	TableFunction create_session(CREATE_SESSION_FN_NAME, {LogicalType::BIGINT}, N6kCreateSessionScan,
	                             N6kCreateSessionBind, N6kServeInitGlobal);
	loader.RegisterFunction(create_session);
	TableFunction ready(OPEN_READY_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kResolveOpenScan,
	                    N6kOpenReadyBind, N6kServeInitGlobal);
	loader.RegisterFunction(ready);
	TableFunction failed(OPEN_FAILED_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                     N6kResolveOpenScan, N6kOpenFailedBind, N6kServeInitGlobal);
	loader.RegisterFunction(failed);
	TableFunction upstream_hello(UPSTREAM_HELLO_FN_NAME, {LogicalType::BIGINT, LogicalType::VARCHAR},
	                             N6kUpstreamHelloScan, N6kUpstreamHelloBind, N6kServeInitGlobal);
	loader.RegisterFunction(upstream_hello);
	TableFunction add_upstream(ADD_UPSTREAM_FN_NAME,
	                           {LogicalType::UUID, LogicalType::BIGINT, LogicalType::LIST(LogicalType::VARCHAR)},
	                           N6kUpstreamScan, N6kAddUpstreamBind, N6kServeInitGlobal);
	add_upstream.named_parameters["rank"] = LogicalType::BIGINT;
	loader.RegisterFunction(add_upstream);
	TableFunction set_upstream_rank(SET_UPSTREAM_RANK_FN_NAME,
	                                {LogicalType::UUID, LogicalType::BIGINT, LogicalType::BIGINT}, N6kUpstreamScan,
	                                N6kSetUpstreamRankBind, N6kServeInitGlobal);
	loader.RegisterFunction(set_upstream_rank);
	TableFunction remove_upstream(REMOVE_UPSTREAM_FN_NAME, {LogicalType::UUID, LogicalType::BIGINT}, N6kUpstreamScan,
	                              N6kRemoveUpstreamBind, N6kServeInitGlobal);
	loader.RegisterFunction(remove_upstream);
	TableFunction upstream_status(UPSTREAM_STATUS_FN_NAME, {LogicalType::UUID, LogicalType::BIGINT}, N6kUpstreamScan,
	                              N6kUpstreamStatusBind, N6kServeInitGlobal);
	loader.RegisterFunction(upstream_status);
	TableFunction send_text(SEND_TEXT_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kSendTextScan,
	                        N6kSendTextBind, N6kServeInitGlobal);
	loader.RegisterFunction(send_text);
	TableFunction close(CLOSE_FN_NAME, {LogicalType::UUID, LogicalType::INTEGER, LogicalType::VARCHAR}, N6kCloseScan,
	                    N6kCloseBind, N6kServeInitGlobal);
	loader.RegisterFunction(close);
	TableFunction session_stats(SESSION_STATS_FN_NAME, {LogicalType::UUID}, N6kSessionStatsScan, N6kSessionStatsBind,
	                            N6kServeInitGlobal);
	loader.RegisterFunction(session_stats);
	TableFunction add(ADD_CATALOG_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServedCatalogScan,
	                  N6kAddCatalogBind, N6kServeInitGlobal);
	loader.RegisterFunction(add);
	TableFunction add_lazy(ADD_LAZY_CATALOG_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServedCatalogScan,
	                       N6kAddLazyCatalogBind, N6kServeInitGlobal);
	add_lazy.named_parameters["rank"] = LogicalType::BIGINT;
	loader.RegisterFunction(add_lazy);
	TableFunction remove(REMOVE_CATALOG_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServedCatalogScan,
	                     N6kRemoveCatalogBind, N6kServeInitGlobal);
	loader.RegisterFunction(remove);
	TableFunction pause(PAUSE_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServedCatalogScan, N6kPauseBind,
	                    N6kServeInitGlobal);
	loader.RegisterFunction(pause);
	TableFunction resume(RESUME_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServedCatalogScan, N6kResumeBind,
	                     N6kServeInitGlobal);
	loader.RegisterFunction(resume);
	TableFunction inflight(INFLIGHT_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServedCatalogScan,
	                       N6kInflightBind, N6kServeInitGlobal);
	loader.RegisterFunction(inflight);
	TableFunction cancel(CANCEL_INFLIGHT_FN_NAME, {LogicalType::UUID, LogicalType::VARCHAR}, N6kServedCatalogScan,
	                     N6kCancelInflightBind, N6kServeInitGlobal);
	loader.RegisterFunction(cancel);
}

} // namespace duckdb
