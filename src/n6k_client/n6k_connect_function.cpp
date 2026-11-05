#include "n6k_connect_function.hpp"

#include "n6k_credentials.hpp"
#include "n6k_endpoint.hpp"
#include "n6k_served.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>

#ifndef WASM_LOADABLE_EXTENSIONS
#include "connection_hub.hpp"
#include "n6k_host_callback.hpp"

#ifndef _WIN32
#include "ws_transport_raw.hpp"

#include <sys/socket.h>
#include <sys/stat.h>
#endif
#endif

namespace duckdb {
namespace {

static constexpr const char *FN_NAME = "n6k_connect";

struct N6kConnectBindData : public TableFunctionData {
	// http(s):// base URL, as ATTACH builds it.
	string url;
	string query;
	int fd = -1;
	bool has_token = false;
	string token;
	string scope;
	string on_text;
	string on_close;
};

struct N6kConnectState : public GlobalTableFunctionState {
	bool done = false;
};

static const Value *FindNamed(TableFunctionBindInput &input, const char *name) {
	auto it = input.named_parameters.find(name);
	return it == input.named_parameters.end() ? nullptr : &it->second;
}

static string BindString(TableFunctionBindInput &input, const char *name) {
	auto value = FindNamed(input, name);
	if (!value) {
		return {};
	}
	if (value->IsNull()) {
		throw BinderException("%s: %s must not be NULL", FN_NAME, name);
	}
	return value->GetValue<string>();
}

static string BindUdf(TableFunctionBindInput &input, const char *name) {
	auto udf = BindString(input, name);
	if (FindNamed(input, name) && udf.empty()) {
		throw BinderException("%s: %s must name a scalar function", FN_NAME, name);
	}
	return udf;
}

// n6k(s)://host[:port][/prefix][?query] -> http(s)://host[:port][/prefix], the base URL ATTACH builds, and
// "?query", which the dial appends after /ws.
static void SplitN6kUrl(const string &url, string &base_url, string &query) {
	const bool secure = StringUtil::StartsWith(url, "n6ks://");
	if (!secure && !StringUtil::StartsWith(url, "n6k://")) {
		throw BinderException("%s: url must be n6k://host[:port][/prefix][?query] or n6ks://…, got '%s'", FN_NAME, url);
	}
	auto rest = url.substr(secure ? 7 : 6);
	auto question = rest.find('?');
	if (question != string::npos) {
		query = rest.substr(question);
		rest = rest.substr(0, question);
	}
	if (rest.empty() || rest[0] == '/') {
		throw BinderException("%s: url '%s' names no host", FN_NAME, url);
	}
	if (StringUtil::EndsWith(rest, "/")) {
		rest = rest.substr(0, rest.size() - 1);
	}
	base_url = (secure ? "https://" : "http://") + rest;
}

unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                              vector<Identifier> &names) {
	names = {"connection_id", "capabilities"};
	return_types = {LogicalType::UUID, LogicalType::LIST(LogicalType::VARCHAR)};
	n6k::ThrowIfServed(context, FN_NAME);
#ifdef WASM_LOADABLE_EXTENSIONS
	throw NotImplementedException("%s is only supported in the native build", FN_NAME);
#else
	auto result = make_uniq<N6kConnectBindData>();
	result->url = BindString(input, "url");
	auto fd_value = FindNamed(input, "wsFd");
	if (fd_value && !fd_value->IsNull()) {
		const auto fd = fd_value->GetValue<int64_t>();
		if (fd < 0 || fd > static_cast<int64_t>(NumericLimits<int32_t>::Maximum())) {
			throw BinderException("%s: %s is not a valid file descriptor", FN_NAME, std::to_string(fd));
		}
		result->fd = static_cast<int>(fd);
	}
	if (result->url.empty() == (result->fd < 0)) {
		throw BinderException("%s requires exactly one of url or wsFd: SELECT * FROM %s(url => 'n6k://host:port') "
		                      "or %s(wsFd => 7, token => '...')",
		                      FN_NAME, FN_NAME, FN_NAME);
	}
#ifdef _WIN32
	if (result->fd >= 0) {
		throw NotImplementedException("%s: wsFd is not supported on Windows", FN_NAME);
	}
#endif
	if (FindNamed(input, "token")) {
		result->has_token = true;
		result->token = BindString(input, "token");
	}
	result->scope = BindString(input, "scope");
	if (!result->url.empty()) {
		if (!result->scope.empty()) {
			throw BinderException("%s: scope only applies to wsFd; with url, secrets are matched against the url",
			                      FN_NAME);
		}
		const auto url = result->url;
		SplitN6kUrl(url, result->url, result->query);
		n6k::ParseN6kEndpoint(result->url);
	} else if (!result->has_token && result->scope.empty()) {
		throw BinderException("%s requires token or scope: SELECT * FROM %s(wsFd => 7, scope => 'host:port')", FN_NAME,
		                      FN_NAME);
	}
	result->on_text = BindUdf(input, "on_text");
	result->on_close = BindUdf(input, "on_close");
	return std::move(result);
#endif
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kConnectState>();
}

#ifndef WASM_LOADABLE_EXTENSIONS

static std::shared_ptr<n6k::ConnectionHub> Connect(ClientContext &context, const N6kConnectBindData &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	// Fail on a misnamed UDF here, not silently on the first message.
	for (auto *udf : {&bind_data.on_text, &bind_data.on_close}) {
		if (!udf->empty()) {
			n6k::HostCallback check(db, *udf, 2);
		}
	}
	n6k::ConnectionHub::Options opts;
	string key;
	if (!bind_data.url.empty()) {
		const auto token = bind_data.has_token ? bind_data.token : ResolveAttachToken(context, bind_data.url, "");
		const auto ws_url = n6k::WsUrlFromBaseUrl(bind_data.url, bind_data.query);
		key = n6k::WebSocketKey(ws_url, token);
		opts = n6k::WebSocketOptions(ws_url, token);
#ifndef WASM_LOADABLE_EXTENSIONS
		if (!bind_data.has_token && !token.empty()) {
			opts.resolve_token = RedialTokenResolver(db, bind_data.url);
		}
#endif
	} else {
#ifndef _WIN32
		struct stat st;
		if (::fstat(bind_data.fd, &st) != 0 || !S_ISSOCK(st.st_mode)) {
			throw InvalidInputException("%s: wsFd %d is not an open socket", FN_NAME, bind_data.fd);
		}
		// The socket itself, not its fd number: two fds for one socket must not get two readers.
		key = "fd\n" + std::to_string(st.st_dev) + ":" + std::to_string(st.st_ino);
		const int fd = bind_data.fd;
		opts.dial = [fd]() {
			return std::unique_ptr<n6k::Transport>(new n6k::RawSocketTransport(fd));
		};
		opts.token = bind_data.has_token ? bind_data.token : ResolveAttachToken(context, bind_data.scope, "");
		// The socket's owner relays JSON-RPC to an app of its own, which answers only after an offer.
		opts.offer_app_capabilities = true;
		opts.endpoint = "wsFd " + std::to_string(fd);
#endif
	}
	const bool wants_callbacks = !bind_data.on_text.empty() || !bind_data.on_close.empty();
	if (wants_callbacks) {
		opts.offer_app_capabilities = true;
	}
	opts.on_text = bind_data.on_text;
	opts.on_close = bind_data.on_close;
	opts.logger = db.GetLogManager().GlobalLoggerReference();

	auto registry = n6k::ConnectionRegistry::Get(db);
	auto acquired =
	    registry->Acquire(key, [&]() { return n6k::ConnectionHub::Create(db.shared_from_this(), std::move(opts)); });
	auto &hub = acquired.hub;
	if (!acquired.created && wants_callbacks) {
		registry->Release(hub->Id(), "n6k_connect refused");
		throw InvalidInputException("%s: connection %s is already open to that endpoint; on_text / on_close can only "
		                            "be given by the n6k_connect that opens it",
		                            FN_NAME, hub->IdString());
	}
	try {
		hub->EnsureConnected([&context] { return context.IsInterrupted(); });
	} catch (...) {
		registry->Release(hub->Id(), "connect failed");
#ifndef _WIN32
		// The host still holds its fd, so only this tells a pump on the other end that the handshake failed.
		if (acquired.created && bind_data.fd >= 0) {
			::shutdown(bind_data.fd, SHUT_RDWR);
		}
#endif
		throw;
	}
	registry->AddConnectLease(hub->Id());
	return hub;
}

#endif

void Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kConnectState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;
#ifndef WASM_LOADABLE_EXTENSIONS
	auto hub = Connect(context, data_p.bind_data->Cast<N6kConnectBindData>());
	vector<Value> capabilities;
	for (auto &cap : hub->HelloCapabilities()) {
		capabilities.emplace_back(cap);
	}
	output.data[0].SetValue(0, Value::UUID(hub->Id()));
	output.data[1].SetValue(0, Value::LIST(LogicalType::VARCHAR, std::move(capabilities)));
	output.SetChildCardinality(1);
#else
	(void)context;
	output.SetChildCardinality(0);
#endif
}

} // namespace

void RegisterN6kConnect(ExtensionLoader &loader) {
	TableFunction fn(FN_NAME, {}, Scan, Bind, InitGlobal);
	fn.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("url", LogicalType::VARCHAR);
		options.Add("wsFd", LogicalType::BIGINT);
		options.Add("token", LogicalType::VARCHAR);
		options.Add("scope", LogicalType::VARCHAR);
		options.Add("on_text", LogicalType::VARCHAR);
		options.Add("on_close", LogicalType::VARCHAR);
	});
	loader.RegisterFunction(fn);
}

} // namespace duckdb
