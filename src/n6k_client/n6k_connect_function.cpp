#include "n6k_connect_function.hpp"

#include "n6k_credentials.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_served.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>

#if !defined(WASM_LOADABLE_EXTENSIONS) && !defined(_WIN32)
#define N6K_HAS_FD_TRANSPORT 1
#include "n6k_fd_hello.hpp"
#include "ws_fd_hub.hpp"

#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace duckdb {
namespace {

static constexpr const char *FN_NAME = "n6k_connect";

struct N6kConnectBindData : public TableFunctionData {
	int fd = -1;
	bool has_token = false;
	string token;
	string scope;
};

struct N6kConnectState : public GlobalTableFunctionState {
	bool done = false;
};

#ifdef N6K_HAS_FD_TRANSPORT

struct OwnedFd {
	int fd = -1;
	~OwnedFd() {
		if (fd >= 0) {
			::close(fd);
		}
	}
};

static vector<string> Handshake(ClientContext &context, int host_fd, const string &token) {
	if (n6k::FdHub::IsLive(host_fd)) {
		throw InvalidInputException("%s: wsFd %d already carries attached catalogs", FN_NAME, host_fd);
	}
	OwnedFd owned;
	owned.fd = ::dup(host_fd);
	if (owned.fd < 0) {
		throw IOException("%s: dup(%d) failed: %s", FN_NAME, host_fd, std::strerror(errno));
	}
#ifdef SO_NOSIGPIPE
	int on = 1;
	::setsockopt(owned.fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
	try {
		auto capabilities = n6k::HelloOnFd(context, owned.fd, token, FN_NAME);
		n6k::FdHub::SetHelloCapabilities(host_fd, capabilities);
		return capabilities;
	} catch (...) {
		::shutdown(owned.fd, SHUT_RDWR);
		throw;
	}
}

#endif

unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"connected", "capabilities"};
	return_types = {LogicalType::BOOLEAN, LogicalType::LIST(LogicalType::VARCHAR)};
	n6k::ThrowIfServed(context, FN_NAME);
#ifndef N6K_HAS_FD_TRANSPORT
	throw NotImplementedException("%s: wsFd is only supported in the native, non-Windows build", FN_NAME);
#else
	auto result = make_uniq<N6kConnectBindData>();
	auto fd_it = input.named_parameters.find("wsFd");
	if (fd_it == input.named_parameters.end() || fd_it->second.IsNull()) {
		throw BinderException("%s requires wsFd: SELECT * FROM %s(wsFd => 7, token => '...')", FN_NAME, FN_NAME);
	}
	const auto fd = fd_it->second.GetValue<int64_t>();
	if (fd < 0 || fd > static_cast<int64_t>(NumericLimits<int32_t>::Maximum())) {
		throw BinderException("%s: %s is not a valid file descriptor", FN_NAME, std::to_string(fd));
	}
	result->fd = static_cast<int>(fd);
	auto token_it = input.named_parameters.find("token");
	if (token_it != input.named_parameters.end()) {
		if (token_it->second.IsNull()) {
			throw BinderException("%s: token must not be NULL", FN_NAME);
		}
		result->has_token = true;
		result->token = token_it->second.GetValue<string>();
	}
	auto scope_it = input.named_parameters.find("scope");
	if (scope_it != input.named_parameters.end()) {
		if (scope_it->second.IsNull()) {
			throw BinderException("%s: scope must not be NULL", FN_NAME);
		}
		result->scope = scope_it->second.GetValue<string>();
	}
	if (!result->has_token && result->scope.empty()) {
		throw BinderException("%s requires token or scope: SELECT * FROM %s(wsFd => 7, scope => 'host:port')", FN_NAME,
		                      FN_NAME);
	}
	return std::move(result);
#endif
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kConnectState>();
}

void Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kConnectState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
	vector<Value> capabilities;
#ifdef N6K_HAS_FD_TRANSPORT
	auto &bind_data = data_p.bind_data->Cast<N6kConnectBindData>();
	for (auto &cap :
	     Handshake(context, bind_data.fd,
	               bind_data.has_token ? bind_data.token : ResolveAttachToken(context, bind_data.scope, ""))) {
		capabilities.emplace_back(cap);
	}
#else
	(void)context;
#endif
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value::LIST(LogicalType::VARCHAR, std::move(capabilities)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kConnect(ExtensionLoader &loader) {
	TableFunction fn(FN_NAME, {}, Scan, Bind, InitGlobal);
	fn.named_parameters["wsFd"] = LogicalType::BIGINT;
	fn.named_parameters["token"] = LogicalType::VARCHAR;
	fn.named_parameters["scope"] = LogicalType::VARCHAR;
	loader.RegisterFunction(fn);
}

} // namespace duckdb
