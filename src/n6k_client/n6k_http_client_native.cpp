#ifndef WASM_LOADABLE_EXTENSIONS

#include "n6k_err_throw.hpp"
#include "n6k_http_client.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class NativeHttpClient : public N6kHttpClient {
public:
	string Get(ClientContext &context, const string &url) override {
		auto &db = DatabaseInstance::GetDatabase(context);
		auto &http_util = HTTPUtil::Get(db);
		auto params = http_util.InitializeParameters(context, url);

		string response_body;
		GetRequestInfo request(
		    url, HTTPHeaders(db), *params, [](const HTTPResponse &) -> bool { return true; },
		    [&response_body](const_data_ptr_t data, idx_t data_length) -> bool {
			    response_body.append(reinterpret_cast<const char *>(data), data_length);
			    return true;
		    });
		// No auth header: this GET targets a public OAuth discovery document.

		auto client = http_util.InitializeClient(*params, request.proto_host_port);
		auto response = http_util.SendRequest(request, client);
		if (!response->Success()) {
			n6k::ThrowN6kHttpError("GET", url, static_cast<int>(response->status), response_body, response->GetError());
		}
		return response_body;
	}

	string PostForm(ClientContext &context, const string &url, const string &form_body) override {
		auto &db = DatabaseInstance::GetDatabase(context);
		auto &http_util = HTTPUtil::Get(db);
		auto params = http_util.InitializeParameters(context, url);

		PostRequestInfo request(url, HTTPHeaders(db), *params, const_data_ptr_cast(form_body.data()), form_body.size());
		// OAuth token endpoint: no n6k bearer; the credential is the subject_token in the body.
		request.headers.Insert("Content-Type", "application/x-www-form-urlencoded");
		request.headers.Insert("Accept", "application/json");

		auto client = http_util.InitializeClient(*params, request.proto_host_port);
		auto response = http_util.SendRequest(request, client);
		if (!response->Success()) {
			n6k::ThrowN6kHttpError("POST", url, static_cast<int>(response->status), request.buffer_out,
			                       response->GetError());
		}
		return request.buffer_out;
	}

	N6kFormResponse PostFormStatus(ClientContext &context, const string &url, const string &form_body) override {
		auto &db = DatabaseInstance::GetDatabase(context);
		auto &http_util = HTTPUtil::Get(db);
		auto params = http_util.InitializeParameters(context, url);

		PostRequestInfo request(url, HTTPHeaders(db), *params, const_data_ptr_cast(form_body.data()), form_body.size());
		request.headers.Insert("Content-Type", "application/x-www-form-urlencoded");
		request.headers.Insert("Accept", "application/json");

		auto client = http_util.InitializeClient(*params, request.proto_host_port);
		auto response = http_util.SendRequest(request, client);
		// Only a transport failure is fatal; an HTTP error status is handed back for the caller to interpret.
		if (response->HasRequestError()) {
			throw IOException("n6k HTTP POST %s failed: %s", url, response->GetRequestError());
		}
		string body = request.buffer_out.empty() ? response->body : request.buffer_out;
		return {static_cast<int>(response->status), body};
	}
};

N6kHttpClient &N6kHttpClient::Get() {
	static NativeHttpClient instance;
	return instance;
}

} // namespace duckdb

#endif // !WASM_LOADABLE_EXTENSIONS
