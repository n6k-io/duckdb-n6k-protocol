#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ClientContext;

// Form POST result that does not throw on HTTP error status; RFC 8628 polling reads the 400 body.
struct N6kFormResponse {
	int status;
	string body;
};

// Native-only: the WASM implementation throws from every method.
class N6kHttpClient {
public:
	virtual ~N6kHttpClient() = default;

	// Plain GET as text, no auth header (OAuth discovery).
	virtual string Get(ClientContext &context, const string &url) = 0;
	// Form-encoded POST as text (OAuth token endpoint).
	virtual string PostForm(ClientContext &context, const string &url, const string &form_body) = 0;
	// Like PostForm but returns status+body instead of throwing (RFC 8628 poll errors).
	virtual N6kFormResponse PostFormStatus(ClientContext &context, const string &url, const string &form_body) = 0;

	static N6kHttpClient &Get();
};

} // namespace duckdb
