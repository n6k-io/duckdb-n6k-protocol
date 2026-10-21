#ifdef WASM_LOADABLE_EXTENSIONS

#include "n6k_http_client.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

// The browser build makes no HTTP requests. Everything the extension needs travels over the
// WebSocket session (`CatalogSession`); the OAuth entry points below exist only because
// the interface is shared with the native build, where they are reachable.
class WasmHttpClient : public N6kHttpClient {
public:
	string Get(ClientContext &, const string &url) override {
		throw NotImplementedException("n6k: HTTP GET (OAuth discovery) is not available in the browser");
	}

	string PostForm(ClientContext &, const string &url, const string &) override {
		throw NotImplementedException("n6k: OAuth token exchange is not available in the browser");
	}

	N6kFormResponse PostFormStatus(ClientContext &, const string &url, const string &) override {
		throw NotImplementedException("n6k: OAuth device flow is not available in the browser");
	}
};

N6kHttpClient &N6kHttpClient::Get() {
	static WasmHttpClient instance;
	return instance;
}

} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
