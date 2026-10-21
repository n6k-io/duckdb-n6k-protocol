#include "n6k_server_extension.hpp"
#include "insert_scan.hpp"
#include "n6k_exception_types.hpp"
#include "n6k_serve_function.hpp"
#ifdef N6K_SERVER_WITH_WS
#include "n6k_serve_http_function.hpp"
#endif

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void N6kServerLoadInternal(ExtensionLoader &loader) {
	RegisterN6kServeFunction(loader);
	RegisterN6kServeFdFunction(loader);
	RegisterN6kHandshakeFunctions(loader);
	RegisterN6kServePushInvalidateFunction(loader);
	RegisterN6kServeStatsFunction(loader);
#ifdef N6K_SERVER_WITH_WS
	RegisterN6kServeHttpFunction(loader);
#endif
	n6k::RegisterInsertScanFunction(loader);
	// The reactor maps its errors through this table (serve_reactor.cpp); registered here so a
	// server-only process can read it too.
	n6k::RegisterExceptionTypesFunction(loader);
}

void N6kServerExtension::Load(ExtensionLoader &loader) {
	N6kServerLoadInternal(loader);
}

std::string N6kServerExtension::Name() {
	return "n6k_server";
}

std::string N6kServerExtension::Version() const {
#ifdef EXT_VERSION_N6K_SERVER
	return EXT_VERSION_N6K_SERVER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(n6k_server, loader) {
	duckdb::N6kServerLoadInternal(loader);
}
}
