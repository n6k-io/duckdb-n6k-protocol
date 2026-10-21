#define DUCKDB_EXTENSION_MAIN

#include "n6k_client_extension.hpp"
#include "n6k_table_function.hpp"
#include "duckdb.hpp"

namespace duckdb {

void N6kClientExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string N6kClientExtension::Name() {
	return "n6k_client";
}

std::string N6kClientExtension::Version() const {
#ifdef EXT_VERSION_N6K_CLIENT
	return EXT_VERSION_N6K_CLIENT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(n6k_client, loader) {
	duckdb::LoadInternal(loader);
}
}
