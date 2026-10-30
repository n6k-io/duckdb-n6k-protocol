#include "n6k_testing_extension.hpp"

#include "arrow_ipc_decode_selftest.hpp"
#include "wire_codec_bench.hpp"
#include "fixture_functions.hpp"
#include "sql_builder_functions.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void N6kTestingLoadInternal(ExtensionLoader &loader) {
	RegisterN6kTestingSqlBuilders(loader);
	RegisterN6kTestingArrowIpcDecode(loader);
	RegisterN6kTestingWireCodecBench(loader);
	n6k::RegisterN6kTestingFixtures(loader);
}

void N6kTestingExtension::Load(ExtensionLoader &loader) {
	N6kTestingLoadInternal(loader);
}

std::string N6kTestingExtension::Name() {
	return "n6k_testing";
}

std::string N6kTestingExtension::Version() const {
#ifdef EXT_VERSION_N6K_TESTING
	return EXT_VERSION_N6K_TESTING;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(n6k_testing, loader) {
	duckdb::N6kTestingLoadInternal(loader);
}
}
