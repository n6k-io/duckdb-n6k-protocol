#include "n6k_table_function.hpp"
#include "n6k_rpc_function.hpp"
#include "n6k_version_function.hpp"
#include "n6k_capabilities_function.hpp"
#include "n6k_client_settings_function.hpp"
#include "n6k_thread_selftest.hpp"
#include "n6k_io_thread_selftest.hpp"
#include "n6k_connect_function.hpp"
#include "n6k_connection_functions.hpp"
#include "n6k_list_catalogs_function.hpp"
#include "n6k_parse_sql_get_tables.hpp"
#include "n6k_split_statements.hpp"
#include "n6k_table_permissions.hpp"
#include "n6k_storage.hpp"
#include "n6k_secret.hpp"
#include "n6k_login_function.hpp"
#include "n6k_source.hpp"
#include "n6k_ws_log.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS
#include "n6k_glue_generated.hpp"
#include "n6k_wasm_main_thread.hpp"
#endif

namespace duckdb {

void LoadInternal(ExtensionLoader &loader) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// globalThis.n6k for every later call into JS; a no-op if already installed.
	n6k::RunScriptOnMain(n6k::N6K_MAIN_GLUE_JS);
#endif
	RegisterN6kRpc(loader);
	RegisterN6kVersion(loader);
	RegisterN6kSelftestThreadParkWake(loader);
	RegisterN6kCapabilities(loader);
	RegisterN6kClientSettings(loader);
	RegisterN6kSelftestIoThreadDoorbell(loader);
	RegisterN6kConnect(loader);
	RegisterN6kConnectionFunctions(loader);
	RegisterN6kListCatalogs(loader);
	RegisterN6kParseSqlGetTables(loader);
	RegisterN6kSplitStatements(loader);
	RegisterN6kSecret(loader);
	RegisterN6kLogin(loader);
	RegisterN6kTablePermissions(loader);

	auto &db = loader.GetDatabaseInstance();
	n6k::RegisterWsLogType(db);
	RegisterCrossingPass(db);
	StorageExtension::Register(DBConfig::GetConfig(db), "n6k", CreateN6kStorageExtension());
}

} // namespace duckdb
