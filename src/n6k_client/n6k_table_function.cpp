#include "n6k_table_function.hpp"
#include "n6k_rpc_function.hpp"
#include "n6k_version_function.hpp"
#include "n6k_capabilities_function.hpp"
#include "n6k_thread_selftest.hpp"
#include "n6k_io_thread_selftest.hpp"
#include "ws_send_after_start_selftest.hpp"
#include "open_ns_selftest.hpp"
#include "open_pending_selftest.hpp"
#include "ws_fd_hub_selftest.hpp"
#include "n6k_connect_function.hpp"
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

namespace duckdb {

void LoadInternal(ExtensionLoader &loader) {
	RegisterN6kRpc(loader);
	RegisterN6kVersion(loader);
	RegisterN6kSelftestThreadParkWake(loader);
	RegisterN6kCapabilities(loader);
	RegisterN6kSelftestIoThreadDoorbell(loader);
	RegisterN6kSelftestSendAfterStart(loader);
	RegisterN6kSelftestOpenNsDistinct(loader);
	RegisterN6kSelftestOpenPending(loader);
	RegisterN6kSelftestFdHubRouting(loader);
	RegisterN6kConnect(loader);
	RegisterN6kListCatalogs(loader);
	RegisterN6kParseSqlGetTables(loader);
	RegisterN6kSplitStatements(loader);
	RegisterN6kSecret(loader);
	RegisterN6kLogin(loader);
	RegisterN6kTablePermissions(loader);

	auto &db = loader.GetDatabaseInstance();
	n6k::RegisterWsLogType(db);
	Crossing<N6kSource>::RegisterPass(db);
	StorageExtension::Register(DBConfig::GetConfig(db), "n6k", CreateN6kStorageExtension());
}

} // namespace duckdb
