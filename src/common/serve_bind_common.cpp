#include "serve_bind_common.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {
namespace n6k {

// System and temporary catalogs are never served. The startup in-memory database is excluded too, when
// anything else is attached. A connection
// opened with no path carries one it never asked for — `duckdb.connect()` names it `memory` — and
// serving it would hand every client an empty, writable catalog the host never meant to expose.
//
// Only the *in-memory* startup database is dropped. `duckdb mydata.duckdb` asked for `mydata`
// explicitly, so it stays even once something else is attached; and a connection whose only catalog
// is the in-memory default still serves it, since that is plainly what was meant.
static vector<std::string> AttachedUserCatalogs(ClientContext &context) {
	vector<std::string> names;
	vector<std::string> requested;
	for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
		if (db->IsSystem() || db->IsTemporary()) {
			continue;
		}
		names.push_back(db->GetName());
		if (!db->IsInitialDatabase() || !db->GetCatalog().InMemory()) {
			requested.push_back(db->GetName());
		}
	}
	return requested.empty() ? names : requested;
}

vector<std::string> ResolveServedCatalogs(ClientContext &context, const vector<Value> &inputs, idx_t first_catalog,
                                          const char *fn_name) {
	if (inputs.size() == first_catalog + 1 && !inputs[first_catalog].IsNull() &&
	    inputs[first_catalog].GetValue<std::string>() == ALL_CATALOGS) {
		auto catalogs = AttachedUserCatalogs(context);
		if (catalogs.empty()) {
			throw BinderException("%s: '%s' found no attached catalogs to serve; ATTACH one first", fn_name,
			                      ALL_CATALOGS);
		}
		return catalogs;
	}

	vector<std::string> catalogs;
	case_insensitive_set_t seen;
	for (idx_t i = first_catalog; i < inputs.size(); i++) {
		auto &arg = inputs[i];
		if (arg.IsNull()) {
			throw BinderException("%s requires a catalog argument: CALL %s('mydata')", fn_name, fn_name);
		}
		auto catalog_name = arg.GetValue<std::string>();
		if (catalog_name == ALL_CATALOGS) {
			throw BinderException("%s: '%s' serves every attached catalog and cannot be combined with names", fn_name,
			                      ALL_CATALOGS);
		}
		try {
			(void)Catalog::GetCatalog(context, catalog_name);
		} catch (const std::exception &) {
			throw BinderException("%s: catalog \"%s\" is not attached; ATTACH it before serving", fn_name,
			                      catalog_name);
		}
		// Two sessions must never resolve to the same catalog by accident.
		if (!seen.insert(catalog_name).second) {
			throw BinderException("%s: catalog \"%s\" listed more than once", fn_name, catalog_name);
		}
		catalogs.push_back(std::move(catalog_name));
	}
	return catalogs;
}

vector<std::pair<std::string, std::string>>
ResolvePendingCatalogs(const Value &pending, const vector<std::string> &served, const char *fn_name) {
	vector<std::pair<std::string, std::string>> out;
	if (pending.IsNull()) {
		return out;
	}
	case_insensitive_set_t seen(served.begin(), served.end());
	for (auto &entry : MapValue::GetChildren(pending)) {
		auto &kv = StructValue::GetChildren(entry);
		if (kv[0].IsNull()) {
			throw BinderException("%s: pending catalog names must not be NULL", fn_name);
		}
		auto catalog_name = kv[0].GetValue<std::string>();
		if (!seen.insert(catalog_name).second) {
			throw BinderException("%s: catalog \"%s\" listed more than once", fn_name, catalog_name);
		}
		out.emplace_back(std::move(catalog_name), kv[1].IsNull() ? std::string() : kv[1].GetValue<std::string>());
	}
	return out;
}

} // namespace n6k
} // namespace duckdb
