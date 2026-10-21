#include "n6k_schema_entry.hpp"
#include "n6k_yyjson_util.hpp"
#include "n6k_catalog.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_rpc_function.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "yyjson.hpp"

namespace duckdb {

namespace {

using duckdb_yyjson::yyjson_mut_arr;
using duckdb_yyjson::yyjson_mut_arr_append;
using duckdb_yyjson::yyjson_mut_doc;
using duckdb_yyjson::yyjson_mut_doc_free;
using duckdb_yyjson::yyjson_mut_doc_new;
using duckdb_yyjson::yyjson_mut_doc_set_root;
using duckdb_yyjson::yyjson_mut_obj;
using duckdb_yyjson::yyjson_mut_obj_add_strncpy;
using duckdb_yyjson::yyjson_mut_write;

StreamCancelCheck InterruptCheckOf(CatalogTransaction &transaction) {
	if (!transaction.HasContext()) {
		return StreamCancelCheck();
	}
	auto &context = transaction.GetContext();
	return [&context]() {
		return context.IsInterrupted();
	};
}

} // namespace

N6kSchemaEntry::N6kSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, std::shared_ptr<CatalogSession> session_p)
    : SchemaCatalogEntry(catalog, info), session(std::move(session_p)) {
}

N6kCatalog &N6kSchemaEntry::GetN6kCatalog() {
	return catalog.Cast<N6kCatalog>();
}

CrossingAttach &N6kSchemaEntry::Attach() {
	return GetN6kCatalog().Attach();
}

optional_ptr<CatalogEntry> N6kSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	auto entry_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();

	if (entry_type == CatalogType::TABLE_ENTRY || entry_type == CatalogType::VIEW_ENTRY) {
		session->PollPushEvents();
		return Attach().LookupTable(name, *this, entry_name, transaction.transaction);
	}
	if (entry_type == CatalogType::TABLE_FUNCTION_ENTRY) {
		return GetOrCreateTableFunctionEntry(transaction, entry_name);
	}
	return nullptr;
}

optional_ptr<CatalogEntry> N6kSchemaEntry::GetOrCreateTableFunctionEntry(CatalogTransaction transaction,
                                                                         const string &entry_name) {
	{
		lock_guard<mutex> lock(table_functions_lock);
		auto it = table_functions.find(entry_name);
		if (it != table_functions.end()) {
			return it->second.get();
		}
	}
	TableFunction tf;
	if (entry_name == "exec") {
		tf = MakeN6kCatalogExecFunction(session);
	} else if (entry_name == "query") {
		tf = MakeN6kCatalogQueryFunction(session);
	} else {
		tf = MakeN6kCatalogRpcFunction(entry_name, session);
	}
	auto info = CreateTableFunctionInfo(tf);
	auto entry = make_uniq<TableFunctionCatalogEntry>(catalog, *this, info);
	auto *ptr = entry.get();
	lock_guard<mutex> lock(table_functions_lock);
	table_functions[entry_name] = std::move(entry);
	return ptr;
}

void N6kSchemaEntry::Scan(ClientContext &context, CatalogType type,
                          const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		session->PollPushEvents();
		case_insensitive_set_t seen;
		Attach().ScanTables(name, *this, seen, callback, &Transaction::Get(context, catalog));
	}
	if (type == CatalogType::TABLE_FUNCTION_ENTRY) {
		lock_guard<mutex> lock(table_functions_lock);
		for (auto &kv : table_functions) {
			callback(*kv.second);
		}
	}
}

void N6kSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		case_insensitive_set_t seen;
		Attach().ScanTables(name, *this, seen, callback, nullptr);
	}
	if (type == CatalogType::TABLE_FUNCTION_ENTRY) {
		lock_guard<mutex> lock(table_functions_lock);
		for (auto &kv : table_functions) {
			callback(*kv.second);
		}
	}
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &create_info = info.Base();

	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *arr = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, arr);
	for (auto &col : create_info.columns.Logical()) {
		auto *col_obj = yyjson_mut_obj(doc);
		const auto &col_name = col.Name();
		auto type_str = col.Type().ToString();
		yyjson_mut_obj_add_strncpy(doc, col_obj, "name", col_name.c_str(), col_name.size());
		yyjson_mut_obj_add_strncpy(doc, col_obj, "type", type_str.c_str(), type_str.size());
		yyjson_mut_arr_append(arr, col_obj);
	}
	auto columns_json = n6k::SerializeJsonDocAndFree(doc);

	session->CreateTable(name, create_info.table, columns_json, InterruptCheckOf(transaction));
	Attach().Refresh(name);
	return Attach().LookupTable(name, *this, create_info.table, transaction.transaction);
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Creating functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (!transaction.HasContext()) {
		throw BinderException("Cannot create view without client context");
	}
	return Attach().CreateView(transaction.GetContext(), *transaction.transaction, *this, info);
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                       TableCatalogEntry &table) {
	throw BinderException("Creating indexes in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("Creating sequences in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                               CreateTableFunctionInfo &info) {
	throw BinderException("Creating table functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                              CreateCopyFunctionInfo &info) {
	throw BinderException("Creating copy functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                CreatePragmaFunctionInfo &info) {
	throw BinderException("Creating pragma functions in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw BinderException("Creating collations in n6k attached databases is not supported");
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Creating types in n6k attached databases is not supported");
}

void N6kSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type == CatalogType::VIEW_ENTRY) {
		CrossingDdl ddl;
		ddl.verb = CrossingVerb::DROP;
		ddl.schema = name;
		ddl.table = info.name;
		ddl.drop = &info;
		Attach().Ddl(context, Transaction::Get(context, catalog), *this, ddl);
		return;
	}
	throw BinderException("Dropping entries in n6k attached databases is not supported");
}

void N6kSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw BinderException("Only ALTER TABLE is supported on n6k attached databases");
	}
	auto &alter = info.Cast<AlterTableInfo>();

	string kind;
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	switch (alter.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add = alter.Cast<AddColumnInfo>();
		kind = "add_column";
		const auto &col_name = add.new_column.Name();
		auto type_str = add.new_column.Type().ToString();
		yyjson_mut_obj_add_strncpy(doc, root, "name", col_name.c_str(), col_name.size());
		yyjson_mut_obj_add_strncpy(doc, root, "type", type_str.c_str(), type_str.size());
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &rem = alter.Cast<RemoveColumnInfo>();
		kind = "drop_column";
		yyjson_mut_obj_add_strncpy(doc, root, "name", rem.removed_column.c_str(), rem.removed_column.size());
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &ren = alter.Cast<RenameColumnInfo>();
		kind = "rename_column";
		yyjson_mut_obj_add_strncpy(doc, root, "old_name", ren.old_name.c_str(), ren.old_name.size());
		yyjson_mut_obj_add_strncpy(doc, root, "new_name", ren.new_name.c_str(), ren.new_name.size());
		break;
	}
	default:
		yyjson_mut_doc_free(doc);
		throw BinderException(
		    "ALTER TABLE %s is not supported on n6k attached databases (only ADD COLUMN, DROP COLUMN, RENAME COLUMN)",
		    alter.ToString());
	}
	auto details_json = n6k::SerializeJsonDocAndFree(doc);

	session->AlterTable(alter.schema, alter.name, kind, details_json, InterruptCheckOf(transaction));
	Attach().Refresh(name);
}

} // namespace duckdb
