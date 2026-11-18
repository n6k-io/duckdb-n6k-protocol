#include "n6k_schema_entry.hpp"
#include "n6k_catalog.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_rpc_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

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
		session->MarkDisconnectedIfConnectionReplaced();
		return Attach().LookupTable(name.GetIdentifierName(), *this, entry_name, transaction.transaction);
	}
	if (entry_type == CatalogType::TABLE_FUNCTION_ENTRY) {
		if (entry_name == "exec" || entry_name == "query" || entry_name == "rpc" ||
		    !GetN6kCatalog().Source().ListsTableFunctions()) {
			return GetOrCreateTableFunctionEntry(transaction, entry_name);
		}
		auto entries = SyncListedFunctions(&entry_name);
		return entries.empty() ? nullptr : &entries[0].get();
	}
	return nullptr;
}

vector<reference<CatalogEntry>> N6kSchemaEntry::SyncListedFunctions(const string *only) {
	auto rows = GetN6kCatalog().Source().TableFunctions(name.GetIdentifierName());
	case_insensitive_set_t listed;
	for (auto &row : rows) {
		listed.insert(row.name);
	}
	vector<reference<CatalogEntry>> entries;
	lock_guard<mutex> lock(table_functions_lock);
	for (auto it = listed_functions.begin(); it != listed_functions.end();) {
		if (listed.count(it->first)) {
			++it;
			continue;
		}
		retired.push_back(std::move(it->second.entry));
		it = listed_functions.erase(it);
	}
	for (auto &row : rows) {
		if (only && !StringUtil::CIEquals(row.name, *only)) {
			continue;
		}
		auto existing = listed_functions.find(row.name);
		if (existing != listed_functions.end() && existing->second.row == row) {
			entries.push_back(*existing->second.entry);
			continue;
		}
		CreateTableFunctionInfo info(MakeN6kListedFunctionSet(catalog.GetDatabase(), row, session));
		info.internal = false;
		if (!row.description.empty() || !row.examples.empty()) {
			FunctionDescription description;
			description.description = row.description;
			description.examples = row.examples;
			info.descriptions.push_back(std::move(description));
		}
		auto entry = make_uniq<TableFunctionCatalogEntry>(catalog, *this, info);
		if (row.has_comment) {
			entry->comment = Value(row.comment);
		}
		entries.push_back(*entry);
		if (existing != listed_functions.end()) {
			retired.push_back(std::move(existing->second.entry));
			existing->second = ListedFunction {row, std::move(entry)};
		} else {
			listed_functions.emplace(row.name, ListedFunction {row, std::move(entry)});
		}
	}
	return entries;
}

void N6kSchemaEntry::ScanTableFunctions(const std::function<void(CatalogEntry &)> &callback) {
	if (GetN6kCatalog().Source().ListsTableFunctions()) {
		for (auto &entry : SyncListedFunctions(nullptr)) {
			callback(entry.get());
		}
		return;
	}
	lock_guard<mutex> lock(table_functions_lock);
	for (auto &kv : table_functions) {
		callback(*kv.second);
	}
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
		session->MarkDisconnectedIfConnectionReplaced();
		case_insensitive_set_t seen;
		Attach().ScanTables(name.GetIdentifierName(), *this, seen, callback, &Transaction::Get(context, catalog));
	}
	if (type == CatalogType::TABLE_FUNCTION_ENTRY) {
		ScanTableFunctions(callback);
	}
}

void N6kSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type == CatalogType::TABLE_ENTRY) {
		case_insensitive_set_t seen;
		Attach().ScanTables(name.GetIdentifierName(), *this, seen, callback, nullptr);
	}
	if (type == CatalogType::TABLE_FUNCTION_ENTRY) {
		ScanTableFunctions(callback);
	}
}

optional_ptr<CatalogEntry> N6kSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	if (!transaction.HasContext()) {
		throw BinderException("Cannot create table without client context");
	}
	CrossingDdl ddl;
	ddl.verb = CrossingVerb::CREATE;
	ddl.schema = name.GetIdentifierName();
	ddl.table = info.Base().GetTableName().GetIdentifierName();
	ddl.create = info.base.get();
	Attach().Ddl(transaction.GetContext(), *transaction.transaction, *this, ddl);
	return Attach().LookupTable(ddl.schema, *this, ddl.table, transaction.transaction);
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
	if (info.type == CatalogType::VIEW_ENTRY || info.type == CatalogType::TABLE_ENTRY) {
		CrossingDdl ddl;
		ddl.verb = CrossingVerb::DROP;
		ddl.schema = name.GetIdentifierName();
		ddl.table = info.GetQualifiedName().Name().GetIdentifierName();
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
	if (!transaction.HasContext()) {
		throw BinderException("Cannot alter a table without client context");
	}
	auto &schema_name = name.GetIdentifierName();
	auto &entry_name = info.GetQualifiedName().Name();
	if (!Attach().ServesTable(schema_name, entry_name.GetIdentifierName(), transaction.transaction)) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException::MissingEntry(info.GetCatalogType(), entry_name, string());
	}
	CrossingDdl ddl;
	ddl.verb = CrossingVerb::ALTER;
	ddl.schema = schema_name;
	ddl.table = entry_name.GetIdentifierName();
	ddl.alter = &info;
	Attach().Ddl(transaction.GetContext(), *transaction.transaction, *this, ddl);
}

} // namespace duckdb
