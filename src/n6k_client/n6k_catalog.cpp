#include "n6k_catalog.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_schema_entry.hpp"
#include "n6k_insert.hpp"
#include "ws_json.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

N6kCatalog::N6kCatalog(AttachedDatabase &db, string base_url_p, std::shared_ptr<CatalogSession> session_p)
    : Catalog(db), base_url(std::move(base_url_p)), session(std::move(session_p)),
      attach(db, Crossing<N6kSource>::Adapt(make_uniq<N6kSource>(db.GetDatabase(), session, db.GetName()))) {
	session->SetPushHandler([this](uint8_t op, const std::string &body) {
		if (op == n6k::OP_CATALOG_INVALIDATED) {
			HandlePushInvalidate(body);
		}
	});
}

N6kCatalog::~N6kCatalog() {
	session->SetPushHandler(nullptr);
	session->Detach();
}

N6kSource &N6kCatalog::Source() {
	return attach.Source<N6kSource>();
}

void N6kCatalog::OnDetach(ClientContext &context) {
	attach.Detach(context);
}

void N6kCatalog::HandlePushInvalidate(const std::string &body) {
	n6k::JsonDoc doc(body.data(), body.size());
	if (!doc.Parsed()) {
		return;
	}
	auto schema_names = n6k::JsonGetStrArray(doc.Root(), "schemas");
	if (schema_names.empty()) {
		return;
	}
	InvalidateSchemas(schema_names);
}

void N6kCatalog::InvalidateSchemas(const std::vector<std::string> &schema_names) {
	for (auto &name : schema_names) {
		attach.Refresh(name);
	}
}

void N6kCatalog::Initialize(bool load_builtin) {
}

string N6kCatalog::GetCatalogType() {
	return "n6k";
}

N6kSchemaEntry &N6kCatalog::GetOrCreateSchema(CatalogTransaction transaction, const string &schema_name) {
	lock_guard<mutex> lock(schemas_lock);
	auto it = schemas.find(schema_name);
	if (it != schemas.end()) {
		return *it->second;
	}

	CreateSchemaInfo info;
	info.schema = schema_name;
	auto schema_entry = make_uniq<N6kSchemaEntry>(*this, info, session);
	auto &result = *schema_entry;
	schemas[schema_name] = std::move(schema_entry);
	return result;
}

void N6kCatalog::LoadSchemasOnce() {
	lock_guard<mutex> load_lock(schemas_load_lock);
	if (schemas_loaded_) {
		return;
	}
	auto schema_names = attach.Schemas();
	auto transaction = CatalogTransaction::GetSystemTransaction(GetAttached().GetDatabase());
	for (auto &name : schema_names) {
		GetOrCreateSchema(transaction, name);
	}
	GetOrCreateSchema(transaction, DEFAULT_SCHEMA);
	schemas_loaded_ = true;
}

optional_ptr<CatalogEntry> N6kCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	lock_guard<mutex> lock(schemas_lock);
	auto it = schemas.find(info.schema);
	if (it != schemas.end()) {
		if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return it->second.get();
		}
		throw CatalogException("Schema '%s' already exists", info.schema);
	}
	auto schema_entry = make_uniq<N6kSchemaEntry>(*this, info, session);
	auto result = schema_entry.get();
	schemas[info.schema] = std::move(schema_entry);
	return result;
}

optional_ptr<SchemaCatalogEntry> N6kCatalog::LookupSchema(CatalogTransaction transaction,
                                                          const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	if (!StringUtil::CIEquals(schema_name, DEFAULT_SCHEMA)) {
		LoadSchemasOnce();
	}
	lock_guard<mutex> lock(schemas_lock);
	auto it = schemas.find(schema_name);
	if (it == schemas.end()) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException("Schema '%s' not found", schema_name);
	}
	return it->second.get();
}

void N6kCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	LoadSchemasOnce();
	lock_guard<mutex> lock(schemas_lock);
	for (auto &kv : schemas) {
		callback(*kv.second);
	}
}

void N6kCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("Dropping schemas in n6k attached databases is not supported");
}

PhysicalOperator &N6kCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &schema = op.schema.Cast<N6kSchemaEntry>();
	auto &insert = planner.Make<N6kInsert>(schema, std::move(op.info), vector<LogicalType> {LogicalType::BIGINT},
	                                       op.estimated_cardinality);
	insert.children.push_back(plan);
	return insert;
}

PhysicalOperator &N6kCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	throw InternalException("n6k: an insert into '%s' bypassed crossing", op.table.name);
}

PhysicalOperator &N6kCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	throw InternalException("n6k: a delete from '%s' bypassed crossing", op.table.name);
}

PhysicalOperator &N6kCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	throw InternalException("n6k: an update of '%s' bypassed crossing", op.table.name);
}

DatabaseSize N6kCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

bool N6kCatalog::InMemory() {
	return false;
}

string N6kCatalog::GetDBPath() {
	return base_url;
}

} // namespace duckdb
