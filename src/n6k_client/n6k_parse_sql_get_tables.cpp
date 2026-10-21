#include "n6k_parse_sql_get_tables.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <algorithm>
#include <tuple>
#include <unordered_set>

namespace duckdb {

struct TableRefRow {
	string catalog;
	string schema;
	string table;
	string ref_type;
	string privilege;

	bool operator<(const TableRefRow &other) const {
		return std::tie(catalog, schema, table, ref_type, privilege) <
		       std::tie(other.catalog, other.schema, other.table, other.ref_type, other.privilege);
	}
	bool operator==(const TableRefRow &other) const {
		return catalog == other.catalog && schema == other.schema && table == other.table &&
		       ref_type == other.ref_type && privilege == other.privilege;
	}
};

// [catalog.][schema.]name, passed through except the 2-part a.foo: a=catalog if attached, else
// schema in default catalog, else unresolved wildcard
static TableRefRow ResolveRef(ClientContext &context, const string &catalog, const string &schema, const string &table,
                              const string &ref_type, const string &privilege) {
	if (!catalog.empty()) {
		return {catalog, schema, table, ref_type, privilege};
	}
	if (schema.empty()) {
		return {"", "", table, ref_type, privilege};
	}
	try {
		auto &db_mgr = DatabaseManager::Get(context);
		if (db_mgr.GetDatabase(context, schema)) {
			return {schema, "", table, ref_type, privilege};
		}
		auto default_cat = DatabaseManager::GetDefaultDatabase(context);
		if (!default_cat.empty()) {
			auto sch = Catalog::GetSchema(context, default_cat, schema, OnEntryNotFound::RETURN_NULL);
			if (sch) {
				return {default_cat, schema, table, ref_type, privilege};
			}
		}
	} catch (...) {
		// lookups can fail (bad txn state); fall through to unresolved rather than failing the parse
	}
	return {"", schema, table, ref_type, privilege};
}

static void CollectFromTableRef(ClientContext &context, const TableRef &ref,
                                const std::unordered_set<string> &cte_aliases, const string &privilege,
                                vector<TableRefRow> &out);
static void CollectFromQueryNode(ClientContext &context, const QueryNode &node, std::unordered_set<string> cte_aliases,
                                 vector<TableRefRow> &out);
static void CollectFromStatement(ClientContext &context, const SQLStatement &stmt, vector<TableRefRow> &out);

// privilege = access the enclosing statement grants; subquery bodies always recurse as "select"
static void CollectFromTableRef(ClientContext &context, const TableRef &ref,
                                const std::unordered_set<string> &cte_aliases, const string &privilege,
                                vector<TableRefRow> &out) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		if (base.table_name.empty()) {
			break;
		}
		if (base.catalog_name.empty() && base.schema_name.empty() && cte_aliases.count(base.table_name) > 0) {
			break;
		}
		out.push_back(
		    ResolveRef(context, base.catalog_name, base.schema_name, base.table_name, "base_table", privilege));
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.left) {
			CollectFromTableRef(context, *join.left, cte_aliases, privilege, out);
		}
		if (join.right) {
			CollectFromTableRef(context, *join.right, cte_aliases, privilege, out);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			CollectFromQueryNode(context, *subquery.subquery->node, cte_aliases, out);
		}
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf = ref.Cast<TableFunctionRef>();
		if (tf.function && tf.function->GetExpressionType() == ExpressionType::FUNCTION) {
			auto &fexpr = tf.function->Cast<FunctionExpression>();
			if (!fexpr.function_name.empty()) {
				out.push_back(
				    ResolveRef(context, fexpr.catalog, fexpr.schema, fexpr.function_name, "table_function", privilege));
			}
		}
		if (tf.subquery && tf.subquery->node) {
			CollectFromQueryNode(context, *tf.subquery->node, cte_aliases, out);
		}
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &pivot = ref.Cast<PivotRef>();
		if (pivot.source) {
			CollectFromTableRef(context, *pivot.source, cte_aliases, privilege, out);
		}
		break;
	}
	case TableReferenceType::SHOW_REF: {
		// DESCRIBE/SUMMARIZE recurse via query; SHOW TABLES/DATABASES have no query and emit nothing
		auto &show = ref.Cast<ShowRef>();
		if (show.query) {
			CollectFromQueryNode(context, *show.query, cte_aliases, out);
		}
		break;
	}
	default:
		break;
	}
}

static void CollectFromQueryNode(ClientContext &context, const QueryNode &node, std::unordered_set<string> cte_aliases,
                                 vector<TableRefRow> &out) {
	// recurse CTE bodies with outer alias set, then add this node's CTE names before the main node
	for (auto &entry : node.cte_map.map) {
		if (entry.second && entry.second->query && entry.second->query->node) {
			CollectFromQueryNode(context, *entry.second->query->node, cte_aliases, out);
		}
	}
	for (auto &entry : node.cte_map.map) {
		cte_aliases.insert(entry.first);
	}

	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			CollectFromTableRef(context, *select.from_table, cte_aliases, "select", out);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			if (child) {
				CollectFromQueryNode(context, *child, cte_aliases, out);
			}
		}
		break;
	}
	default:
		break;
	}
}

static void CollectFromStatement(ClientContext &context, const SQLStatement &stmt, vector<TableRefRow> &out) {
	std::unordered_set<string> cte_aliases;
	switch (stmt.type) {
	case StatementType::SELECT_STATEMENT: {
		auto &select = stmt.Cast<SelectStatement>();
		if (select.node) {
			CollectFromQueryNode(context, *select.node, cte_aliases, out);
		}
		break;
	}
	case StatementType::INSERT_STATEMENT: {
		auto &insert = stmt.Cast<InsertStatement>();
		if (!insert.table.empty()) {
			out.push_back(ResolveRef(context, insert.catalog, insert.schema, insert.table, "base_table", "insert"));
		}
		if (insert.select_statement && insert.select_statement->node) {
			CollectFromQueryNode(context, *insert.select_statement->node, cte_aliases, out);
		}
		break;
	}
	case StatementType::UPDATE_STATEMENT: {
		auto &update = stmt.Cast<UpdateStatement>();
		if (update.table) {
			CollectFromTableRef(context, *update.table, cte_aliases, "update", out);
		}
		if (update.from_table) {
			CollectFromTableRef(context, *update.from_table, cte_aliases, "select", out);
		}
		break;
	}
	case StatementType::DELETE_STATEMENT: {
		auto &del = stmt.Cast<DeleteStatement>();
		if (del.table) {
			CollectFromTableRef(context, *del.table, cte_aliases, "delete", out);
		}
		for (auto &using_clause : del.using_clauses) {
			if (using_clause) {
				CollectFromTableRef(context, *using_clause, cte_aliases, "select", out);
			}
		}
		break;
	}
	case StatementType::CREATE_STATEMENT: {
		auto &create = stmt.Cast<CreateStatement>();
		if (!create.info) {
			break;
		}
		auto &info = *create.info;
		switch (info.type) {
		case CatalogType::TABLE_ENTRY: {
			auto &table_info = info.Cast<CreateTableInfo>();
			if (!table_info.table.empty()) {
				out.push_back(ResolveRef(context, table_info.catalog, table_info.schema, table_info.table, "base_table",
				                         "create"));
			}
			if (table_info.query && table_info.query->node) {
				CollectFromQueryNode(context, *table_info.query->node, cte_aliases, out);
			}
			break;
		}
		case CatalogType::VIEW_ENTRY: {
			auto &view_info = info.Cast<CreateViewInfo>();
			if (!view_info.view_name.empty()) {
				out.push_back(ResolveRef(context, view_info.catalog, view_info.schema, view_info.view_name,
				                         "base_table", "create"));
			}
			if (view_info.query && view_info.query->node) {
				CollectFromQueryNode(context, *view_info.query->node, cte_aliases, out);
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	case StatementType::DROP_STATEMENT: {
		auto &drop = stmt.Cast<DropStatement>();
		if (drop.info && (drop.info->type == CatalogType::TABLE_ENTRY || drop.info->type == CatalogType::VIEW_ENTRY) &&
		    !drop.info->name.empty()) {
			out.push_back(
			    ResolveRef(context, drop.info->catalog, drop.info->schema, drop.info->name, "base_table", "drop"));
		}
		break;
	}
	case StatementType::ALTER_STATEMENT: {
		auto &alter = stmt.Cast<AlterStatement>();
		if (alter.info) {
			auto catalog_type = alter.info->GetCatalogType();
			if ((catalog_type == CatalogType::TABLE_ENTRY || catalog_type == CatalogType::VIEW_ENTRY) &&
			    !alter.info->name.empty()) {
				out.push_back(ResolveRef(context, alter.info->catalog, alter.info->schema, alter.info->name,
				                         "base_table", "alter"));
			}
		}
		break;
	}
	default:
		break;
	}
}

struct ParseSqlGetTablesBindData : public TableFunctionData {
	vector<TableRefRow> tables;
};

struct ParseSqlGetTablesState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

static unique_ptr<FunctionData> ParseSqlGetTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("catalog");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("schema");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("ref_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("privilege_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto sql = input.inputs[0].GetValue<string>();

	Parser parser;
	parser.ParseQuery(sql);

	vector<TableRefRow> rows;
	for (auto &stmt : parser.statements) {
		CollectFromStatement(context, *stmt, rows);
	}

	std::sort(rows.begin(), rows.end());
	rows.erase(std::unique(rows.begin(), rows.end()), rows.end());

	auto result = make_uniq<ParseSqlGetTablesBindData>();
	result->tables = std::move(rows);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseSqlGetTablesInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	return make_uniq<ParseSqlGetTablesState>();
}

static void SetStringOrNull(DataChunk &output, idx_t col, idx_t row, const string &value) {
	if (value.empty()) {
		FlatVector::SetNull(output.data[col], row, true);
	} else {
		output.SetValue(col, row, Value(value));
	}
}

static void ParseSqlGetTablesScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParseSqlGetTablesBindData>();
	auto &state = data_p.global_state->Cast<ParseSqlGetTablesState>();
	idx_t count = 0;
	while (state.offset < bind_data.tables.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.tables[state.offset];
		SetStringOrNull(output, 0, count, row.catalog);
		SetStringOrNull(output, 1, count, row.schema);
		output.SetValue(2, count, Value(row.table));
		output.SetValue(3, count, Value(row.ref_type));
		output.SetValue(4, count, Value(row.privilege));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void RegisterN6kParseSqlGetTables(ExtensionLoader &loader) {
	TableFunction func("n6k_parse_sql_get_tables", {LogicalType::VARCHAR}, ParseSqlGetTablesScan, ParseSqlGetTablesBind,
	                   ParseSqlGetTablesInitGlobal);
	loader.RegisterFunction(func);
}

} // namespace duckdb
