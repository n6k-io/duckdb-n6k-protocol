#include "n6k_source.hpp"

#include "n6k_link.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_wire_type.hpp"
#include "crossing_substrait.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

#include <algorithm>

namespace duckdb {

// The server spells verbs as crossing does; one it names that crossing does not know is dropped.
static vector<CrossingVerb> CrossingVerbsNamed(const vector<string> &names) {
	vector<CrossingVerb> verbs;
	for (auto verb : CrossingVerbs()) {
		if (std::find(names.begin(), names.end(), CrossingVerbName(verb)) != names.end()) {
			verbs.push_back(verb);
		}
	}
	return verbs;
}

//! The operators crossing can move (RuleKindFor in crossing's rules.cpp).
static constexpr LogicalOperatorType CROSSING_OPERATOR_TYPES[] = {
    LogicalOperatorType::LOGICAL_FILTER,
    LogicalOperatorType::LOGICAL_PROJECTION,
    LogicalOperatorType::LOGICAL_DISTINCT,
    LogicalOperatorType::LOGICAL_SAMPLE,
    LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY,
    LogicalOperatorType::LOGICAL_WINDOW,
    LogicalOperatorType::LOGICAL_UNNEST,
    LogicalOperatorType::LOGICAL_PIVOT,
    LogicalOperatorType::LOGICAL_JOIN,
    LogicalOperatorType::LOGICAL_COMPARISON_JOIN,
    LogicalOperatorType::LOGICAL_ANY_JOIN,
    LogicalOperatorType::LOGICAL_ASOF_JOIN,
    LogicalOperatorType::LOGICAL_POSITIONAL_JOIN,
    LogicalOperatorType::LOGICAL_CROSS_PRODUCT,
    LogicalOperatorType::LOGICAL_UNION,
    LogicalOperatorType::LOGICAL_EXCEPT,
    LogicalOperatorType::LOGICAL_INTERSECT,
    LogicalOperatorType::LOGICAL_LIMIT,
    LogicalOperatorType::LOGICAL_ORDER_BY,
    LogicalOperatorType::LOGICAL_TOP_N,
    LogicalOperatorType::LOGICAL_MATERIALIZED_CTE,
    LogicalOperatorType::LOGICAL_CTE_REF,
};

static string CrossingOperatorName(LogicalOperatorType type) {
	return StringUtil::Lower(LogicalOperatorToString(type));
}

static vector<string> AllCrossingOperators() {
	vector<string> names;
	for (auto type : CROSSING_OPERATOR_TYPES) {
		names.push_back(CrossingOperatorName(type));
	}
	return names;
}

N6kSource::N6kSource(DatabaseInstance &db_p, std::shared_ptr<CatalogSession> session_p, string catalog_name_p)
    : db(db_p), session(std::move(session_p)), catalog_name(std::move(catalog_name_p)),
      group(session->HasCapability(n6k::CAP_CATALOG_PLANS) ? session->SocketGroup() : nullptr),
      floor_catalog(group ? session->ServerCatalog() : string()) {
	for (auto &name : AllCrossingOperators()) {
		crossing_operators.insert(name);
	}
}

vector<string> N6kSource::CrossingOperators() {
	lock_guard<mutex> guard(crossing_lock);
	vector<string> names;
	for (auto &name : AllCrossingOperators()) {
		if (crossing_operators.count(name)) {
			names.push_back(name);
		}
	}
	return names;
}

vector<string> N6kSource::SetCrossingOperators(const string &action, const vector<string> &names) {
	auto all = AllCrossingOperators();
	case_insensitive_set_t known(all.begin(), all.end());
	for (auto &name : names) {
		if (!known.count(name)) {
			throw InvalidInputException("n6k_client_set: unknown crossing operator \"%s\"; expected one of: %s", name,
			                            StringUtil::Join(all, ", "));
		}
	}
	{
		lock_guard<mutex> guard(crossing_lock);
		if (StringUtil::CIEquals(action, "set")) {
			crossing_operators = case_insensitive_set_t(names.begin(), names.end());
		} else if (StringUtil::CIEquals(action, "add")) {
			crossing_operators.insert(names.begin(), names.end());
		} else if (StringUtil::CIEquals(action, "remove")) {
			for (auto &name : names) {
				crossing_operators.erase(name);
			}
		} else {
			throw InvalidInputException("n6k_client_set: unknown action \"%s\"; expected set, add or remove", action);
		}
	}
	return CrossingOperators();
}

const void *N6kSource::Group() const {
	return group ? group : this;
}

void N6kSource::ForgetListing() {
	lock_guard<mutex> guard(lock);
	listed = false;
}

void N6kSource::ForgetListing() {
	lock_guard<mutex> guard(lock);
	listed = false;
}

void N6kSource::ListIfNeeded() {
	if (listed) {
		return;
	}
	tables.clear();
	schema_verbs.clear();
	auto listing = session->ListTables();
	for (auto &row : listing.tables) {
		N6kTableInfo info;
		info.verbs = row.verbs;
		info.row_ids = row.row_ids;
		info.primary_keys = row.primary_keys;
		info.not_null = row.not_null;
		info.unique_keys = row.unique_keys;
		tables[row.schema][row.name] = std::move(info);
	}
	for (auto &row : listing.schemas) {
		schema_verbs[row.name] = row.verbs;
	}
	listed = true;
}

vector<string> N6kSource::Schemas() {
	auto out = session->ListSchemas();
	if (std::find(out.begin(), out.end(), string(DEFAULT_SCHEMA)) == out.end()) {
		out.push_back(DEFAULT_SCHEMA);
	}
	return out;
}

vector<string> N6kSource::Tables(const string &schema) {
	lock_guard<mutex> guard(lock);
	listed = false;
	ListIfNeeded();
	vector<string> out;
	auto it = tables.find(schema);
	if (it == tables.end()) {
		return out;
	}
	for (auto &entry : it->second) {
		out.push_back(entry.first);
	}
	return out;
}

N6kTableInfo N6kSource::Info(const string &schema, const string &table) {
	lock_guard<mutex> guard(lock);
	ListIfNeeded();
	auto schema_it = tables.find(schema);
	if (schema_it == tables.end()) {
		return N6kTableInfo();
	}
	auto table_it = schema_it->second.find(table);
	return table_it == schema_it->second.end() ? N6kTableInfo() : table_it->second;
}

static optional_idx ColumnPosition(const CrossingTable &table, const string &column) {
	for (idx_t i = 0; i < table.column_names.size(); i++) {
		if (StringUtil::CIEquals(table.column_names[i], column)) {
			return i;
		}
	}
	return optional_idx();
}

// The listing can predate the description, so a constraint on a column no longer described is skipped.
static void DeclareUnique(CrossingTable &table, const std::vector<std::string> &columns, bool is_primary_key) {
	vector<Identifier> names;
	for (auto &column : columns) {
		if (!ColumnPosition(table, column).IsValid()) {
			return;
		}
		names.emplace_back(column);
	}
	if (!names.empty()) {
		table.constraints.push_back(make_uniq<UniqueConstraint>(std::move(names), is_primary_key));
	}
}

static void DeclareConstraints(CrossingTable &table, const N6kTableInfo &info) {
	set<idx_t> not_null;
	auto add_not_null = [&](const vector<string> &columns) {
		for (auto &column : columns) {
			auto position = ColumnPosition(table, column);
			if (position.IsValid()) {
				not_null.insert(position.GetIndex());
			}
		}
	};
	add_not_null(info.not_null);
	add_not_null(info.primary_keys);
	for (auto position : not_null) {
		table.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(position)));
	}
	DeclareUnique(table, info.primary_keys, true);
	for (auto &unique_key : info.unique_keys) {
		DeclareUnique(table, unique_key, false);
	}
}

CrossingTable N6kSource::Describe(const string &schema, const string &name) {
	ArrowArrayStream schema_stream;
	schema_stream.release = nullptr;
	session->FetchTableSchema(schema, name, &schema_stream);

	ArrowSchema arrow_schema;
	if (schema_stream.get_schema(&schema_stream, &arrow_schema) != 0) {
		if (schema_stream.release) {
			schema_stream.release(&schema_stream);
		}
		throw IOException("n6k[%s]: table \"%s.%s\" returned an unreadable Arrow schema", catalog_name, schema, name);
	}

	CrossingTable table;
	table.name = name;
	table.catalog = floor_catalog;
	{
		Connection conn(db);
		for (int i = 0; i < arrow_schema.n_children; i++) {
			auto *child = arrow_schema.children[i];
			ArrowSchemaMetadata metadata(child->metadata);
			if (metadata.GetOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME) == n6k::DUCKDB_TYPE_EXTENSION) {
				table.Column(child->name, n6k::DeserializeTypeFromBase64(
				                              metadata.GetOption(ArrowSchemaMetadata::ARROW_METADATA_KEY)));
				continue;
			}
			auto arrow_type = ArrowType::GetArrowLogicalType(*conn.context, *child);
			table.Column(child->name, arrow_type->GetDuckType(true));
		}
	}
	if (arrow_schema.release) {
		arrow_schema.release(&arrow_schema);
	}
	if (schema_stream.release) {
		schema_stream.release(&schema_stream);
	}

	auto info = Info(schema, name);
	table.verbs = CrossingVerbsNamed(info.verbs);
	table.returning = true;
	table.row_ids = info.row_ids;
	DeclareConstraints(table, info);
	return table;
}

void N6kSource::DescribeChanged(const string &schema, CrossingTable &table) {
	CrossingSource::DescribeChanged(schema, table);
	table.catalog = floor_catalog;
	auto info = Info(schema, table.name);
	if (!info.verbs.empty()) {
		table.verbs = CrossingVerbsNamed(info.verbs);
		table.row_ids = info.row_ids;
	}
}

CrossingSchema N6kSource::DescribeSchema(const string &schema) {
	lock_guard<mutex> guard(lock);
	ListIfNeeded();
	CrossingSchema described;
	described.name = schema;
	auto it = schema_verbs.find(schema);
	if (it != schema_verbs.end()) {
		described.verbs = CrossingVerbsNamed(it->second);
	}
	return described;
}

static bool NamedCallOf(const Expression &expr, string &name, vector<string> &args) {
	auto add_args = [&](const vector<unique_ptr<Expression>> &children) {
		for (auto &child : children) {
			args.push_back(child->GetReturnType().ToString());
		}
	};
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		auto &call = expr.Cast<BoundFunctionExpression>();
		name = call.Function().GetName().GetIdentifierName();
		add_args(call.GetChildren());
		return true;
	}
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &call = expr.Cast<BoundAggregateExpression>();
		name = call.Function().GetName().GetIdentifierName();
		add_args(call.GetChildren());
		return true;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &call = expr.Cast<BoundWindowExpression>();
		if (!call.AggregateFunction()) {
			return false;
		}
		name = call.AggregateFunction()->GetName().GetIdentifierName();
		add_args(call.GetChildren());
		return true;
	}
	default:
		return false;
	}
}

bool N6kSource::ServerHasFunction(const string &name, const vector<string> &args) {
	auto signature = name + "(" + StringUtil::Join(args, ", ") + ")";
	{
		lock_guard<mutex> guard(server_functions_lock);
		auto known = server_functions.find(signature);
		if (known != server_functions.end()) {
			return known->second;
		}
	}
	auto exists = session->HasFunction(name, args);
	lock_guard<mutex> guard(server_functions_lock);
	server_functions[signature] = exists;
	return exists;
}

void N6kSource::ForgetServerFunctions() {
	lock_guard<mutex> guard(server_functions_lock);
	server_functions.clear();
}

static bool NamedCallOf(const Expression &expr, string &name, vector<string> &args) {
	auto add_args = [&](const vector<unique_ptr<Expression>> &children) {
		for (auto &child : children) {
			args.push_back(child->GetReturnType().ToString());
		}
	};
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION: {
		auto &call = expr.Cast<BoundFunctionExpression>();
		name = call.Function().GetName().GetIdentifierName();
		add_args(call.GetChildren());
		return true;
	}
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &call = expr.Cast<BoundAggregateExpression>();
		name = call.Function().GetName().GetIdentifierName();
		add_args(call.GetChildren());
		return true;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &call = expr.Cast<BoundWindowExpression>();
		if (!call.AggregateFunction()) {
			return false;
		}
		name = call.AggregateFunction()->GetName().GetIdentifierName();
		add_args(call.GetChildren());
		return true;
	}
	default:
		return false;
	}
}

bool N6kSource::ServerHasFunction(const string &name, const vector<string> &args) {
	auto signature = name + "(" + StringUtil::Join(args, ", ") + ")";
	{
		lock_guard<mutex> guard(server_functions_lock);
		auto known = server_functions.find(signature);
		if (known != server_functions.end()) {
			return known->second;
		}
	}
	auto exists = session->HasFunction(name, args);
	lock_guard<mutex> guard(server_functions_lock);
	server_functions[signature] = exists;
	return exists;
}

void N6kSource::ForgetServerFunctions() {
	lock_guard<mutex> guard(server_functions_lock);
	server_functions.clear();
}

CrossingVerdict N6kSource::AcceptsCall(const Expression &expr) {
	if (!session->HasCapability(n6k::CAP_CROSSING)) {
		return CrossingVerdict::No("the server does not accept plans");
	}
	std::string reason;
	if (!SubstraitCanRenderCall(expr, reason)) {
		return CrossingVerdict::No(reason);
	}
	string name;
	vector<string> args;
	if (!NamedCallOf(expr, name, args)) {
		return CrossingVerdict::Yes();
	}
	if (!session->HasCapability(n6k::CAP_HAS_FUNCTION)) {
		return CrossingVerdict::No("the server cannot say which functions it has");
	}
	if (!ServerHasFunction(name, args)) {
		return CrossingVerdict::No("the server has no function " + name + "(" + StringUtil::Join(args, ", ") + ")");
	}
	return CrossingVerdict::Yes();
}

CrossingVerdict N6kSource::AcceptsType(const LogicalType &type) {
	return CrossingVerdict::Yes();
}

CrossingVerdict N6kSource::AcceptsOperator(const LogicalOperator &op) {
	auto name = CrossingOperatorName(op.type);
	{
		lock_guard<mutex> guard(crossing_lock);
		if (!crossing_operators.count(name)) {
			return CrossingVerdict::No(name + " is not in the n6k_client_set crossing list");
		}
	}
	std::string reason;
	if (!SubstraitCanRenderOperator(op, reason)) {
		return CrossingVerdict::No(reason);
	}
	return CrossingVerdict::Yes();
}

namespace {

class N6kNearLink : public CrossingLink {
public:
	N6kNearLink(std::shared_ptr<CatalogSession> session_p, std::shared_ptr<n6k::N6kLinkState> state_p)
	    : session(std::move(session_p)), state(std::move(state_p)) {
	}
	~N6kNearLink() override {
		try {
			session->CloseLink(*state);
		} catch (...) { // NOLINT(bugprone-empty-catch): a dead connection has already released the link
		}
	}

	idx_t Streams() override {
		return state->Streams();
	}
	unique_ptr<CrossingStream> Open(idx_t id, optional_ptr<CrossingReceiver> receiver) override {
		return state->Open(id, receiver);
	}

private:
	std::shared_ptr<CatalogSession> session;
	std::shared_ptr<n6k::N6kLinkState> state;
};

} // namespace

unique_ptr<CrossingLink> N6kSource::Connect(ClientContext &context) {
	const auto threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	auto state = session->OpenLink(db, MaxValue<idx_t>(threads, 1), [&context]() { return context.IsInterrupted(); });
	// Weak: a strong reference would have the context's own transaction keep the context alive.
	weak_ptr<ClientContext> weak_context = context.shared_from_this();
	std::weak_ptr<n6k::N6kLinkState> weak_state = state;
	state->SetOnClose([weak_context, weak_state]() {
		auto interrupted = weak_context.lock();
		auto link = weak_state.lock();
		if (interrupted && link && interrupted->IsInterrupted()) {
			link->SendCancel();
		}
	});
	return make_uniq<N6kNearLink>(session, std::move(state));
}

void N6kSource::Detach(ClientContext &context) {
	session->SetPushHandler(nullptr);
	session->Detach();
}

} // namespace duckdb
