#include "n6k_source.hpp"

#include "n6k_body_builders.hpp"
#include "n6k_fetch.hpp"
#include "n6k_frame_source.hpp"
#include "crossing_substrait.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include <algorithm>

namespace duckdb {

namespace {

void ReplaceColumnRefs(unique_ptr<Expression> &expr, const vector<ColumnBinding> &bindings) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &ref = expr->Cast<BoundColumnRefExpression>();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == ref.binding) {
				expr = make_uniq<BoundReferenceExpression>(ref.return_type, i);
				return;
			}
		}
		throw InternalException("n6k crossing: a feed column is not in scope");
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { ReplaceColumnRefs(child, bindings); });
}

vector<unique_ptr<DataChunk>> EvaluateFeed(ClientContext &context, LogicalOperator &op) {
	vector<unique_ptr<DataChunk>> chunks;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CHUNK_GET: {
		auto rows = SeamRowsOf(op);
		ColumnDataScanState scan_state;
		rows->InitializeScan(scan_state);
		while (true) {
			auto chunk = make_uniq<DataChunk>();
			chunk->Initialize(Allocator::DefaultAllocator(), rows->Types());
			if (!rows->Scan(scan_state, *chunk) || chunk->size() == 0) {
				break;
			}
			chunks.push_back(std::move(chunk));
		}
		return chunks;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &get = op.Cast<LogicalExpressionGet>();
		for (idx_t start = 0; start < get.expressions.size(); start += STANDARD_VECTOR_SIZE) {
			auto end = MinValue<idx_t>(start + STANDARD_VECTOR_SIZE, get.expressions.size());
			auto chunk = make_uniq<DataChunk>();
			chunk->Initialize(Allocator::DefaultAllocator(), get.expr_types);
			for (idx_t row = start; row < end; row++) {
				auto &exprs = get.expressions[row];
				for (idx_t col = 0; col < exprs.size(); col++) {
					auto value = ExpressionExecutor::EvaluateScalar(context, *exprs[col]);
					chunk->SetValue(col, row - start, value.DefaultCastAs(get.expr_types[col]));
				}
			}
			chunk->SetCardinality(end - start);
			chunks.push_back(std::move(chunk));
		}
		return chunks;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &proj = op.Cast<LogicalProjection>();
		auto inputs = EvaluateFeed(context, *op.children[0]);
		auto bindings = op.children[0]->GetColumnBindings();
		vector<unique_ptr<Expression>> exprs;
		vector<LogicalType> types;
		for (auto &expr : proj.expressions) {
			auto copy = expr->Copy();
			ReplaceColumnRefs(copy, bindings);
			types.push_back(copy->return_type);
			exprs.push_back(std::move(copy));
		}
		ExpressionExecutor executor(context, exprs);
		for (auto &input : inputs) {
			auto out = make_uniq<DataChunk>();
			out->Initialize(Allocator::DefaultAllocator(), types);
			executor.Execute(*input, *out);
			chunks.push_back(std::move(out));
		}
		return chunks;
	}
	default:
		throw NotImplementedException("n6k crossing: cannot evaluate a %s feed locally", EnumUtil::ToChars(op.type));
	}
}

bool ContainsFloor(const LogicalOperator &op) {
	if (FloorOf(op)) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsFloor(*child)) {
			return true;
		}
	}
	return false;
}

struct ArmedWaker {
	std::mutex lock;
	bool armed = false;
	CrossingWaker waker;

	void Arm(const CrossingWaker &w) {
		std::lock_guard<std::mutex> guard(lock);
		waker = w;
		armed = true;
	}

	void FireIfArmed() {
		CrossingWaker to_wake;
		{
			std::lock_guard<std::mutex> guard(lock);
			if (!armed) {
				return;
			}
			armed = false;
			to_wake = waker;
		}
		to_wake.Wake();
	}
};

template <class State>
void WakeOnFrame(const std::shared_ptr<State> &state) {
	std::weak_ptr<State> weak = state;
	state->req->SetOnFrame([weak]() {
		if (auto held = weak.lock()) {
			held->waker.FireIfArmed();
		}
	});
}

unique_ptr<CrossingWriteResult> WriteResultOf(const string &catalog, const n6k::Frame &frame) {
	if (frame.type == n6k::FrameType::ERR) {
		n6k::ThrowN6kError(catalog, n6k::OP_WRITE_PLAN, frame.payload);
	}
	if (frame.type != n6k::FrameType::RESP_END) {
		return nullptr;
	}
	n6k::JsonDoc doc(frame.payload);
	auto *rowcount = doc.Parsed() ? duckdb_yyjson::yyjson_obj_get(doc.Root(), "rowcount") : nullptr;
	if (!rowcount || !duckdb_yyjson::yyjson_is_int(rowcount)) {
		throw IOException("n6k[" + catalog + "] write: server response has no rowcount");
	}
	auto affected = NumericCast<idx_t>(duckdb_yyjson::yyjson_get_sint(rowcount));
	return make_uniq<CrossingWriteResult>(CrossingWriteResult::Done(affected));
}

struct N6kWriteState {
	std::shared_ptr<n6k::RequestState> req;
	ArmedWaker waker;

	~N6kWriteState() {
		if (req) {
			req->SetOnFrame({});
		}
	}
};

struct N6kReadState : public N6kFrameSourceState {
	explicit N6kReadState(ClientContext &ctx) : N6kFrameSourceState(ctx) {
	}

	ArrowTableSchema arrow_table;
	bool schema_ready = false;
	bool any_cast = false;
	vector<bool> needs_cast;
	unique_ptr<DataChunk> scratch;
	ArmedWaker waker;
};

void ResolveReadSchema(ClientContext &context, N6kReadState &state, const vector<LogicalType> &types,
                       const string &catalog) {
	ArrowTableFunction::PopulateArrowTableSchema(context, state.arrow_table, *state.decoder.GetSchema());
	auto wire_types = state.arrow_table.GetTypes();
	if (wire_types.size() != types.size()) {
		throw IOException("n6k[" + catalog + "] read: server returned " + std::to_string(wire_types.size()) +
		                  " columns, plan expects " + std::to_string(types.size()));
	}
	state.needs_cast.resize(types.size(), false);
	for (idx_t i = 0; i < types.size(); i++) {
		if (wire_types[i] != types[i]) {
			state.needs_cast[i] = true;
			state.any_cast = true;
		}
	}
	if (state.any_cast) {
		state.scratch = make_uniq<DataChunk>();
		state.scratch->Initialize(context, wire_types);
	}
	vector<column_t> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		ids.push_back(i);
	}
	state.scan_state.column_ids = std::move(ids);
	state.schema_ready = true;
}

void EmitReadChunk(ClientContext &context, N6kReadState &state, DataChunk &chunk, idx_t count,
                   const vector<LogicalType> &types) {
	if (!state.any_cast) {
		chunk.SetCardinality(count);
		ArrowTableFunction::ArrowToDuckDB(state.scan_state, state.arrow_table.GetColumns(), chunk);
		chunk.Verify();
		return;
	}
	state.scratch->Reset();
	state.scratch->SetCardinality(count);
	ArrowTableFunction::ArrowToDuckDB(state.scan_state, state.arrow_table.GetColumns(), *state.scratch);
	chunk.SetCardinality(state.scratch->size());
	for (idx_t i = 0; i < types.size(); i++) {
		if (state.needs_cast[i]) {
			VectorOperations::Cast(context, state.scratch->data[i], chunk.data[i], state.scratch->size());
		} else {
			chunk.data[i].Reference(state.scratch->data[i]);
		}
	}
	chunk.Verify();
}

bool DrainAndResolve(ClientContext &context, N6kReadState &state, const vector<LogicalType> &types,
                     const string &catalog) {
	N6kDrainFrames(state);
	if (!state.schema_ready && state.decoder.HasSchema()) {
		ResolveReadSchema(context, state, types, catalog);
	}
	return state.decoder.HasBatch() || state.stream_done || state.saw_error;
}

CrossingReader OpenReader(ClientContext &context, const std::shared_ptr<CatalogSession> &session, const string &catalog,
                          const string &body, const vector<LogicalType> &types) {
	auto state = std::make_shared<N6kReadState>(context);
	state->req = session->StartOpRequest(n6k::OP_READ_PLAN, body, [&context]() { return context.IsInterrupted(); });
	WakeOnFrame(state);

	return [state, catalog, types](ClientContext &ctx, DataChunk &chunk,
	                               const CrossingWaker &waker) -> CrossingReadResult {
		for (;;) {
			if (state->has_batch) {
				auto len = static_cast<idx_t>(state->scan_state.chunk->arrow_array.length);
				if (state->scan_state.chunk_offset >= len) {
					state->has_batch = false;
				} else {
					idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, len - state->scan_state.chunk_offset);
					EmitReadChunk(ctx, *state, chunk, count, types);
					state->scan_state.chunk_offset += chunk.size();
					return CrossingReadResult::Rows();
				}
			}
			if (state->decoder.HasBatch()) {
				if (!state->schema_ready) {
					throw IOException("n6k[" + catalog + "] read: a batch arrived before the schema");
				}
				state->scan_state.Reset();
				auto wrapper = make_shared_ptr<ArrowArrayWrapper>();
				state->decoder.TryPopBatch(&wrapper->arrow_array);
				state->scan_state.chunk = std::move(wrapper);
				state->scan_state.chunk_offset = 0;
				state->has_batch = true;
				continue;
			}
			if (state->saw_error) {
				N6kThrowSourceError(*state, catalog, n6k::OP_READ_PLAN, "read");
			}
			if (state->stream_done) {
				return CrossingReadResult::Done();
			}
			if (DrainAndResolve(ctx, *state, types, catalog)) {
				continue;
			}
			state->waker.Arm(waker);
			if (DrainAndResolve(ctx, *state, types, catalog)) {
				continue;
			}
			return CrossingReadResult::Wait();
		}
	};
}

} // namespace

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

string RenderCrossingPlan(const LogicalOperator &plan) {
	return RenderSubstraitJson(plan);
}

N6kSource::N6kSource(DatabaseInstance &db_p, std::shared_ptr<CatalogSession> session_p, string catalog_name_p)
    : db(db_p), session(std::move(session_p)), catalog_name(std::move(catalog_name_p)),
      group(session->HasCapability(n6k::CAP_CATALOG_PLANS) ? session->SocketGroup() : nullptr),
      floor_catalog(group ? session->ServerCatalog() : string()) {
}

const void *N6kSource::Group() {
	return group;
}

void N6kSource::ListIfNeeded() {
	if (listed) {
		return;
	}
	tables.clear();
	schema_verbs.clear();
	auto listing = session->ListTables(-1);
	for (auto &row : listing.tables) {
		N6kTableInfo info;
		info.verbs = row.verbs;
		info.primary_keys = row.primary_keys;
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

vector<string> N6kSource::TablesIn(const string &schema, int64_t txn) {
	if (txn < 0) {
		return Tables(schema);
	}
	vector<string> out;
	for (auto &row : session->ListTables(txn).tables) {
		if (StringUtil::CIEquals(row.schema, schema)) {
			out.push_back(row.name);
		}
	}
	return out;
}

CrossingTable N6kSource::Describe(const string &schema, const string &name) {
	return DescribeIn(schema, name, -1);
}

CrossingTable N6kSource::DescribeIn(const string &schema, const string &name, int64_t txn) {
	ArrowArrayStream schema_stream;
	schema_stream.release = nullptr;
	session->FetchTableSchema(schema, name, txn, &schema_stream);

	ArrowSchema arrow_schema;
	if (schema_stream.get_schema(&schema_stream, &arrow_schema) != 0) {
		if (schema_stream.release) {
			schema_stream.release(&schema_stream);
		}
		throw IOException("n6k[%s]: table \"%s.%s\" returned an unreadable Arrow schema", catalog_name, schema, name);
	}

	CrossingTable table;
	table.name = name;
	{
		Connection conn(db);
		for (int i = 0; i < arrow_schema.n_children; i++) {
			auto *child = arrow_schema.children[i];
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
	{
		lock_guard<mutex> guard(lock);
		Columns columns;
		columns.names = table.column_names;
		columns.types = table.column_types;
		described[schema][name] = std::move(columns);
	}

	N6kTableInfo info;
	if (txn < 0) {
		info = Info(schema, name);
	} else {
		for (auto &row : session->ListTables(txn).tables) {
			if (StringUtil::CIEquals(row.schema, schema) && StringUtil::CIEquals(row.name, name)) {
				info.verbs = row.verbs;
				info.primary_keys = row.primary_keys;
			}
		}
	}
	table.verbs = CrossingVerbsNamed(info.verbs);
	table.key = info.primary_keys;
	table.key_unique = !table.key.empty();
	return table;
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

CrossingPlan N6kSource::Plan(const CrossingPlanRequest &request) {
	if (request.verb != CrossingVerb::SELECT) {
		return CrossingPlan::Of(MakeSeamNode(0, request.seam.types));
	}
	Columns columns;
	{
		lock_guard<mutex> guard(lock);
		auto schema_it = described.find(request.schema);
		if (schema_it == described.end()) {
			return CrossingPlan::Declined("the table was never described");
		}
		auto table_it = schema_it->second.find(request.table);
		if (table_it == schema_it->second.end()) {
			return CrossingPlan::Declined("the table was never described");
		}
		columns = table_it->second;
	}
	return CrossingPlan::Of(
	    MakeFloorNode(0, request.schema, request.table, columns.names, columns.types, floor_catalog));
}

CrossingVerdict N6kSource::AcceptsCall(const Expression &expr) {
	if (!session->HasCapability(n6k::CAP_CROSSING)) {
		return CrossingVerdict::No("the server does not accept plans");
	}
	std::string reason;
	if (!SubstraitCanRenderCall(expr, reason)) {
		return CrossingVerdict::No(reason);
	}
	return CrossingVerdict::Yes();
}

CrossingVerdict N6kSource::AcceptsType(const LogicalType &type) {
	return CrossingVerdict::Yes();
}

CrossingVerdict N6kSource::AcceptsOperator(const LogicalOperator &op) {
	std::string reason;
	if (!SubstraitCanRenderOperator(op, reason)) {
		return CrossingVerdict::No(reason);
	}
	return CrossingVerdict::Yes();
}

unique_ptr<N6kSession> N6kSource::Begin(ClientContext &context) {
	return make_uniq<N6kSession>(*this, session, catalog_name, context.transaction.IsAutoCommit());
}

void N6kSource::Detach(ClientContext &context) {
	session->SetPushHandler(nullptr);
	session->Detach();
}

N6kSession::N6kSession(N6kSource &source_p, std::shared_ptr<CatalogSession> session_p, string catalog_name_p,
                       bool autocommit_p)
    : source(source_p), session(std::move(session_p)), catalog_name(std::move(catalog_name_p)),
      autocommit(autocommit_p) {
}

N6kSession::~N6kSession() {
	if (txn < 0) {
		return;
	}
	try {
		session->TxnEnd(txn, false);
	} catch (...) {
	}
}

int64_t N6kSession::Txn() {
	if (autocommit) {
		return -1;
	}
	lock_guard<mutex> guard(lock);
	if (txn < 0) {
		txn = session->TxnBegin();
	}
	return txn;
}

vector<string> N6kSession::Tables(const string &schema) {
	return source.TablesIn(schema, Txn());
}

CrossingTable N6kSession::Describe(const string &schema, const string &name) {
	return source.DescribeIn(schema, name, Txn());
}

static const char *OnConflictName(OnCreateConflict on_conflict) {
	switch (on_conflict) {
	case OnCreateConflict::ERROR_ON_CONFLICT:
		return "error";
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return "ignore";
	case OnCreateConflict::REPLACE_ON_CONFLICT:
		return "replace";
	default:
		throw NotImplementedException("n6k crossing: CREATE VIEW with on-conflict '%s' is not supported",
		                              EnumUtil::ToChars(on_conflict));
	}
}

// Only views reach here: n6k's schema entry sends tables over its own ops.
void N6kSession::Ddl(ClientContext &context, const CrossingDdl &ddl) {
	StreamCancelCheck cancel = [&context]() {
		return context.IsInterrupted();
	};
	if (ddl.verb == CrossingVerb::CREATE && ddl.plan) {
		auto &view = ddl.create->Cast<CreateViewInfo>();
		session->RunOp(n6k::OP_CREATE_VIEW,
		               n6k_body::CreateView(ddl.schema, view.view_name, OnConflictName(view.on_conflict),
		                                    CrossingViewColumnNames(view), RenderCrossingPlan(*ddl.plan), Txn()),
		               cancel);
		return;
	}
	if (ddl.verb == CrossingVerb::DROP && ddl.drop->type == CatalogType::VIEW_ENTRY) {
		session->RunOp(n6k::OP_DROP_VIEW,
		               n6k_body::DropView(ddl.schema, ddl.drop->name,
		                                  ddl.drop->if_not_found == OnEntryNotFound::RETURN_NULL, Txn()),
		               cancel);
		return;
	}
	throw NotImplementedException("n6k crossing: %s is not shipped through crossing", CrossingVerbName(ddl.verb));
}

void N6kSession::Commit() {
	lock_guard<mutex> guard(lock);
	if (txn < 0) {
		return;
	}
	auto ending = txn;
	txn = -1;
	session->TxnEnd(ending, true);
}

void N6kSession::Rollback() {
	lock_guard<mutex> guard(lock);
	if (txn < 0) {
		return;
	}
	auto ending = txn;
	txn = -1;
	session->TxnEnd(ending, false);
}

CrossingScan N6kSession::Read(ClientContext &context, const CrossingQuery &query) {
	auto body = n6k_body::ReadPlan(RenderCrossingPlan(query.plan), Txn());
	CrossingScan scan;
	scan.partitions = 1;
	auto session_copy = session;
	auto catalog = catalog_name;
	auto types = query.types;
	scan.open = [session_copy, catalog, body, types](ClientContext &ctx, idx_t) {
		return OpenReader(ctx, session_copy, catalog, body, types);
	};
	return scan;
}

CrossingWriter N6kSession::Write(ClientContext &context, const CrossingQuery &query) {
	if (query.written.table.empty()) {
		throw InternalException("n6k crossing: a write names no table");
	}
	auto &use = query.written;
	std::string plan_json;
	std::string arrow;
	if (ContainsFloor(query.plan)) {
		plan_json = RenderCrossingPlan(query.plan);
	} else {
		auto chunks = EvaluateFeed(
		    context, const_cast<LogicalOperator &>(query.plan)); // NOLINT(cppcoreguidelines-pro-type-const-cast)
		vector<LogicalType> types = chunks.empty() ? query.plan.types : chunks[0]->GetTypes();
		vector<string> names;
		for (idx_t i = 0; i < types.size(); i++) {
			names.push_back("c" + std::to_string(i));
		}
		ArrowBuffer ipc_buf;
		ArrowBufferInit(&ipc_buf);
		SerializeChunksToArrowIPC(context, types, names, chunks, &ipc_buf);
		arrow.assign(reinterpret_cast<const char *>(ipc_buf.data), static_cast<size_t>(ipc_buf.size_bytes));
		ArrowBufferReset(&ipc_buf);
	}
	auto body = n6k_body::WritePlan(CrossingVerbName(query.kind), use.schema, use.table, query.key_columns,
	                                query.set_columns, plan_json, Txn());
	body.push_back('\n');
	body.append(arrow);

	auto state = std::make_shared<N6kWriteState>();
	auto session_copy = session;
	auto catalog = catalog_name;
	return [state, session_copy, catalog, body](ClientContext &ctx, const CrossingWaker &waker) {
		if (!state->req) {
			state->req =
			    session_copy->StartOpRequest(n6k::OP_WRITE_PLAN, body, [&ctx]() { return ctx.IsInterrupted(); });
			WakeOnFrame(state);
		}
		for (;;) {
			n6k::Frame frame;
			while (state->req->TryNext(frame)) {
				if (auto done = WriteResultOf(catalog, frame)) {
					return *done;
				}
			}
			if (state->req->Exhausted()) {
				throw IOException("n6k[" + catalog + "] write: the request ended without a result");
			}
			state->waker.Arm(waker);
			if (state->req->TryNext(frame)) {
				if (auto done = WriteResultOf(catalog, frame)) {
					return *done;
				}
				continue;
			}
			return CrossingWriteResult::Wait();
		}
	};
}

} // namespace duckdb
