#include "request_handlers.hpp"

#include "arrow_ipc_encode.hpp"
#include "frame_codec.hpp"
#include "insert_scan.hpp"
#include "n6k_catalog_list.hpp"
#include "n6k_permissions.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_sql_builder.hpp"
#include "crossing.hpp"
#include "crossing_substrait.hpp"
#include "serve_reactor.hpp"
#include "ws_json.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <mutex>

namespace duckdb {
namespace n6k {

// The wire carries only a function name, never a schema, so RPC resolves in one: `main`, matching
// where CREATE MACRO lands by default.
static constexpr const char *RPC_SCHEMA = "main";

// Where OP_RPC_TABLE stages the rows pushed with the call. Temp, so it is scoped to the worker's
// own Connection and two concurrent requests cannot see each other's input.
static constexpr const char *RPC_INPUT_VIEW = "__n6k_rpc_in";

static std::string PackSchemasChunk(const FrameTarget &target, const vector<string> &schemas) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_CHUNK, target);
	PackString(pk, "schemas");
	pk.pack_array(static_cast<uint32_t>(schemas.size()));
	for (auto &s : schemas) {
		pk.pack(s);
	}
	return std::string(sb.data(), sb.size());
}

void SendError(ServeReactor &reactor, const FrameTarget &target, const std::string &type, const std::string &message,
               bool retriable) {
	reactor.SendFrame(PackErr(target, type, message, retriable));
}

static void HandleSchemaList(ServeReactor &reactor, Connection &conn, const std::string &catalog,
                             const FrameTarget &target) {
	vector<string> schemas;
	// Direct catalog introspection needs an active transaction (unlike SendQuery).
	conn.context->RunFunctionInTransaction([&]() {
		auto &cat = Catalog::GetCatalog(*conn.context, catalog);
		ListCatalogSchemas(*conn.context, cat, schemas);
	});

	reactor.SendFrame(PackSchemasChunk(target, schemas));
	reactor.SendFrame(PackRespEnd(target));
}

static void PackStrings(msgpack::packer<msgpack::sbuffer> &pk, const vector<string> &strings) {
	pk.pack_array(static_cast<uint32_t>(strings.size()));
	for (auto &s : strings) {
		pk.pack(s);
	}
}

static std::string PackTablesChunk(const FrameTarget &target, const vector<TablePermissionRow> &rows,
                                   const vector<SchemaPermissionRow> &schemas) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(4 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_CHUNK, target);
	PackString(pk, "tables");
	pk.pack_array(static_cast<uint32_t>(rows.size()));
	for (auto &r : rows) {
		pk.pack_map(4);
		PackString(pk, "schema");
		pk.pack(r.schema);
		PackString(pk, "name");
		pk.pack(r.name);
		PackString(pk, "verbs");
		PackStrings(pk, r.verbs);
		PackString(pk, "primary_keys");
		PackStrings(pk, r.primary_key);
	}
	PackString(pk, "schemas");
	pk.pack_array(static_cast<uint32_t>(schemas.size()));
	for (auto &schema : schemas) {
		pk.pack_map(2);
		PackString(pk, "name");
		pk.pack(schema.name);
		PackString(pk, "verbs");
		PackStrings(pk, schema.verbs);
	}
	return std::string(sb.data(), sb.size());
}

struct TxnScope {
	std::shared_ptr<PinnedTxn> pinned;
	std::unique_lock<std::mutex> guard;
	Connection *conn = nullptr;
};

static TxnScope ConnectionFor(ServeReactor &reactor, Connection &own, const SessionRef &sess,
                              duckdb_yyjson::yyjson_val *root) {
	TxnScope scope;
	scope.conn = &own;
	auto *txn_val = root ? duckdb_yyjson::yyjson_obj_get(root, "txn") : nullptr;
	if (!txn_val) {
		return scope;
	}
	if (!duckdb_yyjson::yyjson_is_int(txn_val)) {
		throw InvalidInputException("n6k_server: \"txn\" must be an integer");
	}
	auto txn = duckdb_yyjson::yyjson_get_sint(txn_val);
	scope.pinned = reactor.FindTxn(sess.ns, txn);
	if (!scope.pinned) {
		throw TransactionException("n6k_server: transaction %lld is not open on this session", txn);
	}
	scope.guard = std::unique_lock<std::mutex>(scope.pinned->mu);
	if (!scope.pinned->conn || scope.pinned->dropped.load()) {
		throw TransactionException("n6k_server: transaction %lld was already closed", txn);
	}
	scope.conn = scope.pinned->conn.get();
	return scope;
}

static void HandleTablesList(ServeReactor &reactor, Connection &own, const SessionRef &sess, const char *body,
                             size_t body_len, const FrameTarget &target) {
	string schema_filter_str;
	bool has_filter = false;
	JsonDoc doc(body, body_len);
	if (body_len > 0) {
		if (!doc.Parsed() || !duckdb_yyjson::yyjson_is_obj(doc.Root())) {
			throw InvalidInputException("n6k_server: OP_TABLES_LIST body is not a JSON object");
		}
		auto *schema_val = duckdb_yyjson::yyjson_obj_get(doc.Root(), "schema");
		if (schema_val) {
			if (!duckdb_yyjson::yyjson_is_str(schema_val)) {
				throw InvalidInputException("n6k_server: OP_TABLES_LIST \"schema\" must be a string");
			}
			schema_filter_str = duckdb_yyjson::yyjson_get_str(schema_val);
			has_filter = !schema_filter_str.empty();
		}
	}
	auto scope = ConnectionFor(reactor, own, sess, body_len > 0 ? doc.Root() : nullptr);
	auto &conn = *scope.conn;
	const std::string &catalog = sess.catalog;
	optional_ptr<const string> schema_filter = has_filter ? &schema_filter_str : nullptr;
	vector<TablePermissionRow> rows;
	vector<SchemaPermissionRow> schemas;
	conn.context->RunFunctionInTransaction([&]() {
		auto &cat = Catalog::GetCatalog(*conn.context, catalog);
		// Dispatching, not the plain collector: a bridge- or provider-backed catalog registers its own
		// and knows what the catalog metadata cannot express — whether a bridged table is writeable,
		// and the primary key that survived the hop. Falls back to plain classification otherwise.
		CollectCatalogPermissions(*conn.context, cat, schema_filter, nullptr, rows);
		CollectSchemaPermissions(*conn.context, cat, schema_filter, schemas);
	});
	reactor.SendFrame(PackTablesChunk(target, rows, schemas));
	reactor.SendFrame(PackRespEnd(target));
}

// Per-request cap on rows per RESP_CHUNK; 0 leaves batching to DuckDB.
//
// Caps chunk SIZE only, not delivery pacing. Shrinking `streaming_buffer_size` would pace delivery
// too, but a buffer too small to hold one chunk deadlocks a real table scan — the first Fetch never
// returns. `range()` and literal selects survive it, which is why it looks fine until a table is
// involved.
static idx_t RequestedBatchRows(duckdb_yyjson::yyjson_val *root) {
	return static_cast<idx_t>(JsonGetUint(root, "_batch_rows", 0));
}

static bool SendRowsAsCreditedFrames(ServeReactor &reactor, ArrowIpcStreamEncoder &enc, const FrameTarget &target,
                                     RequestSlot &slot, DataChunk &chunk, idx_t batch_rows) {
	if (batch_rows == 0 || chunk.size() <= batch_rows) {
		if (!reactor.AcquireCredit(slot)) {
			return false;
		}
		reactor.SendFrame(PackRespChunkArrow(target, enc.EncodeChunk(chunk)));
		return true;
	}
	// A real copy at ordinary vector capacity: slicing `chunk` as a view hands the Arrow encoder
	// dictionary vectors borrowing the parent's buffers, and sizing the destination to `batch_rows`
	// gives it a capacity of 1 or 2 — the encoder survives neither.
	//
	// DataChunk::Copy takes the range as [offset, source_count) over `sel` and requires the
	// destination empty and flat, hence the Reset each round.
	SelectionVector sel(chunk.size());
	for (idx_t i = 0; i < chunk.size(); i++) {
		sel.set_index(i, i);
	}
	DataChunk slice;
	slice.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
	for (idx_t offset = 0; offset < chunk.size(); offset += batch_rows) {
		const idx_t end = MinValue<idx_t>(offset + batch_rows, chunk.size());
		slice.Reset();
		chunk.Copy(slice, sel, end, offset);
		if (!reactor.AcquireCredit(slot)) {
			return false;
		}
		reactor.SendFrame(PackRespChunkArrow(target, enc.EncodeChunk(slice)));
	}
	return true;
}

static void StreamResult(ServeReactor &reactor, QueryResult &result, ClientContext &context, const FrameTarget &target,
                         RequestSlot &slot, idx_t batch_rows = 0) {
	if (result.HasError()) {
		result.ThrowError();
	}
	ArrowIpcStreamEncoder enc(context, result.types, result.names);
	reactor.SendFrame(PackRespSchema(target, enc.SchemaMessage()));
	bool cancelled = false;
	while (auto chunk = result.Fetch()) {
		if (chunk->size() == 0) {
			continue;
		}
		if (!SendRowsAsCreditedFrames(reactor, enc, target, slot, *chunk, batch_rows)) {
			cancelled = true;
			break;
		}
	}
	if (cancelled) {
		reactor.SendFrame(PackRespEndCancelled(target));
		return;
	}
	auto eos = enc.TakeEndOfStreamMessage();
	if (!eos.empty()) {
		reactor.SendFrame(PackRespChunkArrow(target, eos));
	}
	reactor.SendFrame(PackRespEndRowcountNull(target));
}

// Raw passthrough SQL: not catalog-qualified, so it can reach other catalogs.
static void HandleQuery(ServeReactor &reactor, Connection &conn, const char *body, size_t body_len,
                        const FrameTarget &target, RequestSlot &slot) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_QUERY body is not valid JSON");
	}
	auto *root = doc.Root();
	auto sql = JsonGetStr(root, "sql");
	if (sql.empty()) {
		throw InvalidInputException("n6k_server: OP_QUERY requires a \"sql\" field");
	}
	const auto batch_rows = RequestedBatchRows(root);
	auto result = conn.SendQuery(sql);
	StreamResult(reactor, *result, *conn.context, target, slot, batch_rows);
}

static SubstraitCatalogCheck PlanCatalogCheck(ServeReactor &reactor, RequestSlot &slot) {
	return [&reactor, &slot](const string &catalog) {
		reactor.AdmitPlanCatalog(slot, catalog);
	};
}

static void HandleReadPlan(ServeReactor &reactor, Connection &own, const SessionRef &sess, const char *body,
                           size_t body_len, const FrameTarget &target, RequestSlot &slot) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_READ_PLAN body is not valid JSON");
	}
	auto *root = doc.Root();
	auto plan = JsonGetStr(root, "plan");
	if (plan.empty()) {
		throw InvalidInputException("n6k_server: OP_READ_PLAN requires a \"plan\" field");
	}
	const auto batch_rows = RequestedBatchRows(root);
	auto scope = ConnectionFor(reactor, own, sess, root);
	auto &conn = *scope.conn;
	auto rel = DecodeSubstraitJson(conn, sess.catalog, plan, "", PlanCatalogCheck(reactor, slot));
	auto result = conn.context->Query(make_uniq<RelationStatement>(rel), QueryParameters(!scope.pinned));
	StreamResult(reactor, *result, *conn.context, target, slot, batch_rows);
}

static void HandleTxnBegin(ServeReactor &reactor, const SessionRef &sess, const FrameTarget &target) {
	reactor.SendFrame(PackRespEndTxn(target, reactor.BeginTxn(sess.ns)));
}

static void HandleTxnEnd(ServeReactor &reactor, const SessionRef &sess, const char *body, size_t body_len,
                         const FrameTarget &target, bool commit) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: transaction body is not valid JSON");
	}
	auto *txn_val = duckdb_yyjson::yyjson_obj_get(doc.Root(), "txn");
	if (!txn_val || !duckdb_yyjson::yyjson_is_int(txn_val)) {
		throw InvalidInputException("n6k_server: a transaction end requires \"txn\"");
	}
	auto txn = duckdb_yyjson::yyjson_get_sint(txn_val);
	auto pinned = reactor.TakeTxn(sess.ns, txn);
	if (!pinned) {
		throw TransactionException("n6k_server: transaction %lld is not open on this session", txn);
	}
	std::lock_guard<std::mutex> guard(pinned->mu);
	if (commit) {
		pinned->conn->Commit();
	} else {
		pinned->conn->Rollback();
	}
	pinned->conn.reset();
	reactor.SendFrame(PackRespEndOk(target));
}

static void HandleTableSchema(ServeReactor &reactor, Connection &own, const SessionRef &sess, const char *body,
                              size_t body_len, const FrameTarget &target) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_TABLE_SCHEMA body is not valid JSON");
	}
	auto scope = ConnectionFor(reactor, own, sess, doc.Root());
	auto &conn = *scope.conn;
	const std::string &catalog = sess.catalog;
	auto schema = JsonGetStr(doc.Root(), "schema");
	auto table = JsonGetStr(doc.Root(), "table");
	if (schema.empty() || table.empty()) {
		throw InvalidInputException("n6k_server: OP_TABLE_SCHEMA requires \"schema\" and \"table\"");
	}
	auto result = conn.Query(BuildTableSchemaSql(catalog, schema, table));
	if (result->HasError()) {
		result->ThrowError();
	}
	ArrowIpcStreamEncoder enc(*conn.context, result->types, result->names);
	reactor.SendFrame(PackRespSchema(target, enc.SchemaMessage()));
	reactor.SendFrame(PackRespEnd(target));
}

// The result's first cell is the affected-row count (absent → 0).
static void HandleExec(ServeReactor &reactor, Connection &conn, const char *body, size_t body_len,
                       const FrameTarget &target) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_EXEC body is not valid JSON");
	}
	auto sql = JsonGetStr(doc.Root(), "sql");
	if (sql.empty()) {
		throw InvalidInputException("n6k_server: OP_EXEC requires a \"sql\" field");
	}
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError();
	}
	int64_t rowcount = 0;
	if (result->RowCount() > 0 && result->ColumnCount() > 0) {
		auto value = result->GetValue(0, 0);
		if (!value.IsNull()) {
			rowcount = value.GetValue<int64_t>();
		}
	}
	reactor.SendFrame(PackRespEndRowcount(target, rowcount));
}

static void HandleCreateTable(ServeReactor &reactor, Connection &conn, const std::string &catalog, const char *body,
                              size_t body_len, const FrameTarget &target) {
	using namespace duckdb_yyjson; // NOLINT
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_CREATE_TABLE body is not valid JSON");
	}
	auto *root = doc.Root();
	auto schema = JsonGetStr(root, "schema");
	auto name = JsonGetStr(root, "name");
	auto *cols = yyjson_obj_get(root, "columns");
	if (schema.empty() || name.empty() || !cols || !yyjson_is_arr(cols)) {
		throw InvalidInputException("n6k_server: OP_CREATE_TABLE requires \"schema\", \"name\", and \"columns\"");
	}
	auto result = conn.Query(BuildCreateTableSql(catalog, schema, name, cols));
	if (result->HasError()) {
		result->ThrowError();
	}
	reactor.SendFrame(PackRespEndOk(target));
}

static void HandleAlterTable(ServeReactor &reactor, Connection &conn, const std::string &catalog, const char *body,
                             size_t body_len, const FrameTarget &target) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_ALTER_TABLE body is not valid JSON");
	}
	auto *root = doc.Root();
	auto schema = JsonGetStr(root, "schema");
	auto table = JsonGetStr(root, "table");
	auto kind = JsonGetStr(root, "kind");
	if (schema.empty() || table.empty() || kind.empty()) {
		throw InvalidInputException("n6k_server: OP_ALTER_TABLE requires \"schema\", \"table\", and \"kind\"");
	}
	auto *details = duckdb_yyjson::yyjson_obj_get(root, "details");
	auto result = conn.Query(BuildAlterSql(catalog, schema, table, kind, details));
	if (result->HasError()) {
		result->ThrowError();
	}
	reactor.SendFrame(PackRespEndOk(target));
}

static OnCreateConflict ViewOnConflict(const std::string &on_conflict) {
	if (on_conflict == "error") {
		return OnCreateConflict::ERROR_ON_CONFLICT;
	}
	if (on_conflict == "ignore") {
		return OnCreateConflict::IGNORE_ON_CONFLICT;
	}
	if (on_conflict == "replace") {
		return OnCreateConflict::REPLACE_ON_CONFLICT;
	}
	throw InvalidInputException("n6k_server: OP_CREATE_VIEW \"on_conflict\" must be error, ignore or replace");
}

static void HandleCreateView(ServeReactor &reactor, Connection &own, const SessionRef &sess, const char *body,
                             size_t body_len, const FrameTarget &target, RequestSlot &slot) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_CREATE_VIEW body is not valid JSON");
	}
	auto *root = doc.Root();
	auto schema = JsonGetStr(root, "schema");
	auto name = JsonGetStr(root, "name");
	auto plan = JsonGetStr(root, "plan");
	if (schema.empty() || name.empty() || plan.empty()) {
		throw InvalidInputException("n6k_server: OP_CREATE_VIEW requires \"schema\", \"name\", and \"plan\"");
	}
	auto on_conflict = ViewOnConflict(JsonGetStr(root, "on_conflict"));
	auto names_json = JsonGetStrArray(root, "names");
	vector<string> names(names_json.begin(), names_json.end());
	auto scope = ConnectionFor(reactor, own, sess, root);
	auto &conn = *scope.conn;

	auto info = make_uniq<CreateViewInfo>(sess.catalog, schema, name);
	info->query = DecodeSubstraitView(conn, sess.catalog, plan, names, PlanCatalogCheck(reactor, slot));
	info->on_conflict = on_conflict;
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	auto result = conn.Query(std::move(statement));
	if (result->HasError()) {
		result->ThrowError();
	}
	reactor.SendFrame(PackRespEndOk(target));
}

static void HandleDropView(ServeReactor &reactor, Connection &own, const SessionRef &sess, const char *body,
                           size_t body_len, const FrameTarget &target) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_DROP_VIEW body is not valid JSON");
	}
	auto *root = doc.Root();
	auto schema = JsonGetStr(root, "schema");
	auto name = JsonGetStr(root, "name");
	if (schema.empty() || name.empty()) {
		throw InvalidInputException("n6k_server: OP_DROP_VIEW requires \"schema\" and \"name\"");
	}
	auto scope = ConnectionFor(reactor, own, sess, root);
	auto &conn = *scope.conn;

	auto statement = make_uniq<DropStatement>();
	statement->info->type = CatalogType::VIEW_ENTRY;
	statement->info->catalog = sess.catalog;
	statement->info->schema = schema;
	statement->info->name = name;
	statement->info->if_not_found =
	    JsonGetBool(root, "if_exists") ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	auto result = conn.Query(std::move(statement));
	if (result->HasError()) {
		result->ThrowError();
	}
	reactor.SendFrame(PackRespEndOk(target));
}

static void HandleInsert(ServeReactor &reactor, Connection &conn, const std::string &catalog, const char *body,
                         size_t body_len, const FrameTarget &target) {
	auto *nl = static_cast<const char *>(memchr(body, '\n', body_len));
	if (!nl) {
		throw InvalidInputException("n6k_server: OP_INSERT body missing header newline");
	}
	size_t header_len = static_cast<size_t>(nl - body);
	JsonDoc doc(body, header_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_INSERT header is not valid JSON");
	}
	auto schema = JsonGetStr(doc.Root(), "schema");
	auto table = JsonGetStr(doc.Root(), "table");
	if (schema.empty() || table.empty()) {
		throw InvalidInputException("n6k_server: OP_INSERT requires \"schema\" and \"table\"");
	}

	const uint8_t *arrow = reinterpret_cast<const uint8_t *>(body) + header_len + 1;
	size_t arrow_len = body_len - header_len - 1;

	int64_t rowcount = CountArrowIpcRows(arrow, arrow_len);

	InsertArrowBytes bytes {arrow, arrow_len};
	auto rel = conn.TableFunction(N6K_INSERT_SCAN_FN, {Value::POINTER(reinterpret_cast<uintptr_t>(&bytes))});
	rel->Insert(catalog, schema, table);

	reactor.SendFrame(PackRespEndRowcount(target, rowcount));
}

static CrossingVerb WriteVerbOf(const std::string &verb) {
	if (verb == "insert") {
		return CrossingVerb::INSERT;
	}
	if (verb == "update") {
		return CrossingVerb::UPDATE;
	}
	if (verb == "delete") {
		return CrossingVerb::DELETE_;
	}
	throw InvalidInputException("n6k_server: OP_WRITE_PLAN verb '%s' is not insert, update or delete", verb);
}

// ExecuteWrite binds and runs inside a transaction. A pinned connection has one open; the worker's
// own connection gets one for the statement.
static idx_t WriteInTransaction(Connection &conn, bool pinned, const CrossingWriteTarget &target,
                                unique_ptr<TableRef> seam) {
	if (pinned) {
		return ExecuteWrite(*conn.context, target, std::move(seam));
	}
	conn.BeginTransaction();
	idx_t changed;
	try {
		changed = ExecuteWrite(*conn.context, target, std::move(seam));
	} catch (...) {
		conn.Rollback();
		throw;
	}
	conn.Commit();
	return changed;
}

static void HandleWritePlan(ServeReactor &reactor, Connection &own, const SessionRef &sess, const char *body,
                            size_t body_len, const FrameTarget &target, RequestSlot &slot) {
	const std::string &catalog = sess.catalog;
	auto *nl = static_cast<const char *>(memchr(body, '\n', body_len));
	if (!nl) {
		throw InvalidInputException("n6k_server: OP_WRITE_PLAN body missing header newline");
	}
	size_t header_len = static_cast<size_t>(nl - body);
	JsonDoc doc(body, header_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_WRITE_PLAN header is not valid JSON");
	}
	auto *root = doc.Root();
	CrossingWriteTarget write;
	write.catalog = catalog;
	write.schema = JsonGetStr(root, "schema");
	write.table = JsonGetStr(root, "table");
	write.verb = WriteVerbOf(JsonGetStr(root, "verb"));
	write.key_columns = JsonGetStrArray(root, "key_columns");
	write.set_columns = JsonGetStrArray(root, "set_columns");
	auto plan = JsonGetStr(root, "plan");
	if (write.schema.empty() || write.table.empty()) {
		throw InvalidInputException("n6k_server: OP_WRITE_PLAN requires \"schema\" and \"table\"");
	}
	auto scope = ConnectionFor(reactor, own, sess, root);
	auto &conn = *scope.conn;

	const uint8_t *arrow = reinterpret_cast<const uint8_t *>(body) + header_len + 1;
	size_t arrow_len = body_len - header_len - 1;
	InsertArrowBytes bytes {arrow, arrow_len};
	unique_ptr<TableRef> seam;
	if (arrow_len > 0) {
		seam = conn.TableFunction(N6K_INSERT_SCAN_FN, {Value::POINTER(reinterpret_cast<uintptr_t>(&bytes))})
		           ->GetTableRef();
	} else if (!plan.empty()) {
		seam = DecodeSubstraitJson(conn, catalog, plan, "", PlanCatalogCheck(reactor, slot))->GetTableRef();
	} else {
		throw InvalidInputException("n6k_server: OP_WRITE_PLAN carries neither rows nor a plan");
	}
	auto rowcount = WriteInTransaction(conn, scope.pinned != nullptr, write, std::move(seam));
	reactor.SendFrame(PackRespEndRowcount(target, NumericCast<int64_t>(rowcount)));
}

// A generator in the host process, reached through three scalar UDFs it registered on the served
// connection. Python half: packages/python/src/n6k_server/rpc_stream.py.
//
//   open(handle, function, args_json, input_ipc BLOB) -> BLOB   the Arrow IPC schema message
//   next(handle)                                      -> BLOB   one IPC record batch, NULL at end
//   close(handle)                                     -> BLOB   trailing end-of-stream bytes
//
// vcatprovider creates the catalog entry and lists it via vcatprovider_stream_functions().
struct StreamFunctionInfo {
	std::string open_udf;
	std::string next_udf;
	std::string close_udf;
};

struct RpcTarget {
	// The catalog to qualify with, or "" to call the bare name.
	std::string catalog;
	// True when the target declares a TABLE parameter, so the input goes as a sub-select. False
	// means it goes as the name of a view for the target to open itself.
	bool takes_table_arg = false;
	// Set when the resolved entry is a host stream. Non-null means drive the host's generator
	// directly rather than binding SQL: the blobs it produces are already the Arrow the client
	// wants, so routing them through the executor would decode and re-encode them for nothing.
	unique_ptr<StreamFunctionInfo> stream;
};

// The UDFs behind `entry_name`, or nullptr for an ordinary table function or when
// vcatprovider is not loaded (the Prepare fails on the unknown function).
static unique_ptr<StreamFunctionInfo> HostStreamOrNull(Connection &conn, const std::string &catalog,
                                                       const std::string &schema, const std::string &entry_name) {
	auto stmt = conn.Prepare("SELECT open_udf, next_udf, close_udf FROM vcatprovider_stream_functions() "
	                         "WHERE catalog = ? AND schema = ? AND entry_name = ?");
	if (stmt->HasError()) {
		return nullptr;
	}
	vector<Value> params;
	params.emplace_back(catalog);
	params.emplace_back(schema);
	params.emplace_back(entry_name);
	auto result = stmt->Execute(params);
	if (result->HasError()) {
		result->ThrowError();
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return nullptr;
	}
	auto info = make_uniq<StreamFunctionInfo>();
	info->open_udf = StringValue::Get(chunk->GetValue(0, 0));
	info->next_udf = StringValue::Get(chunk->GetValue(1, 0));
	info->close_udf = StringValue::Get(chunk->GetValue(2, 0));
	return info;
}

static bool DeclaresTableArg(CatalogEntry &entry) {
	auto &fn_entry = entry.Cast<TableFunctionCatalogEntry>();
	for (auto &fn : fn_entry.functions.functions) {
		for (auto &arg : fn.arguments) {
			if (arg.id() == LogicalTypeId::TABLE) {
				return true;
			}
		}
		if (fn.varargs.id() == LogicalTypeId::TABLE) {
			return true;
		}
	}
	return false;
}

// A table macro created inside the served catalog lives there; an extension's table function lives
// in the system catalog under its bare name. The served catalog wins, so a local macro shadows a
// global of the same name.
//
// `takes_table_arg` is why this inspects the entry rather than just locating it: a native in-out
// table function declares LogicalType::TABLE and must be handed a sub-select, while a SQL macro
// cannot declare a table parameter at all and instead takes the input view's NAME to open with
// query_table(t). Deciding per target keeps both kinds callable without the client knowing which.
static RpcTarget ResolveRpcTarget(Connection &conn, const std::string &catalog, const std::string &schema,
                                  const std::string &function) {
	RpcTarget target;
	bool local_table_function = false;
	conn.context->RunFunctionInTransaction([&]() {
		// One lookup, not one per kind: TABLE_MACRO_ENTRY and TABLE_FUNCTION_ENTRY resolve from the
		// same catalog set, so asking for either returns whichever is there. Classify after.
		EntryLookupInfo fn_lookup(CatalogType::TABLE_FUNCTION_ENTRY, function);

		auto &cat = Catalog::GetCatalog(*conn.context, catalog);
		if (auto local = cat.GetEntry(*conn.context, schema, fn_lookup, OnEntryNotFound::RETURN_NULL)) {
			target.catalog = catalog;
			// A macro cannot declare a table parameter at all; only a real table function has a
			// shape worth inspecting.
			if (local->type == CatalogType::TABLE_FUNCTION_ENTRY) {
				local_table_function = true;
				target.takes_table_arg = DeclaresTableArg(*local);
			}
			return;
		}
		// Not in the served catalog: fall back to the bare name, where an extension's table functions
		// live. Looked up purely to learn its shape.
		auto global_fn =
		    Catalog::GetEntry(*conn.context, SYSTEM_CATALOG, DEFAULT_SCHEMA, fn_lookup, OnEntryNotFound::RETURN_NULL);
		if (global_fn && global_fn->type == CatalogType::TABLE_FUNCTION_ENTRY) {
			target.takes_table_arg = DeclaresTableArg(*global_fn);
		}
	});
	// Outside the transaction above: this is a query of its own on the same connection.
	if (local_table_function) {
		target.stream = HostStreamOrNull(conn, catalog, schema, function);
	}
	return target;
}

// The request's "args" as JSON text, for a target that parses them itself rather than having them
// rendered into SQL. Absent args are an empty list, not null: the host binds them positionally.
static std::string RpcArgsJson(duckdb_yyjson::yyjson_val *args) {
	if (!args) {
		return "[]";
	}
	if (!duckdb_yyjson::yyjson_is_arr(args)) {
		throw InvalidInputException("n6k_server: rpc \"args\" must be an array");
	}
	size_t len = 0;
	char *txt = duckdb_yyjson::yyjson_val_write(args, 0, &len);
	if (!txt) {
		throw InternalException("n6k_server: failed to serialize rpc \"args\"");
	}
	std::string out(txt, len);
	free(txt);
	return out;
}

// Runs the close UDF exactly once, from the destructor if the stream did not get that far.
//
// Closing runs the host generator's own `finally`, so a subscription unsubscribes when the client
// cancels or the query errors rather than whenever the host's GC gets to it.
struct RpcStreamCloser {
	RpcStreamCloser(Connection &conn, const std::string &close_udf, std::string handle)
	    : conn(conn), close_udf(close_udf), handle(std::move(handle)) {
	}

	~RpcStreamCloser() {
		try {
			CloseAndTakeTrailingBytes();
		} catch (...) { // NOLINT: a failing close must not displace the error already in flight
		}
	}

	RpcStreamCloser(const RpcStreamCloser &) = delete;
	RpcStreamCloser &operator=(const RpcStreamCloser &) = delete;

	std::string CloseAndTakeTrailingBytes() {
		if (closed) {
			return std::string();
		}
		closed = true;
		auto result = conn.Query("SELECT " + QuoteIdent(close_udf) + "('" + handle + "')");
		if (result->HasError()) {
			result->ThrowError();
		}
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			return std::string();
		}
		auto value = chunk->GetValue(0, 0);
		return value.IsNull() ? std::string() : StringValue::Get(value);
	}

	Connection &conn;
	const std::string &close_udf;
	std::string handle;
	bool closed = false;
};

// Both RPC ops, when the name belongs to the host rather than the catalog: drive the host's
// generator one batch at a time and forward each as a RESP_CHUNK.
//
// Pull a batch, THEN wait for credit, so a generator that paces itself in wall-clock time keeps its
// cadence instead of starting its next interval only once the client's credit arrives.
//
// The catalog entry is only used to find the UDF names: binding it as SQL would decode the host's
// Arrow blobs into vectors and immediately re-encode them, and would buffer where this does not.
// `SELECT * FROM db.main.<name>(...)` takes the bound path instead, inside vcatprovider.
static void HandleRpcStream(ServeReactor &reactor, Connection &conn, const StreamFunctionInfo &rpc,
                            const std::string &function, const std::string &args_json, const uint8_t *input,
                            size_t input_len, const FrameTarget &target, RequestSlot &slot) {
	// Only ever a bound parameter, so a counter suffices to keep concurrent calls apart.
	static std::atomic<uint64_t> handle_counter {0};
	const std::string handle = "h" + std::to_string(handle_counter.fetch_add(1) + 1);

	auto open_stmt = conn.Prepare("SELECT " + QuoteIdent(rpc.open_udf) + "(?, ?, ?, ?)");
	if (open_stmt->HasError()) {
		open_stmt->GetErrorObject().Throw("n6k_server: RPC open function \"" + rpc.open_udf + "\" is unusable: ");
	}
	// Bound, never interpolated — `args_json` is the client's, and the pushed rows are raw Arrow.
	// vector<Value>, not the variadic Execute: that overload routes std::string through
	// Value::CreateValue<string>, which builds a BLOB, so a VARCHAR parameter never binds.
	vector<Value> open_params;
	open_params.emplace_back(handle);
	open_params.emplace_back(function);
	open_params.emplace_back(args_json);
	open_params.push_back(input ? Value::BLOB(input, input_len) : Value(LogicalType::BLOB));
	auto open_result = open_stmt->Execute(open_params);
	if (open_result->HasError()) {
		open_result->ThrowError();
	}
	auto open_chunk = open_result->Fetch();
	if (!open_chunk || open_chunk->size() == 0) {
		throw IOException("n6k_server: RPC \"%s\" returned no Arrow schema when opened", function);
	}
	// Named, not a temporary: StringValue::Get borrows from the Value.
	const Value schema_value = open_chunk->GetValue(0, 0);
	if (schema_value.IsNull()) {
		throw IOException("n6k_server: RPC \"%s\" returned no Arrow schema when opened", function);
	}
	// Opened, so the generator exists and must be closed on every path out of here.
	RpcStreamCloser closer(conn, rpc.close_udf, handle);
	const auto &schema_message = StringValue::Get(schema_value);
	// Empty is rejected rather than forwarded: RESP_SCHEMA is what tells the client how to read
	// every chunk that follows, and an empty one fails later and further away.
	if (schema_message.empty()) {
		throw IOException("n6k_server: RPC \"%s\" returned an empty Arrow schema when opened", function);
	}
	reactor.SendFrame(PackRespSchema(target, schema_message));

	auto next_stmt = conn.Prepare("SELECT " + QuoteIdent(rpc.next_udf) + "(?)");
	if (next_stmt->HasError()) {
		next_stmt->GetErrorObject().Throw("n6k_server: RPC next function \"" + rpc.next_udf + "\" is unusable: ");
	}

	bool cancelled = false;
	for (;;) {
		vector<Value> next_params;
		next_params.emplace_back(handle);
		auto next_result = next_stmt->Execute(next_params);
		if (next_result->HasError()) {
			next_result->ThrowError();
		}
		auto chunk = next_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		auto batch = chunk->GetValue(0, 0);
		// NULL is how the host says the generator is exhausted; there is no separate end signal.
		if (batch.IsNull()) {
			break;
		}
		if (!reactor.AcquireCredit(slot)) {
			cancelled = true;
			break;
		}
		reactor.SendFrame(PackRespChunkArrow(target, StringValue::Get(batch)));
	}

	auto trailing = closer.CloseAndTakeTrailingBytes();
	if (cancelled) {
		// No trailing marker on a cancelled stream, matching what a cancelled SQL result sends.
		reactor.SendFrame(PackRespEndCancelled(target));
		return;
	}
	if (!trailing.empty()) {
		reactor.SendFrame(PackRespChunkArrow(target, trailing));
	}
	reactor.SendFrame(PackRespEndRowcountNull(target));
}

// "Scalar" describes the ARGUMENTS, not the return: both RPC ops answer with an Arrow stream. The
// name resolves through ordinary DuckDB resolution rather than a registry, so what is callable is
// whatever the host put in the served catalog or registered on the instance.
static void HandleRpcScalar(ServeReactor &reactor, Connection &conn, const std::string &catalog, const char *body,
                            size_t body_len, const FrameTarget &target, RequestSlot &slot) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_RPC_SCALAR body is not valid JSON");
	}
	auto *root = doc.Root();
	auto function = JsonGetStr(root, "function");
	if (function.empty()) {
		throw InvalidInputException("n6k_server: OP_RPC_SCALAR requires a \"function\" field");
	}
	auto rpc = ResolveRpcTarget(conn, catalog, RPC_SCHEMA, function);
	if (rpc.stream) {
		HandleRpcStream(reactor, conn, *rpc.stream, function, RpcArgsJson(duckdb_yyjson::yyjson_obj_get(root, "args")),
		                nullptr, 0, target, slot);
		return;
	}
	auto sql = BuildRpcCallSql(rpc.catalog, RPC_SCHEMA, function, duckdb_yyjson::yyjson_obj_get(root, "args"));
	const auto batch_rows = RequestedBatchRows(root);
	auto result = conn.SendQuery(sql);
	StreamResult(reactor, *result, *conn.context, target, slot, batch_rows);
}

// Like RPC_SCALAR, plus a table of rows pushed in the request body.
//
// The rows arrive as Arrow IPC after the header's newline exactly as OP_INSERT's do, so they are
// staged through the same private scan function and exposed as a temp view. Every request builds
// its own Connection and therefore its own temp catalog, so the fixed name cannot collide.
static void HandleRpcTable(ServeReactor &reactor, Connection &conn, const std::string &catalog, const char *body,
                           size_t body_len, const FrameTarget &target, RequestSlot &slot) {
	auto *nl = static_cast<const char *>(memchr(body, '\n', body_len));
	if (!nl) {
		throw InvalidInputException("n6k_server: OP_RPC_TABLE body missing header newline");
	}
	size_t header_len = static_cast<size_t>(nl - body);
	JsonDoc doc(body, header_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_RPC_TABLE header is not valid JSON");
	}
	auto *root = doc.Root();
	auto function = JsonGetStr(root, "function");
	if (function.empty()) {
		throw InvalidInputException("n6k_server: OP_RPC_TABLE requires a \"function\" field");
	}

	const uint8_t *arrow = reinterpret_cast<const uint8_t *>(body) + header_len + 1;
	size_t arrow_len = body_len - header_len - 1;

	// Resolved before the rows are staged: a host stream takes the Arrow bytes as they arrived, so
	// building a view it would only read back would be pure cost.
	auto rpc = ResolveRpcTarget(conn, catalog, RPC_SCHEMA, function);
	if (rpc.stream) {
		HandleRpcStream(reactor, conn, *rpc.stream, function, RpcArgsJson(duckdb_yyjson::yyjson_obj_get(root, "args")),
		                arrow, arrow_len, target, slot);
		return;
	}

	InsertArrowBytes bytes {arrow, arrow_len};
	auto rel = conn.TableFunction(N6K_INSERT_SCAN_FN, {Value::POINTER(reinterpret_cast<uintptr_t>(&bytes))});
	rel->CreateView(RPC_INPUT_VIEW, /*replace=*/true, /*temporary=*/true);

	const std::string table_arg = rpc.takes_table_arg ? "(SELECT * FROM " + QuoteIdent(RPC_INPUT_VIEW) + ")"
	                                                  : "'" + std::string(RPC_INPUT_VIEW) + "'";
	auto sql =
	    BuildRpcCallSql(rpc.catalog, RPC_SCHEMA, function, duckdb_yyjson::yyjson_obj_get(root, "args"), table_arg);
	const auto batch_rows = RequestedBatchRows(root);
	auto result = conn.SendQuery(sql);
	StreamResult(reactor, *result, *conn.context, target, slot, batch_rows);
}

void HandleRequest(ServeReactor &reactor, Connection &conn, const SessionRef &sess, uint8_t op, const char *body,
                   size_t body_len, uint32_t req_id, RequestSlot &slot) {
	const FrameTarget target {sess.ns, req_id};
	const std::string &catalog = sess.catalog;
	switch (op) {
	case OP_SCHEMA_LIST:
		HandleSchemaList(reactor, conn, catalog, target);
		break;
	case OP_TABLES_LIST:
		HandleTablesList(reactor, conn, sess, body, body_len, target);
		break;
	case OP_TABLE_SCHEMA:
		HandleTableSchema(reactor, conn, sess, body, body_len, target);
		break;
	case OP_QUERY:
		HandleQuery(reactor, conn, body, body_len, target, slot);
		break;
	case OP_READ_PLAN:
		HandleReadPlan(reactor, conn, sess, body, body_len, target, slot);
		break;
	case OP_WRITE_PLAN:
		HandleWritePlan(reactor, conn, sess, body, body_len, target, slot);
		break;
	case OP_TXN_BEGIN:
		HandleTxnBegin(reactor, sess, target);
		break;
	case OP_TXN_COMMIT:
		HandleTxnEnd(reactor, sess, body, body_len, target, true);
		break;
	case OP_TXN_ROLLBACK:
		HandleTxnEnd(reactor, sess, body, body_len, target, false);
		break;
	case OP_INSERT:
		HandleInsert(reactor, conn, catalog, body, body_len, target);
		break;
	case OP_EXEC:
		HandleExec(reactor, conn, body, body_len, target);
		break;
	case OP_CREATE_TABLE:
		HandleCreateTable(reactor, conn, catalog, body, body_len, target);
		break;
	case OP_ALTER_TABLE:
		HandleAlterTable(reactor, conn, catalog, body, body_len, target);
		break;
	case OP_CREATE_VIEW:
		HandleCreateView(reactor, conn, sess, body, body_len, target, slot);
		break;
	case OP_DROP_VIEW:
		HandleDropView(reactor, conn, sess, body, body_len, target);
		break;
	case OP_RPC_SCALAR:
		HandleRpcScalar(reactor, conn, catalog, body, body_len, target, slot);
		break;
	case OP_RPC_TABLE:
		HandleRpcTable(reactor, conn, catalog, body, body_len, target, slot);
		break;
	default:
		// Unimplemented ops throw a typed FT_ERR rather than dropping the request.
		throw NotImplementedException("n6k_server: op %d not implemented yet", static_cast<int>(op));
	}
}

} // namespace n6k
} // namespace duckdb
