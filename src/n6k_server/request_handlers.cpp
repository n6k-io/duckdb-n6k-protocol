#include "request_handlers.hpp"

#include "arrow_ipc_encode.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "frame_codec.hpp"
#include "insert_scan.hpp"
#include "n6k_catalog_list.hpp"
#include "n6k_permissions.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_sql_builder.hpp"
#include "n6k_sql_escape.hpp"
#include "crossing.hpp"
#include "crossing_substrait.hpp"
#include "serve_reactor.hpp"
#include "ws_json.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>

namespace duckdb {
namespace n6k {

// A request that names no schema resolves in `main`, where CREATE MACRO lands by default.
static constexpr const char *DEFAULT_RPC_SCHEMA = "main";

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

static void HandleSchemaList(ServeReactor &reactor, Connection &conn, const std::string &catalog, RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
	vector<string> schemas;
	// Direct catalog introspection needs an active transaction (unlike SendQuery).
	conn.context->RunFunctionInTransaction([&]() {
		auto &cat = Catalog::GetCatalog(*conn.context, Identifier(catalog));
		ListCatalogSchemas(*conn.context, cat, schemas);
	});

	reactor.SendForRequest(slot, PackSchemasChunk(target, schemas));
	reactor.EndRequest(slot, PackRespEnd(target));
}

static void PackStrings(msgpack::packer<msgpack::sbuffer> &pk, const std::vector<std::string> &strings) {
	pk.pack_array(static_cast<uint32_t>(strings.size()));
	for (auto &s : strings) {
		pk.pack(s);
	}
}

using RelationKey = std::pair<string, string>;

struct ListingComments {
	std::map<RelationKey, Value> relations;
	std::map<RelationKey, vector<std::pair<string, string>>> columns;
	std::map<string, Value> schemas;
};

static std::unique_ptr<QueryResult> QueryListing(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError();
	}
	return std::move(result);
}

static string ListingFilter(const string &catalog, optional_ptr<const string> schema_filter) {
	string where = " WHERE database_name = " + QuoteSqlLiteral(catalog);
	if (schema_filter) {
		where += " AND lower(schema_name) = lower(" + QuoteSqlLiteral(*schema_filter) + ")";
	}
	return where;
}

static ListingComments CollectListingComments(Connection &conn, const string &catalog,
                                              optional_ptr<const string> schema_filter) {
	ListingComments out;
	const auto where = ListingFilter(catalog, schema_filter);
	auto relations =
	    QueryListing(conn, "SELECT schema_name, table_name, comment FROM duckdb_tables()" + where +
	                           " UNION ALL SELECT schema_name, view_name, comment FROM duckdb_views()" + where);
	while (auto chunk = relations->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			out.relations[{StringValue::Get(chunk->GetValue(0, i)), StringValue::Get(chunk->GetValue(1, i))}] =
			    chunk->GetValue(2, i);
		}
	}
	auto columns = QueryListing(conn, "SELECT schema_name, table_name, column_name, comment FROM duckdb_columns()" +
	                                      where + " AND comment IS NOT NULL ORDER BY column_index");
	while (auto chunk = columns->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			out.columns[{StringValue::Get(chunk->GetValue(0, i)), StringValue::Get(chunk->GetValue(1, i))}]
			    .emplace_back(StringValue::Get(chunk->GetValue(2, i)), StringValue::Get(chunk->GetValue(3, i)));
		}
	}
	auto schemas = QueryListing(conn, "SELECT schema_name, comment FROM duckdb_schemas()" + where);
	while (auto chunk = schemas->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			out.schemas[StringValue::Get(chunk->GetValue(0, i))] = chunk->GetValue(1, i);
		}
	}
	return out;
}

static constexpr const char *FUNCTION_ROW_KEYS[] = {
    "schema", "name", "type", "description", "comment", "parameters", "parameter_types", "return_type", "examples"};

static vector<vector<Value>> CollectFunctionRows(Connection &conn, const string &catalog,
                                                 optional_ptr<const string> schema_filter) {
	auto result = QueryListing(conn, "SELECT schema_name, function_name, function_type, description, comment, "
	                                 "parameters, parameter_types, return_type, examples FROM duckdb_functions()" +
	                                     ListingFilter(catalog, schema_filter) +
	                                     " ORDER BY schema_name, function_name, function_oid");
	vector<vector<Value>> rows;
	while (auto chunk = result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			vector<Value> row;
			for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
				row.push_back(chunk->GetValue(c, i));
			}
			rows.push_back(std::move(row));
		}
	}
	return rows;
}

struct TableFunctionParamRow {
	string name;
	const char *kind;
	Value type;
	Value default_sql;
};

struct TableFunctionOverloadRow {
	vector<TableFunctionParamRow> params;
	vector<std::pair<string, string>> options;
};

struct TableFunctionRow {
	string schema;
	string name;
	const char *type;
	Value comment;
	Value description;
	vector<string> examples;
	vector<TableFunctionOverloadRow> overloads;
};

static const char *ParameterKindName(FunctionParameterKind kind) {
	switch (kind) {
	case FunctionParameterKind::POSITIONAL_ONLY:
		return "positional_only";
	case FunctionParameterKind::STANDARD:
		return "standard";
	case FunctionParameterKind::VAR_POSITIONAL:
		return "var_positional";
	case FunctionParameterKind::KEYWORD_ONLY:
		return "keyword_only";
	case FunctionParameterKind::VAR_KEYWORD:
		return "var_keyword";
	}
	throw InternalException("n6k_server: unknown FunctionParameterKind %d", static_cast<int>(kind));
}

static TableFunctionOverloadRow MacroOverloadRow(const MacroFunction &macro) {
	TableFunctionOverloadRow overload;
	for (idx_t i = 0; i < macro.parameters.size(); i++) {
		auto &param_name = macro.parameters[i]->Cast<ColumnRefExpression>().GetColumnName();
		TableFunctionParamRow param {param_name.GetIdentifierName(), "standard", Value(), Value()};
		if (i < macro.types.size() && macro.types[i].id() != LogicalTypeId::UNKNOWN) {
			param.type = Value(macro.types[i].ToString());
		}
		auto default_it = macro.default_parameters.find(param_name);
		if (default_it != macro.default_parameters.end()) {
			param.default_sql = Value(default_it->second->ToString());
		}
		overload.params.push_back(std::move(param));
	}
	return overload;
}

static TableFunctionOverloadRow TableFunctionOverloadOf(const FunctionSignature &signature) {
	TableFunctionOverloadRow overload;
	for (auto &parameter : signature.GetParameters()) {
		TableFunctionParamRow param {parameter.GetName().GetIdentifierName(), ParameterKindName(parameter.GetKind()),
		                             Value(parameter.GetType().ToString()), Value()};
		if (parameter.HasDefaultValue()) {
			param.default_sql = Value(parameter.GetDefaultValue()->ToSQLString());
		}
		overload.params.push_back(std::move(param));
	}
	if (auto options = signature.GetTypedKwargs()) {
		for (auto &option : options->GetOptions()) {
			overload.options.emplace_back(option.name.GetIdentifierName(), option.type.ToString());
		}
	}
	return overload;
}

// The entries duckdb_functions() reads for the catalog's table functions and table macros.
static vector<TableFunctionRow> CollectTableFunctionRows(ClientContext &context, Catalog &catalog,
                                                         optional_ptr<const string> schema_filter) {
	vector<TableFunctionRow> rows;
	catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
		const auto &schema_name = schema.name.GetIdentifierName();
		if (schema_filter && !StringUtil::CIEquals(schema_name, *schema_filter)) {
			return;
		}
		schema.Scan(context, CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			TableFunctionRow row;
			row.schema = schema_name;
			row.name = entry.name.GetIdentifierName();
			row.comment = entry.comment;
			auto &function_entry = entry.Cast<FunctionEntry>();
			if (!function_entry.descriptions.empty()) {
				auto &description = function_entry.descriptions[0];
				if (!description.description.empty()) {
					row.description = Value(description.description);
				}
				row.examples = description.examples;
			}
			if (entry.type == CatalogType::TABLE_MACRO_ENTRY) {
				row.type = "table_macro";
				for (auto &macro : entry.Cast<TableMacroCatalogEntry>().macros) {
					row.overloads.push_back(MacroOverloadRow(*macro));
				}
			} else if (entry.type == CatalogType::TABLE_FUNCTION_ENTRY) {
				row.type = "table";
				auto &functions = entry.Cast<TableFunctionCatalogEntry>().functions;
				for (idx_t i = 0; i < functions.Size(); i++) {
					row.overloads.push_back(TableFunctionOverloadOf(functions.GetFunctionByOffset(i)->GetSignature()));
				}
			} else {
				return;
			}
			rows.push_back(std::move(row));
		});
	});
	return rows;
}

// value must be NULL, VARCHAR or a (nested) list of those; any other type fails in StringValue::Get.
static void PackStringValue(msgpack::packer<msgpack::sbuffer> &pk, const Value &value) {
	if (value.IsNull()) {
		pk.pack_nil();
		return;
	}
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		pk.pack_array(static_cast<uint32_t>(children.size()));
		for (auto &child : children) {
			PackStringValue(pk, child);
		}
		return;
	}
	pk.pack(StringValue::Get(value));
}

template <class KEY>
static Value CommentOf(const std::map<KEY, Value> &comments, const KEY &key) {
	auto it = comments.find(key);
	return it == comments.end() ? Value() : it->second;
}

static void PackTableFunctionRows(msgpack::packer<msgpack::sbuffer> &pk, const vector<TableFunctionRow> &rows) {
	pk.pack_array(static_cast<uint32_t>(rows.size()));
	for (auto &row : rows) {
		pk.pack_map(7);
		PackString(pk, "schema");
		pk.pack(row.schema);
		PackString(pk, "name");
		pk.pack(row.name);
		PackString(pk, "type");
		PackString(pk, row.type);
		PackString(pk, "comment");
		PackStringValue(pk, row.comment.IsNull() ? row.comment : Value(row.comment.ToString()));
		PackString(pk, "description");
		PackStringValue(pk, row.description);
		PackString(pk, "examples");
		PackStrings(pk, row.examples);
		PackString(pk, "overloads");
		pk.pack_array(static_cast<uint32_t>(row.overloads.size()));
		for (auto &overload : row.overloads) {
			pk.pack_map(2);
			PackString(pk, "params");
			pk.pack_array(static_cast<uint32_t>(overload.params.size()));
			for (auto &param : overload.params) {
				pk.pack_map(4);
				PackString(pk, "name");
				pk.pack(param.name);
				PackString(pk, "kind");
				PackString(pk, param.kind);
				PackString(pk, "type");
				PackStringValue(pk, param.type);
				PackString(pk, "default");
				PackStringValue(pk, param.default_sql);
			}
			PackString(pk, "options");
			pk.pack_array(static_cast<uint32_t>(overload.options.size()));
			for (auto &option : overload.options) {
				pk.pack_map(2);
				PackString(pk, "name");
				pk.pack(option.first);
				PackString(pk, "type");
				pk.pack(option.second);
			}
		}
	}
}

static std::string PackTablesChunk(const FrameTarget &target, const vector<TablePermissionRow> &rows,
                                   const vector<SchemaPermissionRow> &schemas, const ListingComments &comments,
                                   const vector<vector<Value>> &functions,
                                   optional_ptr<const vector<TableFunctionRow>> table_functions) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(5 + (table_functions ? 1 : 0) + NsKeyCount(target));
	PackTIdInto(pk, FrameType::RESP_CHUNK, target);
	PackString(pk, "tables");
	pk.pack_array(static_cast<uint32_t>(rows.size()));
	for (auto &r : rows) {
		const RelationKey key {r.schema, r.name};
		pk.pack_map(9);
		PackString(pk, "schema");
		pk.pack(r.schema);
		PackString(pk, "name");
		pk.pack(r.name);
		PackString(pk, "verbs");
		PackStrings(pk, r.verbs);
		PackString(pk, "primary_keys");
		PackStrings(pk, r.primary_key);
		PackString(pk, "not_null");
		PackStrings(pk, r.not_null);
		PackString(pk, "row_ids");
		pk.pack(r.kind != "native_view");
		PackString(pk, "unique_keys");
		pk.pack_array(static_cast<uint32_t>(r.unique_keys.size()));
		for (auto &unique_key : r.unique_keys) {
			PackStrings(pk, unique_key);
		}
		PackString(pk, "comment");
		PackStringValue(pk, CommentOf(comments.relations, key));
		PackString(pk, "column_comments");
		auto columns = comments.columns.find(key);
		if (columns == comments.columns.end()) {
			pk.pack_map(0);
		} else {
			pk.pack_map(static_cast<uint32_t>(columns->second.size()));
			for (auto &column : columns->second) {
				pk.pack(column.first);
				pk.pack(column.second);
			}
		}
	}
	PackString(pk, "schemas");
	pk.pack_array(static_cast<uint32_t>(schemas.size()));
	for (auto &schema : schemas) {
		pk.pack_map(3);
		PackString(pk, "name");
		pk.pack(schema.name);
		PackString(pk, "verbs");
		PackStrings(pk, schema.verbs);
		PackString(pk, "comment");
		PackStringValue(pk, CommentOf(comments.schemas, schema.name));
	}
	PackString(pk, "functions");
	pk.pack_array(static_cast<uint32_t>(functions.size()));
	for (auto &function : functions) {
		pk.pack_map(static_cast<uint32_t>(function.size()));
		for (idx_t c = 0; c < function.size(); c++) {
			PackString(pk, FUNCTION_ROW_KEYS[c]);
			PackStringValue(pk, function[c]);
		}
	}
	if (table_functions) {
		PackString(pk, "table_functions");
		PackTableFunctionRows(pk, *table_functions);
	}
	return std::string(sb.data(), sb.size());
}

static void HandleTablesList(ServeReactor &reactor, Connection &conn, const SessionRef &sess, const char *body,
                             size_t body_len, RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
	string schema_filter_str;
	bool has_filter = false;
	bool want_table_functions = false;
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
		auto *table_functions_val = duckdb_yyjson::yyjson_obj_get(doc.Root(), "table_functions");
		if (table_functions_val) {
			if (!duckdb_yyjson::yyjson_is_bool(table_functions_val)) {
				throw InvalidInputException("n6k_server: OP_TABLES_LIST \"table_functions\" must be a boolean");
			}
			want_table_functions = duckdb_yyjson::yyjson_get_bool(table_functions_val);
		}
	}
	const std::string &catalog = sess.catalog;
	optional_ptr<const string> schema_filter = has_filter ? &schema_filter_str : nullptr;
	vector<TablePermissionRow> rows;
	vector<SchemaPermissionRow> schemas;
	vector<TableFunctionRow> table_functions;
	conn.context->RunFunctionInTransaction([&]() {
		auto &cat = Catalog::GetCatalog(*conn.context, Identifier(catalog));
		// Dispatching, not the plain collector: a bridge- or provider-backed catalog registers its own
		// and knows what the catalog metadata cannot express — whether a bridged table is writeable,
		// and the primary key that survived the hop. Falls back to plain classification otherwise.
		CollectCatalogPermissions(*conn.context, cat, schema_filter, nullptr, rows);
		CollectSchemaPermissions(*conn.context, cat, schema_filter, schemas);
		if (want_table_functions) {
			table_functions = CollectTableFunctionRows(*conn.context, cat, schema_filter);
		}
	});
	auto comments = CollectListingComments(conn, catalog, schema_filter);
	auto functions = CollectFunctionRows(conn, catalog, schema_filter);
	reactor.SendForRequest(slot, PackTablesChunk(target, rows, schemas, comments, functions,
	                                             want_table_functions ? &table_functions : nullptr));
	reactor.EndRequest(slot, PackRespEnd(target));
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

// A query result sent as credited Arrow chunks, one frame at a time, so it can park between any two frames.
class SqlResultStream final : public ParkedStream {
public:
	SqlResultStream(std::unique_ptr<Connection> conn_p, const FrameTarget &target, idx_t batch_rows)
	    : conn(std::move(conn_p)), target_(target), batch_rows_(batch_rows) {
	}

	void Start(ServeReactor &reactor, RequestSlot &slot, unique_ptr<QueryResult> result, bool materialized = false) {
		if (result->HasError()) {
			result->ThrowError();
		}
		stream_ = make_uniq<QueryResultStream<ChunkFormat>>(std::move(result));
		// A statement that expands into several has no schema until the one producing the result runs.
		if (!stream_->MetadataAvailable()) {
			chunk_ = FetchFromStream();
		}
		enc_ = make_uniq<ArrowIpcStreamEncoder>(*conn->context, stream_->GetTypes(), stream_->GetNames());
		reactor.SendForRequest(slot, PackRespSchema(target_, enc_->SchemaMessage()));
	}

	PumpResult Pump(ServeReactor &reactor, RequestSlot &slot) override {
		while (true) {
			if (!chunk_) {
				chunk_ = FetchFromStream();
				if (!chunk_) {
					auto eos = enc_->TakeEndOfStreamMessage();
					if (!eos.empty()) {
						reactor.SendForRequest(slot, PackRespChunkArrow(target_, eos));
					}
					reactor.EndRequest(slot, PackRespEndRowcountNull(target_));
					return PumpResult::DONE;
				}
				offset_ = 0;
				if (chunk_->size() == 0) {
					chunk_.reset();
					continue;
				}
			}
			switch (reactor.TryAcquireCredit(slot)) {
			case ServeReactor::CreditResult::STOPPED:
				Cancel(reactor, slot);
				return PumpResult::DONE;
			case ServeReactor::CreditResult::EMPTY:
				return PumpResult::PARKED;
			case ServeReactor::CreditResult::TAKEN:
				break;
			}
			SendNextFrame(reactor, slot);
			if (reactor.HasWaitingRequests()) {
				return PumpResult::YIELD;
			}
		}
	}

	void Cancel(ServeReactor &reactor, RequestSlot &slot) override {
		reactor.EndRequest(slot, PackRespEndCancelled(target_));
	}

	// Declared before `enc_`, which holds a reference to its context.
	std::unique_ptr<Connection> conn;
	// What the result may still read while it streams: OP_RPC_TABLE's staged input. Declared before `stream_` so it
	// outlives it.
	std::string input;
	InsertArrowBytes input_bytes {nullptr, 0};

private:
	unique_ptr<DataChunk> FetchFromStream() {
		auto chunk = stream_->Fetch();
		if (!chunk) {
			// Null also means an execution error, which the stream records rather than throws.
			if (stream_->HasError()) {
				stream_->GetErrorObject().Throw();
			}
			return chunk;
		}
		chunk->Flatten();
		return chunk;
	}

	void SendNextFrame(ServeReactor &reactor, RequestSlot &slot) {
		auto &chunk = *chunk_;
		if (batch_rows_ == 0 || chunk.size() <= batch_rows_) {
			reactor.SendForRequest(slot, PackRespChunkArrow(target_, enc_->EncodeChunk(chunk)));
			chunk_.reset();
			return;
		}
		// A real copy at ordinary vector capacity: slicing `chunk` as a view hands the Arrow encoder
		// dictionary vectors borrowing the parent's buffers, and sizing the destination to a small
		// `batch_rows` gives it a tiny capacity — the encoder survives neither.
		//
		// DataChunk::Copy takes the range as [offset, source_count) over `sel` and requires the
		// destination empty and flat, hence the Reset each round.
		if (offset_ == 0) {
			sel_.Initialize(chunk.size());
			for (idx_t i = 0; i < chunk.size(); i++) {
				sel_.set_index(i, i);
			}
			if (slice_.ColumnCount() == 0) {
				slice_.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
			}
		}
		const idx_t end = MinValue<idx_t>(offset_ + batch_rows_, chunk.size());
		slice_.Reset();
		chunk.Copy(slice_, sel_, end, offset_);
		reactor.SendForRequest(slot, PackRespChunkArrow(target_, enc_->EncodeChunk(slice_)));
		offset_ = end;
		if (offset_ >= chunk.size()) {
			chunk_.reset();
		}
	}

	FrameTarget target_;
	idx_t batch_rows_;
	unique_ptr<QueryResultStream<ChunkFormat>> stream_;
	unique_ptr<ArrowIpcStreamEncoder> enc_;
	unique_ptr<DataChunk> chunk_;
	idx_t offset_ = 0;
	SelectionVector sel_;
	DataChunk slice_;
};

static std::unique_ptr<ParkedStream> StreamQuery(ServeReactor &reactor, std::unique_ptr<Connection> conn,
                                                 const std::string &sql, RequestSlot &slot, idx_t batch_rows) {
	const FrameTarget target {slot.ns, slot.req_id};
	std::unique_ptr<SqlResultStream> stream(new SqlResultStream(std::move(conn), target, batch_rows));
	stream->Start(reactor, slot, stream->conn->Submit(sql));
	return std::move(stream);
}

// Raw passthrough SQL: not catalog-qualified, so it can reach other catalogs.
static std::unique_ptr<ParkedStream> HandleQuery(ServeReactor &reactor, std::unique_ptr<Connection> &conn,
                                                 const char *body, size_t body_len, RequestSlot &slot) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_QUERY body is not valid JSON");
	}
	auto *root = doc.Root();
	auto sql = JsonGetStr(root, "sql");
	if (sql.empty()) {
		throw InvalidInputException("n6k_server: OP_QUERY requires a \"sql\" field");
	}
	return StreamQuery(reactor, std::move(conn), sql, slot, RequestedBatchRows(root));
}

static void HandleTableSchema(ServeReactor &reactor, Connection &conn, const SessionRef &sess, const char *body,
                              size_t body_len, RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_TABLE_SCHEMA body is not valid JSON");
	}
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
	ArrowIpcStreamEncoder enc(*conn.context, result->GetTypes(), result->GetNames());
	reactor.SendForRequest(slot, PackRespSchema(target, enc.SchemaMessage()));
	reactor.EndRequest(slot, PackRespEnd(target));
}

// The result's first cell is the affected-row count (absent → 0).
static void HandleExec(ServeReactor &reactor, Connection &conn, const char *body, size_t body_len, RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
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
		auto value = result->Fetch()->GetValue(0, 0);
		if (!value.IsNull()) {
			rowcount = value.GetValue<int64_t>();
		}
	}
	reactor.EndRequest(slot, PackRespEndRowcount(target, rowcount));
}

static void HandleCreateSchema(ServeReactor &reactor, Connection &conn, const SessionRef &sess, const char *body,
                               size_t body_len, RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_CREATE_SCHEMA body is not valid JSON");
	}
	auto *root = doc.Root();
	auto schema = JsonGetStr(root, "schema");
	if (schema.empty()) {
		throw InvalidInputException("n6k_server: OP_CREATE_SCHEMA requires \"schema\"");
	}
	auto sql = BuildCreateSchemaSql(sess.catalog, schema, JsonGetStr(root, "on_conflict"));
	auto &context = *conn.context;
	bool grants_create = false;
	context.RunFunctionInTransaction([&]() {
		grants_create = CatalogGrantsCreate(context, Catalog::GetCatalog(context, Identifier(sess.catalog)));
	});
	if (!grants_create) {
		throw PermissionException("n6k_server: catalog '%s' does not allow CREATE SCHEMA", sess.catalog);
	}
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError();
	}
	reactor.EndRequest(slot, PackRespEndOk(target));
}

static bool DeclaredTypeTakes(const LogicalType &declared, const LogicalType &actual) {
	if (declared.id() == LogicalTypeId::ANY || declared.id() == LogicalTypeId::TEMPLATE) {
		return true;
	}
	if (declared.id() != actual.id()) {
		return false;
	}
	// A bare STRUCT, ARRAY or similar declares no children, and reading them would be undefined.
	if (!declared.HasParameters()) {
		return true;
	}
	switch (declared.id()) {
	case LogicalTypeId::LIST:
		return DeclaredTypeTakes(ListType::GetChildType(declared), ListType::GetChildType(actual));
	case LogicalTypeId::ARRAY:
		return DeclaredTypeTakes(ArrayType::GetChildType(declared), ArrayType::GetChildType(actual));
	case LogicalTypeId::MAP:
		return DeclaredTypeTakes(MapType::KeyType(declared), MapType::KeyType(actual)) &&
		       DeclaredTypeTakes(MapType::ValueType(declared), MapType::ValueType(actual));
	case LogicalTypeId::STRUCT: {
		auto count = StructType::GetChildCount(declared);
		if (count == 0) {
			return true;
		}
		if (count != StructType::GetChildCount(actual)) {
			return false;
		}
		for (idx_t i = 0; i < count; i++) {
			if (!DeclaredTypeTakes(StructType::GetChildType(declared, i), StructType::GetChildType(actual, i))) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

static bool SignatureTakes(const FunctionSignature &signature, const vector<LogicalType> &actual) {
	idx_t next = 0;
	for (auto &param : signature.GetParameters()) {
		if (param.IsVariadic()) {
			continue;
		}
		if (next < actual.size() && param.AcceptsPosition()) {
			if (!DeclaredTypeTakes(param.GetType(), actual[next])) {
				return false;
			}
			next++;
		} else if (!param.HasDefaultValue()) {
			return false;
		}
	}
	auto variadic = signature.GetArgs();
	if (!variadic) {
		variadic = signature.GetKwargs();
	}
	for (; next < actual.size(); next++) {
		if (!variadic || !DeclaredTypeTakes(variadic->GetType(), actual[next])) {
			return false;
		}
	}
	return true;
}

template <class ENTRY>
static bool AnyOverloadTakes(ENTRY &entry, const vector<LogicalType> &actual) {
	for (idx_t i = 0; i < entry.functions.Size(); i++) {
		if (SignatureTakes(entry.functions.GetFunctionByOffset(i)->GetSignature(), actual)) {
			return true;
		}
	}
	return false;
}

// Only the system catalog: looking further would ask every attached catalog, and one could be an unreachable upstream.
// Scalar and aggregate functions share a lookup, so asking for one can return the other.
static bool SystemFunctionTakes(ClientContext &context, const string &name, const vector<LogicalType> &actual) {
	QualifiedName qualified(Identifier(string(SYSTEM_CATALOG)), Identifier(string(DEFAULT_SCHEMA)), Identifier(name));
	EntryLookupInfo lookup(CatalogType::SCALAR_FUNCTION_ENTRY, qualified, QueryErrorContext());
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return false;
	}
	switch (entry->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return AnyOverloadTakes(entry->Cast<ScalarFunctionCatalogEntry>(), actual);
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		return AnyOverloadTakes(entry->Cast<AggregateFunctionCatalogEntry>(), actual);
	default:
		return false;
	}
}

static void HandleHasFunction(ServeReactor &reactor, Connection &conn, const char *body, size_t body_len,
                              RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_HAS_FUNCTION body is not valid JSON");
	}
	auto *root = doc.Root();
	auto name = JsonGetStr(root, "name");
	if (name.empty() || !duckdb_yyjson::yyjson_is_arr(duckdb_yyjson::yyjson_obj_get(root, "args"))) {
		throw InvalidInputException("n6k_server: OP_HAS_FUNCTION requires \"name\" and \"args\"");
	}
	auto arg_names = JsonGetStrArray(root, "args");
	bool exists = false;
	conn.context->RunFunctionInTransaction([&]() {
		vector<LogicalType> actual;
		for (auto &arg : arg_names) {
			try {
				actual.push_back(TransformStringToLogicalType(arg, *conn.context));
			} catch (const std::exception &) {
				// A type this server cannot name is a type no function here takes.
				return;
			}
		}
		exists = SystemFunctionTakes(*conn.context, name, actual);
	});
	reactor.EndRequest(slot, PackRespEndExists(target, exists));
}

static void HandleInsert(ServeReactor &reactor, Connection &conn, const std::string &catalog, const char *body,
                         size_t body_len, RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
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
	rel->Insert(Identifier(catalog), Identifier(schema), Identifier(table));

	reactor.EndRequest(slot, PackRespEndRowcount(target, rowcount));
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
		auto &signature = fn->GetSignature();
		for (idx_t i = 0; i < signature.GetParameterCount(); i++) {
			if (signature.GetParameter(i).GetType().id() == LogicalTypeId::TABLE) {
				return true;
			}
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
		(void)Catalog::GetCatalog(*conn.context, Identifier(catalog));
		EntryLookupInfo local_lookup(CatalogType::TABLE_FUNCTION_ENTRY,
		                             QualifiedName(Identifier(catalog), Identifier(schema), Identifier(function)));
		if (auto local = Catalog::GetEntry(*conn.context, local_lookup, OnEntryNotFound::RETURN_NULL)) {
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
		EntryLookupInfo global_lookup(CatalogType::TABLE_FUNCTION_ENTRY,
		                              QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA, Identifier(function)));
		auto global_fn = Catalog::GetEntry(*conn.context, global_lookup, OnEntryNotFound::RETURN_NULL);
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

struct RpcCall {
	std::string function;
	std::string schema;
	duckdb_yyjson::yyjson_val *args = nullptr;
	duckdb_yyjson::yyjson_val *named = nullptr;
};

static RpcCall ParseRpcCall(duckdb_yyjson::yyjson_val *root, const char *op_name) {
	RpcCall call;
	call.function = JsonGetStr(root, "function");
	if (call.function.empty()) {
		throw InvalidInputException("n6k_server: %s requires a \"function\" field", op_name);
	}
	call.schema = JsonGetStr(root, "schema");
	if (call.schema.empty()) {
		call.schema = DEFAULT_RPC_SCHEMA;
	}
	call.args = duckdb_yyjson::yyjson_obj_get(root, "args");
	call.named = duckdb_yyjson::yyjson_obj_get(root, "named");
	if (call.named && !duckdb_yyjson::yyjson_is_obj(call.named)) {
		throw InvalidInputException("n6k_server: rpc \"named\" must be an object");
	}
	if (call.named && duckdb_yyjson::yyjson_obj_size(call.named) == 0) {
		call.named = nullptr;
	}
	return call;
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
	RpcStreamCloser(Connection &conn, std::string close_udf, std::string handle)
	    : conn(conn), close_udf(std::move(close_udf)), handle(std::move(handle)) {
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
	std::string close_udf;
	std::string handle;
	bool closed = false;
};

// Both RPC ops, when the name belongs to the host rather than the catalog: drive the host's
// generator one batch at a time and forward each as a RESP_CHUNK.
//
// Pull a batch, THEN wait for credit, so a generator that paces itself in wall-clock time keeps its
// cadence instead of starting its next interval only once the client's credit arrives.
class HostRpcStream final : public ParkedStream {
public:
	HostRpcStream(std::unique_ptr<Connection> conn, const StreamFunctionInfo &rpc, const std::string &handle,
	              const FrameTarget &target)
	    : conn_(std::move(conn)), next_udf_(rpc.next_udf), handle_(handle), target_(target),
	      closer_(*conn_, rpc.close_udf, handle) {
	}

	void Start(ServeReactor &reactor, RequestSlot &slot, const std::string &schema_message) {
		reactor.SendForRequest(slot, PackRespSchema(target_, schema_message));
		next_stmt_ = conn_->Prepare("SELECT " + QuoteIdent(next_udf_) + "(?)");
		if (next_stmt_->HasError()) {
			next_stmt_->GetErrorObject().Throw("n6k_server: RPC next function \"" + next_udf_ + "\" is unusable: ");
		}
	}

	PumpResult Pump(ServeReactor &reactor, RequestSlot &slot) override {
		while (true) {
			if (!has_batch_) {
				vector<Value> next_params;
				next_params.emplace_back(handle_);
				auto next_result = next_stmt_->Execute(next_params);
				if (next_result->HasError()) {
					next_result->ThrowError();
				}
				auto chunk = next_result->Fetch();
				if (!chunk || chunk->size() == 0 || chunk->GetValue(0, 0).IsNull()) {
					auto trailing = closer_.CloseAndTakeTrailingBytes();
					if (!trailing.empty()) {
						reactor.SendForRequest(slot, PackRespChunkArrow(target_, trailing));
					}
					reactor.EndRequest(slot, PackRespEndRowcountNull(target_));
					return PumpResult::DONE;
				}
				batch_ = chunk->GetValue(0, 0);
				has_batch_ = true;
			}
			switch (reactor.TryAcquireCredit(slot)) {
			case ServeReactor::CreditResult::STOPPED:
				Cancel(reactor, slot);
				return PumpResult::DONE;
			case ServeReactor::CreditResult::EMPTY:
				return PumpResult::PARKED;
			case ServeReactor::CreditResult::TAKEN:
				break;
			}
			reactor.SendForRequest(slot, PackRespChunkArrow(target_, StringValue::Get(batch_)));
			has_batch_ = false;
			batch_ = Value();
			if (reactor.HasWaitingRequests()) {
				return PumpResult::YIELD;
			}
		}
	}

	// No trailing end-of-stream bytes on a cancelled stream.
	void Cancel(ServeReactor &reactor, RequestSlot &slot) override {
		closer_.CloseAndTakeTrailingBytes();
		reactor.EndRequest(slot, PackRespEndCancelled(target_));
	}

private:
	// In this order so the closer runs before its statement and Connection go.
	std::unique_ptr<Connection> conn_;
	std::string next_udf_;
	std::string handle_;
	FrameTarget target_;
	unique_ptr<PreparedStatement> next_stmt_;
	Value batch_;
	bool has_batch_ = false;
	RpcStreamCloser closer_;
};

// The catalog entry is only used to find the UDF names: binding it as SQL would decode the host's
// Arrow blobs into vectors and immediately re-encode them, and would buffer where this does not.
// `SELECT * FROM db.<schema>.<name>(...)` takes the bound path instead, inside vcatprovider.
static std::unique_ptr<ParkedStream> StartRpcStream(ServeReactor &reactor, std::unique_ptr<Connection> conn,
                                                    const StreamFunctionInfo &rpc, const std::string &function,
                                                    const std::string &args_json, const uint8_t *input,
                                                    size_t input_len, RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
	// Only ever a bound parameter, so a counter suffices to keep concurrent calls apart.
	static std::atomic<uint64_t> handle_counter {0};
	const std::string handle = "h" + std::to_string(handle_counter.fetch_add(1) + 1);

	auto open_stmt = conn->Prepare("SELECT " + QuoteIdent(rpc.open_udf) + "(?, ?, ?, ?)");
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
	std::unique_ptr<HostRpcStream> stream(new HostRpcStream(std::move(conn), rpc, handle, target));
	const auto &schema_message = StringValue::Get(schema_value);
	// Empty is rejected rather than forwarded: RESP_SCHEMA is what tells the client how to read
	// every chunk that follows, and an empty one fails later and further away.
	if (schema_message.empty()) {
		throw IOException("n6k_server: RPC \"%s\" returned an empty Arrow schema when opened", function);
	}
	stream->Start(reactor, slot, schema_message);
	return std::move(stream);
}

// "Scalar" describes the ARGUMENTS, not the return: both RPC ops answer with an Arrow stream. The
// name resolves through ordinary DuckDB resolution rather than a registry, so what is callable is
// whatever the host put in the served catalog or registered on the instance.
static std::unique_ptr<ParkedStream> HandleRpcScalar(ServeReactor &reactor, std::unique_ptr<Connection> &conn,
                                                     const std::string &catalog, const char *body, size_t body_len,
                                                     RequestSlot &slot) {
	JsonDoc doc(body, body_len);
	if (!doc.Parsed()) {
		throw InvalidInputException("n6k_server: OP_RPC_SCALAR body is not valid JSON");
	}
	auto *root = doc.Root();
	auto call = ParseRpcCall(root, "OP_RPC_SCALAR");
	auto rpc = ResolveRpcTarget(*conn, catalog, call.schema, call.function);
	// The host's generator takes positional args only; a named call binds through vcatprovider instead.
	if (rpc.stream && !call.named) {
		return StartRpcStream(reactor, std::move(conn), *rpc.stream, call.function, RpcArgsJson(call.args), nullptr, 0,
		                      slot);
	}
	auto sql = BuildRpcCallSql(rpc.catalog, call.schema, call.function, call.args, call.named);
	return StreamQuery(reactor, std::move(conn), sql, slot, RequestedBatchRows(root));
}

// Like RPC_SCALAR, plus a table of rows pushed in the request body.
//
// The rows arrive as Arrow IPC after the header's newline exactly as OP_INSERT's do, so they are
// staged through the same private scan function and exposed as a temp view. Every request builds
// its own Connection and therefore its own temp catalog, so the fixed name cannot collide. The view
// scans the rows while the result streams, so the stream owns a copy: the request body is gone once
// it parks.
static std::unique_ptr<ParkedStream> HandleRpcTable(ServeReactor &reactor, std::unique_ptr<Connection> &conn,
                                                    const std::string &catalog, const char *body, size_t body_len,
                                                    RequestSlot &slot) {
	const FrameTarget target {slot.ns, slot.req_id};
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
	auto call = ParseRpcCall(root, "OP_RPC_TABLE");

	const uint8_t *arrow = reinterpret_cast<const uint8_t *>(body) + header_len + 1;
	size_t arrow_len = body_len - header_len - 1;

	// Resolved before the rows are staged: a host stream takes the Arrow bytes as they arrived, so
	// building a view it would only read back would be pure cost.
	auto rpc = ResolveRpcTarget(*conn, catalog, call.schema, call.function);
	if (rpc.stream && !call.named) {
		return StartRpcStream(reactor, std::move(conn), *rpc.stream, call.function, RpcArgsJson(call.args), arrow,
		                      arrow_len, slot);
	}

	std::unique_ptr<SqlResultStream> stream(new SqlResultStream(std::move(conn), target, RequestedBatchRows(root)));
	stream->input.assign(reinterpret_cast<const char *>(arrow), arrow_len);
	stream->input_bytes = InsertArrowBytes {reinterpret_cast<const uint8_t *>(stream->input.data()), arrow_len};
	auto rel = stream->conn->TableFunction(N6K_INSERT_SCAN_FN,
	                                       {Value::POINTER(reinterpret_cast<uintptr_t>(&stream->input_bytes))});
	rel->CreateView(RPC_INPUT_VIEW, /*replace=*/true, /*temporary=*/true);

	const std::string table_arg = rpc.takes_table_arg ? "(SELECT * FROM " + QuoteIdent(RPC_INPUT_VIEW) + ")"
	                                                  : "'" + std::string(RPC_INPUT_VIEW) + "'";
	auto sql = BuildRpcCallSql(rpc.catalog, call.schema, call.function, call.args, call.named, table_arg);
	stream->Start(reactor, slot, stream->conn->Submit(sql));
	return std::move(stream);
}

std::unique_ptr<ParkedStream> HandleRequest(ServeReactor &reactor, std::unique_ptr<Connection> &own,
                                            const SessionRef &sess, uint8_t op, const char *body, size_t body_len,
                                            RequestSlot &slot) {
	const std::string &catalog = sess.catalog;
	auto &conn = *own;
	switch (op) {
	case OP_SCHEMA_LIST:
		HandleSchemaList(reactor, conn, catalog, slot);
		break;
	case OP_TABLES_LIST:
		HandleTablesList(reactor, conn, sess, body, body_len, slot);
		break;
	case OP_TABLE_SCHEMA:
		HandleTableSchema(reactor, conn, sess, body, body_len, slot);
		break;
	case OP_QUERY:
		return HandleQuery(reactor, own, body, body_len, slot);
	case OP_INSERT:
		HandleInsert(reactor, conn, catalog, body, body_len, slot);
		break;
	case OP_EXEC:
		HandleExec(reactor, conn, body, body_len, slot);
		break;
	case OP_CREATE_SCHEMA:
		HandleCreateSchema(reactor, conn, sess, body, body_len, slot);
		break;
	case OP_HAS_FUNCTION:
		HandleHasFunction(reactor, conn, body, body_len, slot);
		break;
	case OP_RPC_SCALAR:
		return HandleRpcScalar(reactor, own, catalog, body, body_len, slot);
	case OP_RPC_TABLE:
		return HandleRpcTable(reactor, own, catalog, body, body_len, slot);
	default:
		// Unimplemented ops throw a typed FT_ERR rather than dropping the request.
		throw NotImplementedException("n6k_server: op %d not implemented yet", static_cast<int>(op));
	}
	return nullptr;
}

} // namespace n6k
} // namespace duckdb
