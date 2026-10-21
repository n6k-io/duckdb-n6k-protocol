#include "n6k_rpc_function.hpp"
#include "n6k_arrow_stream.hpp"
#include "n6k_catalog.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_fetch.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/string_util.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

static yyjson_mut_val *RpcValueToYYJSON(yyjson_mut_doc *doc, const Value &val) {
	if (val.IsNull()) {
		return yyjson_mut_null(doc);
	}
	auto &type = val.type();
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return yyjson_mut_bool(doc, val.GetValue<bool>());
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return yyjson_mut_sint(doc, val.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return yyjson_mut_uint(doc, val.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return yyjson_mut_real(doc, val.GetValue<double>());
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT: {
		// emit text raw: valid JSON number; avoids double precision loss for wide decimals / 128-bit ints
		auto str = val.ToString();
		return yyjson_mut_rawncpy(doc, str.c_str(), str.size());
	}
	case LogicalTypeId::STRUCT: {
		auto obj = yyjson_mut_obj(doc);
		auto &children = StructValue::GetChildren(val);
		auto &struct_type = val.type();
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child_name = StructType::GetChildName(struct_type, i);
			auto key = yyjson_mut_strncpy(doc, child_name.c_str(), child_name.size());
			auto child_val = RpcValueToYYJSON(doc, children[i]);
			yyjson_mut_obj_add(obj, key, child_val);
		}
		return obj;
	}
	default: {
		auto str = val.ToString();
		return yyjson_mut_strncpy(doc, str.c_str(), str.size());
	}
	}
}

static string RpcValueToJSON(const Value &val) {
	auto *doc = yyjson_mut_doc_new(nullptr);
	auto *root = RpcValueToYYJSON(doc, val);
	yyjson_mut_doc_set_root(doc, root);
	size_t len = 0;
	auto *json = yyjson_mut_write(doc, 0, &len);
	string result(json, len);
	free(json);
	yyjson_mut_doc_free(doc);
	return result;
}

static std::shared_ptr<CatalogSession> GetCatalogSession(ClientContext &context, const string &db_name) {
	auto &catalog = Catalog::GetCatalog(context, db_name);
	return catalog.Cast<N6kCatalog>().session;
}

static string RpcArgsJson(const vector<Value> &inputs, idx_t first) {
	string args_json = "[";
	for (idx_t i = first; i < inputs.size(); i++) {
		if (i > first) {
			args_json += ",";
		}
		args_json += RpcValueToJSON(inputs[i]);
	}
	args_json += "]";
	return args_json;
}

static unique_ptr<FunctionData> ScanDataOverStream(ClientContext &context, unique_ptr<N6kStreamData> stream_data,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<N6kScanFunctionData>(reinterpret_cast<stream_factory_produce_t>(N6kProduceStream),
	                                             std::move(stream_data));
	auto *schema_ptr = result->owned_stream_data.get();
	if (schema_ptr->stream.get_schema(&schema_ptr->stream, &result->schema_root.arrow_schema) != 0) {
		const char *err = schema_ptr->stream.get_last_error(&schema_ptr->stream);
		throw IOException("n6k: failed to read result schema: %s", err ? err : "unknown error");
	}
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	return std::move(result);
}

static unique_ptr<FunctionData> BindRpcScalar(ClientContext &context, CatalogSession &session,
                                              const string &function_name, const string &args_json,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto stream_data = make_uniq<N6kStreamData>();
	session.RpcScalar(function_name, args_json, &stream_data->stream, [&context]() { return context.IsInterrupted(); });
	return ScanDataOverStream(context, std::move(stream_data), return_types, names);
}

static unique_ptr<FunctionData> BindQuery(ClientContext &context, CatalogSession &session, const string &sql,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto stream_data = make_uniq<N6kStreamData>();
	session.Query(sql, &stream_data->stream, [&context]() { return context.IsInterrupted(); });
	return ScanDataOverStream(context, std::move(stream_data), return_types, names);
}

static unique_ptr<FunctionData> N6kRpcBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto function_name = input.inputs[1].GetValue<string>();
	auto session = GetCatalogSession(context, db_name);
	return BindRpcScalar(context, *session, function_name, RpcArgsJson(input.inputs, 2), return_types, names);
}

struct N6kRpcArrowBindData : public FunctionData {
	string function_name;
	std::shared_ptr<CatalogSession> session;
	vector<string> scalar_parts;
	vector<LogicalType> input_types;
	vector<string> input_names;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<N6kRpcArrowBindData>();
		copy->function_name = function_name;
		copy->session = session;
		copy->scalar_parts = scalar_parts;
		copy->input_types = input_types;
		copy->input_names = input_names;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<N6kRpcArrowBindData>();
		return function_name == other.function_name && scalar_parts == other.scalar_parts;
	}
};

struct N6kRpcArrowLocalState : public LocalTableFunctionState {
	vector<unique_ptr<DataChunk>> buffered_chunks;
	bool sent = false;
	ArrowArrayStream response_stream;
};

struct N6kRpcArrowGlobalState : public GlobalTableFunctionState {
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> N6kRpcArrowBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto fn_name = input.inputs[1].GetValue<string>();
	auto session = GetCatalogSession(context, db_name);

	auto bind_data = make_uniq<N6kRpcArrowBindData>();
	bind_data->function_name = fn_name;
	bind_data->session = session;
	bind_data->input_types = input.input_table_types;
	bind_data->input_names = input.input_table_names;

	for (idx_t i = 3; i < input.inputs.size(); i++) {
		bind_data->scalar_parts.push_back(RpcValueToJSON(input.inputs[i]));
	}

	// Probe the server for the result schema, which DuckDB needs before the real call can execute.
	// The probe must be the REAL call — the actual scalar args plus a schema-only (zero-batch) Arrow
	// body describing the declared input relation — because a function resolved through DuckDB has to
	// bind, and `fn()` does not bind when `fn` takes arguments. The server binds it, answers
	// RESP_SCHEMA and streams no rows.
	string probe_args = RpcArgsJson(input.inputs, 3);

	ArrowBuffer probe_buf;
	ArrowBufferInit(&probe_buf);
	vector<unique_ptr<DataChunk>> no_chunks;
	SerializeChunksToArrowIPC(context, bind_data->input_types, bind_data->input_names, no_chunks, &probe_buf);

	auto stream_data = make_uniq<N6kStreamData>();
	session->RpcTable(fn_name, probe_args, static_cast<const uint8_t *>(probe_buf.data),
	                  static_cast<size_t>(probe_buf.size_bytes), &stream_data->stream,
	                  [&context]() { return context.IsInterrupted(); });
	ArrowBufferReset(&probe_buf);

	ArrowSchema response_schema;
	if (stream_data->stream.get_schema(&stream_data->stream, &response_schema) != 0) {
		if (stream_data->stream.release) {
			stream_data->stream.release(&stream_data->stream);
		}
		throw IOException("n6k_catalog_rpc_table: failed to get response schema for %s", fn_name);
	}

	for (int i = 0; i < response_schema.n_children; i++) {
		names.push_back(response_schema.children[i]->name);
	}

	auto scan_data = make_uniq<N6kScanFunctionData>(reinterpret_cast<stream_factory_produce_t>(N6kProduceStream),
	                                                std::move(stream_data));
	ArrowTableFunction::PopulateArrowTableSchema(context, scan_data->arrow_table, response_schema);
	return_types = scan_data->arrow_table.GetTypes();

	if (response_schema.release) {
		response_schema.release(&response_schema);
	}

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> N6kRpcArrowInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<N6kRpcArrowGlobalState>();
}

static unique_ptr<LocalTableFunctionState>
N6kRpcArrowInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
	auto result = make_uniq<N6kRpcArrowLocalState>();
	result->response_stream.release = nullptr;
	return std::move(result);
}

static OperatorResultType N6kRpcArrowInOut(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                           DataChunk &output) {
	auto &lstate = data_p.local_state->Cast<N6kRpcArrowLocalState>();
	if (input.size() > 0) {
		auto copy = make_uniq<DataChunk>();
		copy->Initialize(Allocator::DefaultAllocator(), input.GetTypes());
		input.Copy(*copy);
		lstate.buffered_chunks.push_back(std::move(copy));
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

static OperatorFinalizeResultType N6kRpcArrowFinal(ExecutionContext &context, TableFunctionInput &data_p,
                                                   DataChunk &output) {
	auto &lstate = data_p.local_state->Cast<N6kRpcArrowLocalState>();
	auto &bind_data = data_p.bind_data->Cast<N6kRpcArrowBindData>();

	if (!lstate.sent) {
		ArrowBuffer ipc_buf;
		ArrowBufferInit(&ipc_buf);
		SerializeChunksToArrowIPC(context.client, bind_data.input_types, bind_data.input_names, lstate.buffered_chunks,
		                          &ipc_buf);

		string args_json = "[";
		bool first = true;
		for (auto &json_str : bind_data.scalar_parts) {
			if (!first) {
				args_json += ",";
			}
			args_json += json_str;
			first = false;
		}
		args_json += "]";
		bind_data.session->RpcTable(bind_data.function_name, args_json, static_cast<const uint8_t *>(ipc_buf.data),
		                            static_cast<size_t>(ipc_buf.size_bytes), &lstate.response_stream,
		                            [&context]() { return context.client.IsInterrupted(); });

		ArrowBufferReset(&ipc_buf);
		lstate.buffered_chunks.clear();
		lstate.sent = true;
	}

	ArrowArray batch;
	batch.release = nullptr;
	if (lstate.response_stream.get_next(&lstate.response_stream, &batch) != 0 || batch.release == nullptr) {
		if (lstate.response_stream.release) {
			lstate.response_stream.release(&lstate.response_stream);
			lstate.response_stream.release = nullptr;
		}
		return OperatorFinalizeResultType::FINISHED;
	}

	ArrowSchema response_schema;
	if (lstate.response_stream.get_schema(&lstate.response_stream, &response_schema) != 0) {
		batch.release(&batch);
		throw IOException("n6k_catalog_rpc_table: failed to read response schema for %s", bind_data.function_name);
	}

	ArrowArrayView array_view;
	ArrowArrayViewInitFromSchema(&array_view, &response_schema, nullptr);
	ArrowArrayViewSetArray(&array_view, &batch, nullptr);

	auto row_count = MinValue<idx_t>((idx_t)batch.length, STANDARD_VECTOR_SIZE);
	output.SetCardinality(row_count);

	for (idx_t col = 0; col < output.ColumnCount() && col < (idx_t)response_schema.n_children; col++) {
		auto &out_vec = output.data[col];
		auto *child_view = array_view.children[col];

		if (out_vec.GetType().InternalType() == PhysicalType::VARCHAR) {
			auto result_data = FlatVector::GetData<string_t>(out_vec);
			for (idx_t row = 0; row < row_count; row++) {
				if (ArrowArrayViewIsNull(child_view, static_cast<int64_t>(row))) {
					FlatVector::SetNull(out_vec, row, true);
				} else {
					auto sv = ArrowArrayViewGetStringUnsafe(child_view, static_cast<int64_t>(row));
					result_data[row] = StringVector::AddString(out_vec, sv.data, sv.size_bytes);
				}
			}
		} else {
			auto type_size = GetTypeIdSize(out_vec.GetType().InternalType());
			auto *src = static_cast<const uint8_t *>(child_view->buffer_views[1].data.data);
			if (src) {
				memcpy(FlatVector::GetData(out_vec), src, row_count * type_size);
			}
			for (idx_t row = 0; row < row_count; row++) {
				if (ArrowArrayViewIsNull(child_view, static_cast<int64_t>(row))) {
					FlatVector::SetNull(out_vec, row, true);
				}
			}
		}
	}

	ArrowArrayViewReset(&array_view);
	if (response_schema.release) {
		response_schema.release(&response_schema);
	}
	if (batch.release) {
		batch.release(&batch);
	}

	return row_count > 0 ? OperatorFinalizeResultType::HAVE_MORE_OUTPUT : OperatorFinalizeResultType::FINISHED;
}

// Created dynamically by N6kSchemaEntry::GetOrCreateTableFunctionEntry, so foo.my_func(args) becomes an
// OP_RPC_TABLE request over the catalog's WebSocket.
struct CatalogRpcInfo : public TableFunctionInfo {
	string function_name;
	std::shared_ptr<CatalogSession> session;
	CatalogRpcInfo(string function_name_p, std::shared_ptr<CatalogSession> session_p)
	    : function_name(std::move(function_name_p)), session(std::move(session_p)) {
	}
};

static unique_ptr<FunctionData> CatalogRpcBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<CatalogRpcInfo>();
	// `session` is never null here: this bind only runs for a name looked up inside an ATTACHed
	// catalog, GetCatalogSession throws rather than returning null, and N6kCatalog is built from a
	// CatalogSession::Create that throws on failure.
	return BindRpcScalar(context, *info.session, info.function_name, RpcArgsJson(input.inputs, 0), return_types, names);
}

static unique_ptr<FunctionData> CatalogNamedRpcBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<CatalogRpcInfo>();
	if (input.inputs.empty()) {
		throw BinderException("rpc() requires at least a function name argument");
	}
	auto function_name = input.inputs[0].GetValue<string>();
	return BindRpcScalar(context, *info.session, function_name, RpcArgsJson(input.inputs, 1), return_types, names);
}

struct N6kExecBindData : public TableFunctionData {
	int64_t affected_count;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<N6kExecBindData>();
		copy->affected_count = affected_count;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<N6kExecBindData>();
		return affected_count == other.affected_count;
	}
};

struct N6kExecGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> N6kExecBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto sql = input.inputs[1].GetValue<string>();
	auto session = GetCatalogSession(context, db_name);

	auto bind_data = make_uniq<N6kExecBindData>();
	bind_data->affected_count = session->Exec(sql, [&context]() { return context.IsInterrupted(); });

	names.push_back("affected_rows");
	return_types.push_back(LogicalType::BIGINT);
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> N6kExecInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<N6kExecGlobalState>();
}

static void N6kExecScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &gstate = data_p.global_state->Cast<N6kExecGlobalState>();
	if (gstate.done) {
		return;
	}
	gstate.done = true;
	auto &bind_data = data_p.bind_data->Cast<N6kExecBindData>();
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(bind_data.affected_count));
}

// drop cached schema metadata; work at bind time, reuses N6kExecBindData/N6kExecScan for the count
static unique_ptr<FunctionData> N6kInvalidateCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto schemas_csv = input.inputs[1].GetValue<string>();

	auto &catalog = Catalog::GetCatalog(context, db_name);
	auto &n6k_catalog = catalog.Cast<N6kCatalog>();

	vector<string> schemas;
	for (auto &part : StringUtil::Split(schemas_csv, ',')) {
		if (!part.empty()) {
			schemas.push_back(part);
		}
	}
	n6k_catalog.InvalidateSchemas(schemas);

	auto bind_data = make_uniq<N6kExecBindData>();
	bind_data->affected_count = static_cast<int64_t>(schemas.size());

	names.push_back("invalidated_schemas");
	return_types.push_back(LogicalType::BIGINT);
	return std::move(bind_data);
}

static unique_ptr<FunctionData> N6kQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto db_name = input.inputs[0].GetValue<string>();
	auto sql = input.inputs[1].GetValue<string>();
	auto session = GetCatalogSession(context, db_name);
	return BindQuery(context, *session, sql, return_types, names);
}

struct CatalogSessionInfo : public TableFunctionInfo {
	std::shared_ptr<CatalogSession> session;
	explicit CatalogSessionInfo(std::shared_ptr<CatalogSession> session_p) : session(std::move(session_p)) {
	}
};

static unique_ptr<FunctionData> CatalogExecBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<CatalogSessionInfo>();
	if (input.inputs.empty()) {
		throw BinderException("exec() requires a SQL statement argument");
	}
	auto sql = input.inputs[0].GetValue<string>();

	auto bind_data = make_uniq<N6kExecBindData>();
	bind_data->affected_count = info.session->Exec(sql, [&context]() { return context.IsInterrupted(); });

	names.push_back("affected_rows");
	return_types.push_back(LogicalType::BIGINT);
	return std::move(bind_data);
}

static unique_ptr<FunctionData> CatalogQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<CatalogSessionInfo>();
	if (input.inputs.empty()) {
		throw BinderException("query() requires a SQL statement argument");
	}
	auto sql = input.inputs[0].GetValue<string>();
	return BindQuery(context, *info.session, sql, return_types, names);
}

TableFunction MakeN6kCatalogExecFunction(std::shared_ptr<CatalogSession> session) {
	auto info = make_shared_ptr<CatalogSessionInfo>(std::move(session));
	TableFunction tf("exec", {}, N6kExecScan, CatalogExecBind, N6kExecInitGlobal);
	tf.varargs = LogicalType::ANY;
	tf.function_info = std::move(info);
	return tf;
}

TableFunction MakeN6kCatalogQueryFunction(std::shared_ptr<CatalogSession> session) {
	auto info = make_shared_ptr<CatalogSessionInfo>(std::move(session));
	TableFunction tf("query", {}, ArrowTableFunction::ArrowScanFunction, CatalogQueryBind,
	                 ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	tf.varargs = LogicalType::ANY;
	// no projection_pushdown: bind-time materialize can't remap column_ids (segfault)
	tf.function_info = std::move(info);
	return tf;
}

TableFunction MakeN6kCatalogRpcFunction(const string &function_name, std::shared_ptr<CatalogSession> session) {
	auto rpc_info = make_shared_ptr<CatalogRpcInfo>(function_name, std::move(session));

	table_function_bind_t bind_func;
	if (function_name == "rpc") {
		bind_func = CatalogNamedRpcBind;
	} else {
		bind_func = CatalogRpcBind;
	}

	TableFunction tf(function_name, {}, ArrowTableFunction::ArrowScanFunction, bind_func,
	                 ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	tf.varargs = LogicalType::ANY;
	// no projection_pushdown: bind-time materialize can't remap column_ids (segfault)
	tf.function_info = std::move(rpc_info);
	return tf;
}

void RegisterN6kRpc(ExtensionLoader &loader) {
	TableFunction rpc_func("n6k_catalog_rpc", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       ArrowTableFunction::ArrowScanFunction, N6kRpcBind, ArrowTableFunction::ArrowScanInitGlobal,
	                       ArrowTableFunction::ArrowScanInitLocal);
	rpc_func.varargs = LogicalType::ANY;
	// no projection_pushdown: bind-time materialize can't remap column_ids (segfault)
	loader.RegisterFunction(rpc_func);

	TableFunction rpc_table_func("n6k_catalog_rpc_table",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TABLE}, nullptr,
	                             N6kRpcArrowBind, N6kRpcArrowInitGlobal, N6kRpcArrowInitLocal);
	rpc_table_func.varargs = LogicalType::ANY;
	rpc_table_func.in_out_function = N6kRpcArrowInOut;
	rpc_table_func.in_out_function_final = N6kRpcArrowFinal;
	loader.RegisterFunction(rpc_table_func);

	TableFunction exec_func("n6k_catalog_exec", {LogicalType::VARCHAR, LogicalType::VARCHAR}, N6kExecScan, N6kExecBind,
	                        N6kExecInitGlobal);
	loader.RegisterFunction(exec_func);

	TableFunction invalidate_func("n6k_invalidate_cache", {LogicalType::VARCHAR, LogicalType::VARCHAR}, N6kExecScan,
	                              N6kInvalidateCacheBind, N6kExecInitGlobal);
	loader.RegisterFunction(invalidate_func);

	TableFunction query_func("n6k_catalog_query", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         ArrowTableFunction::ArrowScanFunction, N6kQueryBind,
	                         ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	// no projection_pushdown: bind-time materialize can't remap column_ids (segfault)
	loader.RegisterFunction(query_func);
}

} // namespace duckdb
