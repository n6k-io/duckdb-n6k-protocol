#include "arrow_ipc_decode_selftest.hpp"
#include "n6k_arrow_frame_decoder.hpp"

#include "duckdb/function/table_function.hpp"

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <string>
#include <vector>

namespace duckdb {

namespace {

// Throw the nanoarrow error text verbatim (never as a printf format — a stray % in err.message would
// otherwise raise a formatting exception that masks the real failure).
void ThrowNanoarrow(const char *stage, const ArrowError &err) {
	throw IOException("n6k_testing_arrow_ipc_decode: " + std::string(stage) + ": " + std::string(err.message));
}

struct DecodedRow {
	int32_t id;
	std::string name;
};

std::vector<std::string> BuildPyarrowFrames() {
	ArrowError err;
	err.message[0] = '\0';

	ArrowSchema schema;
	ArrowSchemaInit(&schema);
	if (ArrowSchemaSetTypeStruct(&schema, 2) != NANOARROW_OK ||
	    ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32) != NANOARROW_OK ||
	    ArrowSchemaSetName(schema.children[0], "id") != NANOARROW_OK ||
	    ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING) != NANOARROW_OK ||
	    ArrowSchemaSetName(schema.children[1], "name") != NANOARROW_OK) {
		ThrowNanoarrow("build schema", err);
	}

	ArrowBuffer buf;
	ArrowBufferInit(&buf);
	ArrowIpcOutputStream out_stream;
	ArrowIpcWriter writer;
	if (ArrowIpcOutputStreamInitBuffer(&out_stream, &buf) != NANOARROW_OK ||
	    ArrowIpcWriterInit(&writer, &out_stream) != NANOARROW_OK) {
		schema.release(&schema);
		ThrowNanoarrow("writer init", err);
	}

	// Slice out every byte written to `buf` since the previous flush — one IPC message.
	size_t cursor = 0;
	auto flush = [&]() -> std::string {
		std::string frame(reinterpret_cast<const char *>(buf.data) + cursor,
		                  static_cast<size_t>(buf.size_bytes) - cursor);
		cursor = static_cast<size_t>(buf.size_bytes);
		return frame;
	};

	auto write_batch = [&](const std::vector<int32_t> &ids, const std::vector<const char *> &names) -> std::string {
		ArrowArray array;
		if (ArrowArrayInitFromSchema(&array, &schema, &err) != NANOARROW_OK ||
		    ArrowArrayStartAppending(&array) != NANOARROW_OK) {
			ThrowNanoarrow("array init", err);
		}
		for (size_t i = 0; i < ids.size(); i++) {
			ArrowStringView sv {names[i], static_cast<int64_t>(std::string(names[i]).size())};
			if (ArrowArrayAppendInt(array.children[0], ids[i]) != NANOARROW_OK ||
			    ArrowArrayAppendString(array.children[1], sv) != NANOARROW_OK ||
			    ArrowArrayFinishElement(&array) != NANOARROW_OK) {
				array.release(&array);
				ThrowNanoarrow("array append", err);
			}
		}
		ArrowArrayView view;
		if (ArrowArrayFinishBuildingDefault(&array, &err) != NANOARROW_OK ||
		    ArrowArrayViewInitFromSchema(&view, &schema, &err) != NANOARROW_OK ||
		    ArrowArrayViewSetArray(&view, &array, &err) != NANOARROW_OK ||
		    ArrowIpcWriterWriteArrayView(&writer, &view, &err) != NANOARROW_OK) {
			ArrowArrayViewReset(&view);
			array.release(&array);
			ThrowNanoarrow("write batch", err);
		}
		ArrowArrayViewReset(&view);
		array.release(&array);
		return flush();
	};

	std::vector<std::string> frames;
	try {
		if (ArrowIpcWriterWriteSchema(&writer, &schema, &err) != NANOARROW_OK) {
			ThrowNanoarrow("write schema", err);
		}
		std::string schema_msg = flush();
		std::string batch0 = write_batch({1, 2}, {"Alice", "Bob"});
		std::string batch1 = write_batch({3}, {"Charlie"});
		if (ArrowIpcWriterWriteArrayView(&writer, nullptr, &err) != NANOARROW_OK) {
			ThrowNanoarrow("write eos", err);
		}
		std::string eos = flush();

		frames.emplace_back();                 // empty RESP_SCHEMA frame
		frames.push_back(schema_msg + batch0); // first RESP_CHUNK: two messages in one frame
		frames.push_back(std::move(batch1));
		frames.push_back(std::move(eos));
	} catch (...) {
		ArrowIpcWriterReset(&writer);
		ArrowBufferReset(&buf);
		schema.release(&schema);
		throw;
	}

	ArrowIpcWriterReset(&writer);
	ArrowBufferReset(&buf);
	schema.release(&schema);
	return frames;
}

void CollectBatch(const ArrowSchema *schema, ArrowArray *array, std::vector<DecodedRow> &rows) {
	ArrowError err;
	err.message[0] = '\0';
	ArrowArrayView view;
	if (ArrowArrayViewInitFromSchema(&view, schema, &err) != NANOARROW_OK ||
	    ArrowArrayViewSetArray(&view, array, &err) != NANOARROW_OK) {
		ArrowArrayViewReset(&view);
		ThrowNanoarrow("view batch", err);
	}
	for (int64_t i = 0; i < array->length; i++) {
		DecodedRow row;
		row.id = static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(view.children[0], i));
		ArrowStringView sv = ArrowArrayViewGetStringUnsafe(view.children[1], i);
		row.name.assign(sv.data, static_cast<size_t>(sv.size_bytes));
		rows.push_back(std::move(row));
	}
	ArrowArrayViewReset(&view);
}

std::vector<DecodedRow> RunDecodeSelftest() {
	std::vector<std::string> frames = BuildPyarrowFrames();

	n6k::N6kArrowFrameDecoder decoder;
	std::vector<DecodedRow> rows;
	for (const auto &frame : frames) {
		if (!decoder.PushFrame(frame)) {
			throw IOException("n6k_testing_arrow_ipc_decode: PushFrame: " + decoder.ErrorMessage());
		}
		ArrowArray batch;
		while (decoder.TryPopBatch(&batch)) {
			CollectBatch(decoder.GetSchema(), &batch, rows);
			batch.release(&batch);
		}
	}
	if (!decoder.SawEndOfStream()) {
		throw IOException("n6k_testing_arrow_ipc_decode: stream ended without EOS marker");
	}
	return rows;
}

struct N6kFrameDecodeSelftestBind : public TableFunctionData {};

struct N6kFrameDecodeSelftestState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"id", "name"};
	return_types = {LogicalType::INTEGER, LogicalType::VARCHAR};
	return make_uniq<N6kFrameDecodeSelftestBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kFrameDecodeSelftestState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kFrameDecodeSelftestState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	const std::vector<DecodedRow> rows = RunDecodeSelftest();
	for (idx_t i = 0; i < rows.size(); i++) {
		output.SetValue(0, i, Value::INTEGER(rows[i].id));
		output.SetValue(1, i, Value(rows[i].name));
	}
	output.SetCardinality(rows.size());
}

} // namespace

void RegisterN6kTestingArrowIpcDecode(ExtensionLoader &loader) {
	TableFunction fn("n6k_testing_arrow_ipc_decode", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
