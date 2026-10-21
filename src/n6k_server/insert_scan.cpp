#include "insert_scan.hpp"

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <cstdlib>
#include <cstring>

namespace duckdb {
namespace n6k {

static void OpenIpcReaderOverOwnedBuffer(uint8_t *buf, int64_t len, ArrowArrayStream *out) {
	ArrowBuffer arrow_buf;
	ArrowBufferInit(&arrow_buf);
	arrow_buf.data = buf;
	arrow_buf.size_bytes = len;
	arrow_buf.capacity_bytes = len;

	ArrowIpcInputStream input_stream;
	if (ArrowIpcInputStreamInitBuffer(&input_stream, &arrow_buf) != NANOARROW_OK) {
		free(buf);
		throw IOException("n6k_server: ArrowIpcInputStreamInitBuffer failed");
	}
	if (ArrowIpcArrayStreamReaderInit(out, &input_stream, nullptr) != NANOARROW_OK) {
		if (input_stream.release) {
			input_stream.release(&input_stream);
		}
		throw IOException("n6k_server: ArrowIpcArrayStreamReaderInit failed");
	}
}

static uint8_t *CopyBytes(const uint8_t *data, size_t len) {
	auto *buf = static_cast<uint8_t *>(malloc(len ? len : 1));
	if (!buf) {
		throw IOException("n6k_server: out of memory decoding Arrow insert");
	}
	if (len) {
		memcpy(buf, data, len);
	}
	return buf;
}

int64_t CountArrowIpcRows(const uint8_t *data, size_t len) {
	ArrowArrayStream stream;
	stream.release = nullptr;
	OpenIpcReaderOverOwnedBuffer(CopyBytes(data, len), static_cast<int64_t>(len), &stream);

	int64_t total = 0;
	ArrowSchema schema;
	schema.release = nullptr;
	// A schema that will not read means the payload is malformed. Reporting 0 rows here would ack the
	// INSERT as a no-op, so the client sees success and silently loses every row it sent.
	if (stream.get_schema(&stream, &schema) != 0 || !schema.release) {
		if (stream.release) {
			stream.release(&stream);
		}
		throw InvalidInputException("n6k_server: malformed Arrow IPC insert payload (schema unreadable)");
	}
	schema.release(&schema);
	ArrowArray array;
	while (true) {
		array.release = nullptr;
		if (stream.get_next(&stream, &array) != 0 || !array.release) {
			break;
		}
		total += array.length;
		array.release(&array);
	}
	if (stream.release) {
		stream.release(&stream);
	}
	return total;
}

namespace {
struct InsertStreamData {
	ArrowArrayStream stream;
	bool consumed = false;

	InsertStreamData() {
		stream.release = nullptr;
	}
	~InsertStreamData() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

unique_ptr<ArrowArrayStreamWrapper> TakeInsertStreamOnce(uintptr_t factory_ptr, ArrowStreamParameters &) {
	auto *sd = reinterpret_cast<InsertStreamData *>(factory_ptr);
	if (sd->consumed) {
		throw InternalException("n6k_server: insert stream produced twice");
	}
	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	wrapper->arrow_array_stream = sd->stream;
	sd->stream.release = nullptr;
	sd->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

struct InsertScanData : public ArrowScanFunctionData {
	unique_ptr<InsertStreamData> data_stream;
	unique_ptr<InsertStreamData> schema_stream;

	InsertScanData(stream_factory_produce_t producer, unique_ptr<InsertStreamData> ds, unique_ptr<InsertStreamData> ss)
	    : ArrowScanFunctionData(producer, reinterpret_cast<uintptr_t>(ds.get())), data_stream(std::move(ds)),
	      schema_stream(std::move(ss)) {
	}
};
} // namespace

static unique_ptr<FunctionData> InsertScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto *bytes = reinterpret_cast<InsertArrowBytes *>(input.inputs[0].GetPointer());

	// Decode twice: one stream feeds the scan, the other yields the schema for bind.
	auto data_stream = make_uniq<InsertStreamData>();
	OpenIpcReaderOverOwnedBuffer(CopyBytes(bytes->data, bytes->len), static_cast<int64_t>(bytes->len),
	                             &data_stream->stream);
	auto schema_stream = make_uniq<InsertStreamData>();
	OpenIpcReaderOverOwnedBuffer(CopyBytes(bytes->data, bytes->len), static_cast<int64_t>(bytes->len),
	                             &schema_stream->stream);

	auto result = make_uniq<InsertScanData>(reinterpret_cast<stream_factory_produce_t>(TakeInsertStreamOnce),
	                                        std::move(data_stream), std::move(schema_stream));

	auto *sp = result->schema_stream.get();
	if (sp->stream.get_schema(&sp->stream, &result->schema_root.arrow_schema) != 0) {
		throw InvalidInputException("n6k_server: malformed Arrow IPC insert payload (schema unreadable)");
	}
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	return std::move(result);
}

void RegisterInsertScanFunction(ExtensionLoader &loader) {
	TableFunction func(N6K_INSERT_SCAN_FN, {LogicalType::POINTER}, ArrowTableFunction::ArrowScanFunction,
	                   InsertScanBind, ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	CreateTableFunctionInfo info(std::move(func));
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

} // namespace n6k
} // namespace duckdb
