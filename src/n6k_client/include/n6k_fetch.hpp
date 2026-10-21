#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"
#include <string>

namespace duckdb {

static void AdoptIpcBufferAsArrayStream(uint8_t *buf, int64_t len, ArrowArrayStream *out) {
	struct ArrowBuffer arrow_buf;
	ArrowBufferInit(&arrow_buf);
	arrow_buf.data = buf;
	arrow_buf.size_bytes = len;
	arrow_buf.capacity_bytes = len;

	struct ArrowIpcInputStream input_stream;
	auto rc = ArrowIpcInputStreamInitBuffer(&input_stream, &arrow_buf);
	if (rc != NANOARROW_OK) {
		free(buf);
		throw IOException("n6k: ArrowIpcInputStreamInitBuffer failed");
	}

	rc = ArrowIpcArrayStreamReaderInit(out, &input_stream, nullptr);
	if (rc != NANOARROW_OK) {
		if (input_stream.release) {
			input_stream.release(&input_stream);
		}
		throw IOException("n6k: ArrowIpcArrayStreamReaderInit failed");
	}
}

static void SerializeChunksToArrowIPC(ClientContext &context, const vector<LogicalType> &types,
                                      const vector<string> &col_names, vector<unique_ptr<DataChunk>> &chunks,
                                      ArrowBuffer *out_buf) {
	auto client_props = context.GetClientProperties();

	ArrowSchema arrow_schema;
	ArrowConverter::ToArrowSchema(&arrow_schema, types, col_names, client_props);

	ArrowError error;
	error.message[0] = '\0';
	const auto release_schema = [&]() {
		if (arrow_schema.release) {
			arrow_schema.release(&arrow_schema);
		}
	};

	ArrowIpcOutputStream output_stream;
	if (ArrowIpcOutputStreamInitBuffer(&output_stream, out_buf) != NANOARROW_OK) {
		release_schema();
		throw IOException("n6k: ArrowIpcOutputStreamInitBuffer failed");
	}

	ArrowIpcWriter writer;
	if (ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK) {
		if (output_stream.release) {
			output_stream.release(&output_stream);
		}
		release_schema();
		throw IOException("n6k: ArrowIpcWriterInit failed");
	}
	const auto fail = [&](const char *step) {
		ArrowIpcWriterReset(&writer);
		release_schema();
		throw IOException("n6k: Arrow IPC %s failed: %s", step, error.message);
	};
	if (ArrowIpcWriterWriteSchema(&writer, &arrow_schema, &error) != NANOARROW_OK) {
		fail("write schema");
	}

	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> empty_extensions;
	for (auto &chunk : chunks) {
		if (chunk->size() == 0) {
			continue;
		}
		ArrowArray arrow_array;
		ArrowConverter::ToArrowArray(*chunk, &arrow_array, client_props, empty_extensions);

		ArrowArrayView array_view;
		int rc = ArrowArrayViewInitFromSchema(&array_view, &arrow_schema, &error);
		if (rc == NANOARROW_OK) {
			rc = ArrowArrayViewSetArray(&array_view, &arrow_array, &error);
			if (rc == NANOARROW_OK) {
				rc = ArrowIpcWriterWriteArrayView(&writer, &array_view, &error);
			}
			ArrowArrayViewReset(&array_view);
		}
		if (arrow_array.release) {
			arrow_array.release(&arrow_array);
		}
		if (rc != NANOARROW_OK) {
			fail("write batch");
		}
	}

	ArrowIpcWriterReset(&writer);
	release_schema();
}

} // namespace duckdb
