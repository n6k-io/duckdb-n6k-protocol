#include "catch.hpp"
#include "n6k_arrow_frame_decoder.hpp"

#include "duckdb/common/exception.hpp"

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <string>
#include <vector>

using namespace duckdb;

namespace {

// Verbatim, never as a printf format: a stray % in err.message would mask the real failure.
void ThrowNanoarrow(const char *stage, const ArrowError &err) {
	throw IOException("arrow frame test: " + std::string(stage) + ": " + std::string(err.message));
}

struct DecodedRow {
	int32_t id;
	std::string name;

	bool operator==(const DecodedRow &other) const {
		return id == other.id && name == other.name;
	}
};

std::ostream &operator<<(std::ostream &out, const DecodedRow &row) {
	return out << row.id << ":" << row.name;
}

// The framing pyarrow produces: an empty RESP_SCHEMA frame, a first RESP_CHUNK carrying the schema and first batch as
// two IPC messages, a second batch, and EOS.
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

		frames.emplace_back();
		frames.push_back(schema_msg + batch0);
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

} // namespace

TEST_CASE("the decoder reads the pyarrow framing across frames", "[arrow]") {
	const auto frames = BuildPyarrowFrames();
	n6k::N6kArrowFrameDecoder decoder;
	std::vector<DecodedRow> rows;

	for (const auto &frame : frames) {
		INFO(decoder.ErrorMessage());
		REQUIRE(decoder.PushFrame(frame));
		ArrowArray batch;
		while (decoder.TryPopBatch(&batch)) {
			CollectBatch(decoder.GetSchema(), &batch, rows);
			batch.release(&batch);
		}
	}

	REQUIRE(decoder.SawEndOfStream());
	REQUIRE(rows == std::vector<DecodedRow> {{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}});
}
