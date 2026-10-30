#include "wire_codec_bench.hpp"
#include "arrow_ipc_encode.hpp"
#include "n6k_arrow_frame_decoder.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

namespace duckdb {

namespace {

struct Measured {
	string codec;
	string stage;
	double seconds;
	idx_t bytes;
};

struct Rows {
	vector<LogicalType> types;
	vector<string> names;
	vector<unique_ptr<DataChunk>> chunks;
};

Rows Materialize(DatabaseInstance &db, const string &sql) {
	Connection con(db);
	auto result = con.Query(sql);
	if (result->HasError()) {
		result->ThrowError();
	}
	Rows rows;
	rows.types = result->types;
	rows.names = result->names;
	while (auto chunk = result->Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		rows.chunks.push_back(std::move(chunk));
	}
	return rows;
}

template <class F>
double Median(idx_t reps, F &&run) {
	vector<double> times;
	for (idx_t r = 0; r < reps; r++) {
		auto start = std::chrono::steady_clock::now();
		run();
		times.push_back(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
	}
	std::sort(times.begin(), times.end());
	return times[times.size() / 2];
}

idx_t TotalBytes(const vector<string> &frames) {
	idx_t total = 0;
	for (auto &frame : frames) {
		total += frame.size();
	}
	return total;
}

// The server's Arrow payloads with batch_rows unset: a schema message, then one record batch per chunk
// (envelope excluded).
vector<string> ArrowEncode(ClientContext &context, Rows &rows) {
	n6k::ArrowIpcStreamEncoder encoder(context, rows.types, rows.names);
	vector<string> frames;
	frames.push_back(encoder.SchemaMessage());
	for (auto &chunk : rows.chunks) {
		frames.push_back(encoder.EncodeChunk(*chunk));
	}
	return frames;
}

// The client's decode, excluding EmitReadChunk's type-cast pass.
idx_t ArrowDecode(ClientContext &context, const vector<string> &frames) {
	n6k::N6kArrowFrameDecoder decoder;
	ArrowTableSchema arrow_table;
	DataChunk out;
	vector<column_t> column_ids;
	idx_t rows = 0;
	for (idx_t f = 0; f < frames.size(); f++) {
		if (!decoder.PushFrame(frames[f])) {
			throw IOException("n6k_testing_wire_codec_bench: " + decoder.ErrorMessage());
		}
		if (f == 0) {
			ArrowTableFunction::PopulateArrowTableSchema(context, arrow_table, *decoder.GetSchema());
			out.Initialize(context, arrow_table.GetTypes());
			for (idx_t i = 0; i < arrow_table.GetTypes().size(); i++) {
				column_ids.push_back(i);
			}
		}
		auto wrapper = make_uniq<ArrowArrayWrapper>();
		while (decoder.TryPopBatch(&wrapper->arrow_array)) {
			ArrowScanLocalState scan_state(std::move(wrapper), context);
			scan_state.column_ids = column_ids;
			auto len = static_cast<idx_t>(scan_state.chunk->arrow_array.length);
			while (scan_state.chunk_offset < len) {
				out.Reset();
				auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, len - scan_state.chunk_offset);
				out.SetCardinality(count);
				ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), out);
				scan_state.chunk_offset += count;
				rows += count;
			}
			wrapper = make_uniq<ArrowArrayWrapper>();
		}
	}
	return rows;
}

SerializationOptions NativeOptions() {
	SerializationOptions options;
	options.serialization_compatibility = SerializationCompatibility::FromIndex(7);
	return options;
}

vector<string> NativeEncode(Rows &rows) {
	auto options = NativeOptions();
	vector<string> frames;
	MemoryStream stream;
	for (auto &chunk : rows.chunks) {
		stream.Rewind();
		BinarySerializer serializer(stream, options);
		serializer.Begin();
		chunk->Serialize(serializer);
		serializer.End();
		frames.emplace_back(const_char_ptr_cast(stream.GetData()), stream.GetPosition());
	}
	return frames;
}

idx_t NativeDecode(vector<string> &frames) {
	idx_t rows = 0;
	for (auto &frame : frames) {
		MemoryStream stream(data_ptr_cast(&frame[0]), frame.size());
		BinaryDeserializer deserializer(stream);
		deserializer.Begin();
		DataChunk chunk;
		chunk.Deserialize(deserializer);
		deserializer.End();
		rows += chunk.size();
	}
	return rows;
}

vector<Measured> RunBench(ClientContext &context, const string &sql, idx_t reps) {
	auto rows = Materialize(*context.db, sql);
	idx_t expected = 0;
	for (auto &chunk : rows.chunks) {
		expected += chunk->size();
	}

	vector<Measured> out;
	vector<string> arrow_frames;
	auto arrow_encode = Median(reps, [&]() { arrow_frames = ArrowEncode(context, rows); });
	out.push_back({"arrow", "encode", arrow_encode, TotalBytes(arrow_frames)});
	idx_t decoded = 0;
	auto arrow_decode = Median(reps, [&]() { decoded = ArrowDecode(context, arrow_frames); });
	if (decoded != expected) {
		throw InternalException("n6k_testing_wire_codec_bench: arrow decoded %llu rows, expected %llu", decoded,
		                        expected);
	}
	out.push_back({"arrow", "decode", arrow_decode, TotalBytes(arrow_frames)});

	vector<string> native_frames;
	auto native_encode = Median(reps, [&]() { native_frames = NativeEncode(rows); });
	out.push_back({"native", "encode", native_encode, TotalBytes(native_frames)});
	auto native_decode = Median(reps, [&]() { decoded = NativeDecode(native_frames); });
	if (decoded != expected) {
		throw InternalException("n6k_testing_wire_codec_bench: native decoded %llu rows, expected %llu", decoded,
		                        expected);
	}
	out.push_back({"native", "decode", native_decode, TotalBytes(native_frames)});
	return out;
}

struct BenchBind : public TableFunctionData {
	string sql;
	idx_t reps = 5;
};

struct BenchState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                              vector<string> &names) {
	auto bind = make_uniq<BenchBind>();
	bind->sql = input.inputs[0].GetValue<string>();
	auto reps = input.named_parameters.find("reps");
	if (reps != input.named_parameters.end()) {
		bind->reps = MaxValue<idx_t>(1, reps->second.GetValue<idx_t>());
	}
	names = {"codec", "stage", "seconds", "bytes"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::UBIGINT};
	return std::move(bind);
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<BenchState>();
}

void Scan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<BenchState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;
	auto &bind = data_p.bind_data->Cast<BenchBind>();
	auto measured = RunBench(context, bind.sql, bind.reps);
	for (idx_t i = 0; i < measured.size(); i++) {
		output.SetValue(0, i, Value(measured[i].codec));
		output.SetValue(1, i, Value(measured[i].stage));
		output.SetValue(2, i, Value::DOUBLE(measured[i].seconds));
		output.SetValue(3, i, Value::UBIGINT(measured[i].bytes));
	}
	output.SetCardinality(measured.size());
}

} // namespace

void RegisterN6kTestingWireCodecBench(ExtensionLoader &loader) {
	TableFunction fn("n6k_testing_wire_codec_bench", {LogicalType::VARCHAR}, Scan, Bind, InitGlobal);
	fn.named_parameters["reps"] = LogicalType::UBIGINT;
	loader.RegisterFunction(fn);
}

} // namespace duckdb
