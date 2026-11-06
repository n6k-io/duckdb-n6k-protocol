#include "arrow_ipc_encode.hpp"

#include "n6k_wire_type.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace n6k {

namespace {

vector<string> NamesOf(const vector<Identifier> &identifiers) {
	vector<string> names;
	names.reserve(identifiers.size());
	for (auto &identifier : identifiers) {
		names.push_back(identifier.GetIdentifierName());
	}
	return names;
}

ClientProperties WireProperties(ClientContext &context) {
	auto props = context.GetClientProperties();
	props.arrow_lossless_conversion = true;
	return props;
}

} // namespace

ArrowIpcStreamEncoder::ArrowIpcStreamEncoder(ClientContext &context, const vector<LogicalType> &types,
                                             const vector<Identifier> &names)
    : ArrowIpcStreamEncoder(context, types, NamesOf(names)) {
}

ArrowIpcStreamEncoder::ArrowIpcStreamEncoder(ClientContext &context, const vector<LogicalType> &types,
                                             const vector<string> &names)
    : context_(context) {
	for (auto &type : types) {
		wire_types_.push_back(StripEnums(type));
		needs_cast_ = needs_cast_ || wire_types_.back() != type;
	}
	auto props = WireProperties(context);
	ArrowConverter::ToArrowSchema(&schema_, wire_types_, names, props);
	for (idx_t i = 0; i < types.size(); i++) {
		if (wire_types_[i] == types[i]) {
			continue;
		}
		ArrowSchemaMetadata metadata;
		metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME, DUCKDB_TYPE_EXTENSION);
		metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, SerializeTypeToBase64(types[i]));
		field_metadata_.push_back(metadata.SerializeMetadata());
		schema_.children[i]->metadata = field_metadata_.back().get();
	}
	ArrowBufferInit(&buf_);
	if (ArrowIpcOutputStreamInitBuffer(&out_stream_, &buf_) != NANOARROW_OK) {
		throw IOException("n6k: ArrowIpcOutputStreamInitBuffer failed");
	}
	if (ArrowIpcWriterInit(&writer_, &out_stream_) != NANOARROW_OK) {
		throw IOException("n6k: ArrowIpcWriterInit failed");
	}
}

ArrowIpcStreamEncoder::~ArrowIpcStreamEncoder() {
	ArrowIpcWriterReset(&writer_);
	if (schema_.release) {
		schema_.release(&schema_);
	}
	ArrowBufferReset(&buf_);
}

std::string ArrowIpcStreamEncoder::TakeNewlyWrittenBytes() {
	auto total = static_cast<size_t>(buf_.size_bytes);
	std::string out(reinterpret_cast<const char *>(buf_.data) + cursor_, total - cursor_);
	cursor_ = total;
	return out;
}

std::string ArrowIpcStreamEncoder::SchemaMessage() {
	if (ArrowIpcWriterWriteSchema(&writer_, &schema_, nullptr) != NANOARROW_OK) {
		throw IOException("n6k: ArrowIpcWriterWriteSchema failed");
	}
	return TakeNewlyWrittenBytes();
}

std::string ArrowIpcStreamEncoder::EncodeChunk(DataChunk &chunk) {
	auto props = WireProperties(context_);
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> empty_extensions;

	DataChunk cast_chunk;
	DataChunk *to_encode = &chunk;
	if (needs_cast_) {
		CastChunkToWireTypes(context_, chunk, wire_types_, cast_chunk);
		to_encode = &cast_chunk;
	}

	ArrowArray array;
	ArrowConverter::ToArrowArray(*to_encode, &array, props, empty_extensions);

	ArrowArrayView view;
	std::string result;
	bool ok = ArrowArrayViewInitFromSchema(&view, &schema_, nullptr) == NANOARROW_OK &&
	          ArrowArrayViewSetArray(&view, &array, nullptr) == NANOARROW_OK &&
	          ArrowIpcWriterWriteArrayView(&writer_, &view, nullptr) == NANOARROW_OK;
	ArrowArrayViewReset(&view);
	if (array.release) {
		array.release(&array);
	}
	if (!ok) {
		throw IOException("n6k: encoding Arrow record batch failed");
	}
	return TakeNewlyWrittenBytes();
}

std::string ArrowIpcStreamEncoder::TakeEndOfStreamMessage() {
	if (finalized_) {
		return {};
	}
	finalized_ = true;
	if (ArrowIpcWriterWriteArrayView(&writer_, nullptr, nullptr) != NANOARROW_OK) {
		throw IOException("n6k: writing Arrow end-of-stream failed");
	}
	return TakeNewlyWrittenBytes();
}

} // namespace n6k
} // namespace duckdb
