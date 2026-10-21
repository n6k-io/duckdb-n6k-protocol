#include "arrow_ipc_encode.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {
namespace n6k {

namespace {

LogicalType StripEnums(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::ENUM:
		return LogicalType::VARCHAR;
	case LogicalTypeId::LIST:
		return LogicalType::LIST(StripEnums(ListType::GetChildType(type)));
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(StripEnums(ArrayType::GetChildType(type)), ArrayType::GetSize(type));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(StripEnums(MapType::KeyType(type)), StripEnums(MapType::ValueType(type)));
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, StripEnums(child.second));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::UNION: {
		child_list_t<LogicalType> members;
		for (idx_t i = 0; i < UnionType::GetMemberCount(type); i++) {
			members.emplace_back(UnionType::GetMemberName(type, i), StripEnums(UnionType::GetMemberType(type, i)));
		}
		return LogicalType::UNION(std::move(members));
	}
	default:
		return type;
	}
}

} // namespace

ArrowIpcStreamEncoder::ArrowIpcStreamEncoder(ClientContext &context, const vector<LogicalType> &types,
                                             const vector<string> &names)
    : context_(context) {
	for (auto &type : types) {
		wire_types_.push_back(StripEnums(type));
		needs_cast_ = needs_cast_ || wire_types_.back() != type;
	}
	auto props = context.GetClientProperties();
	ArrowConverter::ToArrowSchema(&schema_, wire_types_, names, props);
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
	auto props = context_.GetClientProperties();
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> empty_extensions;

	DataChunk cast_chunk;
	DataChunk *to_encode = &chunk;
	if (needs_cast_) {
		cast_chunk.Initialize(Allocator::DefaultAllocator(), wire_types_);
		for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
			if (chunk.data[col].GetType() == wire_types_[col]) {
				cast_chunk.data[col].Reference(chunk.data[col]);
			} else {
				VectorOperations::Cast(context_, chunk.data[col], cast_chunk.data[col], chunk.size());
			}
		}
		cast_chunk.SetCardinality(chunk.size());
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
