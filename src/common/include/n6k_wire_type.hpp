#pragma once

#include "duckdb.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string>

namespace duckdb {
namespace n6k {

constexpr const char *DUCKDB_TYPE_EXTENSION = "n6k.duckdb_type";

inline LogicalType StripEnums(const LogicalType &type) {
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

inline void CastChunkToWireTypes(ClientContext &context, DataChunk &chunk, const vector<LogicalType> &wire_types,
                                 DataChunk &out) {
	out.Initialize(Allocator::DefaultAllocator(), wire_types);
	for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
		if (chunk.data[col].GetType() == wire_types[col]) {
			out.data[col].Reference(chunk.data[col]);
		} else {
			VectorOperations::Cast(context, chunk.data[col], out.data[col], chunk.size());
		}
	}
	out.SetChildCardinality(chunk.size());
}

// Base64 because nanoarrow's IPC reader sizes metadata values with strlen.
inline std::string SerializeTypeToBase64(const LogicalType &type) {
	MemoryStream stream;
	BinarySerializer::Serialize(type, stream);
	return Blob::ToBase64(string_t(const_char_ptr_cast(stream.GetData()), NumericCast<uint32_t>(stream.GetPosition())));
}

inline LogicalType DeserializeTypeFromBase64(const std::string &encoded) {
	auto bytes = Blob::FromBase64(encoded);
	MemoryStream stream(data_ptr_cast(&bytes[0]), bytes.size());
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto type = LogicalType::Deserialize(deserializer);
	deserializer.End();
	return type;
}

} // namespace n6k
} // namespace duckdb
