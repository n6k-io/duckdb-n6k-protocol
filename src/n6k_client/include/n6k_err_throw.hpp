#pragma once

// ERR frame: {"exception_type","exception_message"} -> mapped ExceptionType -> typed DuckDB exception.

#include "n6k_protocol_generated.hpp"
#include "ws_json.hpp"

#include "duckdb/common/exception.hpp"

#include <cstdint>
#include <string>

namespace duckdb {
namespace n6k {

struct ErrFrameBody {
	std::string exception_type;
	std::string message;
};

inline ErrFrameBody ParseErrFrame(const std::string &payload) {
	ErrFrameBody out;
	JsonDoc d(payload);
	if (!d.Parsed()) {
		out.message = payload;
		return out;
	}
	out.exception_type = JsonGetStr(d.Root(), "exception_type");
	out.message = JsonGetStr(d.Root(), "exception_message");
	if (out.message.empty()) {
		out.message = payload;
	}
	return out;
}

inline ExceptionType N6kExceptionTypeFromClassName(const std::string &name) {
	if (name == "IOException") {
		return ExceptionType::IO;
	}
	if (name == "CatalogException") {
		return ExceptionType::CATALOG;
	}
	if (name == "BinderException") {
		return ExceptionType::BINDER;
	}
	if (name == "ConstraintException") {
		return ExceptionType::CONSTRAINT;
	}
	if (name == "InvalidInputException") {
		return ExceptionType::INVALID_INPUT;
	}
	if (name == "NotImplementedException") {
		return ExceptionType::NOT_IMPLEMENTED;
	}
	if (name == "ParserException") {
		return ExceptionType::PARSER;
	}
	if (name == "SyntaxException") {
		return ExceptionType::SYNTAX;
	}
	if (name == "ConversionException") {
		return ExceptionType::CONVERSION;
	}
	if (name == "InvalidTypeException") {
		return ExceptionType::INVALID_TYPE;
	}
	if (name == "TypeMismatchException") {
		return ExceptionType::MISMATCH_TYPE;
	}
	if (name == "OutOfRangeException") {
		return ExceptionType::OUT_OF_RANGE;
	}
	if (name == "OutOfMemoryException") {
		return ExceptionType::OUT_OF_MEMORY;
	}
	if (name == "PermissionException") {
		return ExceptionType::PERMISSION;
	}
	if (name == "TransactionException") {
		return ExceptionType::TRANSACTION;
	}
	if (name == "ConnectionException") {
		return ExceptionType::CONNECTION;
	}
	if (name == "FatalException") {
		return ExceptionType::FATAL;
	}
	if (name == "InternalException") {
		return ExceptionType::INTERNAL;
	}
	if (name == "SerializationException") {
		return ExceptionType::SERIALIZATION;
	}
	if (name == "InterruptException") {
		return ExceptionType::INTERRUPT;
	}
	if (name == "SequenceException") {
		return ExceptionType::SEQUENCE;
	}
	if (name == "HTTPException") {
		return ExceptionType::HTTP;
	}
	if (name == "DependencyException") {
		return ExceptionType::DEPENDENCY;
	}
	return ExceptionType::IO;
}

inline const char *OpName(uint8_t op) {
	switch (op) {
	case OP_SCHEMA_LIST:
		return "SCHEMA_LIST";
	case OP_CATALOG_LIST:
		return "CATALOG_LIST";
	case OP_TABLES_LIST:
		return "TABLES_LIST";
	case OP_TABLE_SCHEMA:
		return "TABLE_SCHEMA";
	case OP_INSERT:
		return "INSERT";
	case OP_EXEC:
		return "EXEC";
	case OP_QUERY:
		return "QUERY";
	case OP_RPC_SCALAR:
		return "RPC_SCALAR";
	case OP_RPC_TABLE:
		return "RPC_TABLE";
	case OP_CREATE_TABLE:
		return "CREATE_TABLE";
	case OP_CREATE_VIEW:
		return "CREATE_VIEW";
	case OP_DROP_VIEW:
		return "DROP_VIEW";
	case OP_ALTER_TABLE:
		return "ALTER_TABLE";
	case OP_READ_PLAN:
		return "READ_PLAN";
	case OP_WRITE_PLAN:
		return "WRITE_PLAN";
	case OP_TXN_BEGIN:
		return "TXN_BEGIN";
	case OP_TXN_COMMIT:
		return "TXN_COMMIT";
	case OP_TXN_ROLLBACK:
		return "TXN_ROLLBACK";
	default:
		return "UNKNOWN";
	}
}

// Throws the typed exception with a `n6k[<catalog>] <OP>: <message>` prefix.
[[noreturn]] inline void ThrowN6kError(const std::string &catalog, uint8_t op, const std::string &payload) {
	auto parsed = ParseErrFrame(payload);
	auto etype = N6kExceptionTypeFromClassName(parsed.exception_type);
	std::string prefixed = "n6k[" + catalog + "] " + OpName(op) + ": " + parsed.message;
	throw Exception(etype, prefixed);
}

// Typed exception from an HTTP error response; falls back to IOException with the raw body/transport error.
[[noreturn]] inline void ThrowN6kHttpError(const std::string &method, const std::string &url, int status_code,
                                           const std::string &body, const std::string &transport_error) {
	if (!body.empty()) {
		auto parsed = ParseErrFrame(body);
		if (!parsed.exception_type.empty()) {
			auto etype = N6kExceptionTypeFromClassName(parsed.exception_type);
			std::string prefixed = "n6k HTTP " + method + " " + url + ": " + parsed.message;
			throw Exception(etype, prefixed);
		}
	}
	std::string detail = !body.empty() ? body : transport_error;
	if (detail.size() > 500) {
		detail = detail.substr(0, 500) + "...";
	}
	if (status_code > 0) {
		throw IOException("n6k HTTP %s %s failed (HTTP %d): %s", method, url, status_code, detail);
	}
	throw IOException("n6k HTTP %s %s failed: %s", method, url, detail);
}

} // namespace n6k
} // namespace duckdb
