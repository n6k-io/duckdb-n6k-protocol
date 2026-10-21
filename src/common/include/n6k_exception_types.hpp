#pragma once

#include "duckdb/common/exception.hpp"

#include <string>

namespace duckdb {

class ExtensionLoader;

namespace n6k {

// DuckDB's ExceptionType <-> the protocol's `exception_type` wire value.
//
// DuckDB spells every error type three ways and only one of them belongs on the wire:
//
//   enum            ExceptionType::CATALOG
//   display string  "Catalog"              (Exception::ExceptionTypeToString)
//   message prefix  "Catalog Error: ..."   (ExceptionTypeToString + " Error: ")
//   WIRE VALUE      "CatalogException"     <- the DuckDB class name, what we must send
//
// The wire value is the class name because that is what the client reconstructs
// (N6kExceptionTypeFromClassName, src/n6k_client/include/n6k_err_throw.hpp). Sending the
// display string instead is not a loud failure: the client does not recognize it and
// silently downgrades to IOException per the protocol, so every typed error quietly
// becomes an untyped one. This lives in src/common so no server has to re-derive the
// correspondence and get it subtly wrong.

// The wire `exception_type` for a DuckDB exception type. Types with no protocol
// spelling (PLANNER, OPTIMIZER, EXECUTOR, ...) map to the documented fallback,
// "IOException", which is what a client would infer from them anyway.
const char *WireExceptionName(ExceptionType type);

bool IsRetriableExceptionType(ExceptionType type);

// The message prefix DuckDB puts in front of this type's raw message
// (`ExceptionTypeToString(type) + " Error: "`). Consumers that only see a formatted
// message string -- a Go or Rust host driving DuckDB through the C API, which exposes
// no error type -- recover the type by matching this.
std::string MessagePrefix(ExceptionType type);

// Registers `n6k_exception_types()`, a table function listing the correspondence:
//   (exception_type VARCHAR, duckdb_name VARCHAR, message_prefix VARCHAR, retriable BOOLEAN)
// so a host in any language can build the mapping at startup rather than hardcoding
// strings that go stale on a DuckDB upgrade. Idempotent across extensions.
void RegisterExceptionTypesFunction(ExtensionLoader &loader);

} // namespace n6k
} // namespace duckdb
