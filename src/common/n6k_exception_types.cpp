#include "n6k_exception_types.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <utility>
#include <vector>

namespace duckdb {
namespace n6k {

namespace {

struct WireExceptionEntry {
	ExceptionType type;
	const char *wire_name;
};

// Every DuckDB exception type the protocol has a spelling for, paired with the class
// name the client reconstructs. The set matches EXCEPTION_TYPES in
// packages/python/src/n6k_protocol/protocol.py; the sqllogictest in
// test/sql/n6k_exception_types.test pins them against each other.
//
// Types absent here (PLANNER, OPTIMIZER, EXECUTOR, INDEX, ...) have no protocol
// spelling and fall through to the documented fallback.
constexpr WireExceptionEntry WIRE_EXCEPTION_MAP[] = {
    {ExceptionType::BINDER, "BinderException"},
    {ExceptionType::CATALOG, "CatalogException"},
    {ExceptionType::CONNECTION, "ConnectionException"},
    {ExceptionType::CONSTRAINT, "ConstraintException"},
    {ExceptionType::CONVERSION, "ConversionException"},
    {ExceptionType::DEPENDENCY, "DependencyException"},
    {ExceptionType::FATAL, "FatalException"},
    {ExceptionType::HTTP, "HTTPException"},
    {ExceptionType::INTERNAL, "InternalException"},
    {ExceptionType::INTERRUPT, "InterruptException"},
    {ExceptionType::INVALID_INPUT, "InvalidInputException"},
    {ExceptionType::INVALID_TYPE, "InvalidTypeException"},
    {ExceptionType::IO, "IOException"},
    // MISMATCH_TYPE, not a "TypeMismatch" enum -- the enum and the class name disagree
    // in word order, which is exactly the kind of thing a per-server re-derivation gets
    // wrong.
    {ExceptionType::MISMATCH_TYPE, "TypeMismatchException"},
    {ExceptionType::NOT_IMPLEMENTED, "NotImplementedException"},
    {ExceptionType::OUT_OF_MEMORY, "OutOfMemoryException"},
    {ExceptionType::OUT_OF_RANGE, "OutOfRangeException"},
    {ExceptionType::PARSER, "ParserException"},
    {ExceptionType::PERMISSION, "PermissionException"},
    {ExceptionType::SEQUENCE, "SequenceException"},
    {ExceptionType::SERIALIZATION, "SerializationException"},
    {ExceptionType::SYNTAX, "SyntaxException"},
    {ExceptionType::TRANSACTION, "TransactionException"},
};

struct ExceptionTypesBindData : public TableFunctionData {};

struct ExceptionTypesState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<GlobalTableFunctionState> ExceptionTypesInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<ExceptionTypesState>();
}

unique_ptr<FunctionData> ExceptionTypesBind(ClientContext &, TableFunctionBindInput &,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	names = {"exception_type", "duckdb_name", "message_prefix", "retriable"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	return make_uniq<ExceptionTypesBindData>();
}

void ExceptionTypesScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ExceptionTypesState>();
	constexpr idx_t total = sizeof(WIRE_EXCEPTION_MAP) / sizeof(WIRE_EXCEPTION_MAP[0]);

	idx_t count = 0;
	while (state.offset < total && count < STANDARD_VECTOR_SIZE) {
		auto &entry = WIRE_EXCEPTION_MAP[state.offset];
		output.SetValue(0, count, Value(entry.wire_name));
		output.SetValue(1, count, Value(Exception::ExceptionTypeToString(entry.type)));
		output.SetValue(2, count, Value(MessagePrefix(entry.type)));
		output.SetValue(3, count, Value::BOOLEAN(IsRetriableExceptionType(entry.type)));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

const char *WireExceptionName(ExceptionType type) {
	for (auto &entry : WIRE_EXCEPTION_MAP) {
		if (entry.type == type) {
			return entry.wire_name;
		}
	}
	return "IOException";
}

bool IsRetriableExceptionType(ExceptionType type) {
	// A write-write conflict under DuckDB's optimistic concurrency: nothing is wrong,
	// the client may simply re-issue.
	return type == ExceptionType::TRANSACTION;
}

std::string MessagePrefix(ExceptionType type) {
	return Exception::ExceptionTypeToString(type) + " Error: ";
}

void RegisterExceptionTypesFunction(ExtensionLoader &loader) {
	TableFunction func("n6k_exception_types", {}, ExceptionTypesScan, ExceptionTypesBind, ExceptionTypesInitGlobal);
	// IGNORE_ON_CONFLICT makes a second extension's registration a no-op rather than an error.
	CreateTableFunctionInfo info(std::move(func));
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

} // namespace n6k
} // namespace duckdb
