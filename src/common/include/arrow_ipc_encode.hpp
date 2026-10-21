#pragma once

#include "duckdb.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <string>

namespace duckdb {
namespace n6k {

// One IPC message per call (schema, then batches). Shared: the server splits the messages across
// protocol frames, while the provider path concatenates them into one blob for a Python UDF.
class ArrowIpcStreamEncoder {
public:
	ArrowIpcStreamEncoder(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names);
	~ArrowIpcStreamEncoder();

	ArrowIpcStreamEncoder(const ArrowIpcStreamEncoder &) = delete;
	ArrowIpcStreamEncoder &operator=(const ArrowIpcStreamEncoder &) = delete;

	// The Arrow IPC schema message. Call once, first.
	std::string SchemaMessage();
	std::string EncodeChunk(DataChunk &chunk);
	// May be empty. Call once, last.
	std::string TakeEndOfStreamMessage();

private:
	std::string TakeNewlyWrittenBytes();

	ClientContext &context_;
	// ENUM columns are transmitted as VARCHAR: nanoarrow's IPC writer cannot emit dictionary
	// batches, and DuckDB's Arrow export renders ENUM as a dictionary field unconditionally.
	vector<LogicalType> wire_types_;
	bool needs_cast_ = false;
	ArrowSchema schema_;
	ArrowBuffer buf_;
	ArrowIpcOutputStream out_stream_;
	ArrowIpcWriter writer_;
	size_t cursor_ = 0;
	bool finalized_ = false;
};

} // namespace n6k
} // namespace duckdb
