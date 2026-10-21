#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_fetch.hpp"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

#include <memory>

namespace duckdb {

struct N6kStreamData {
	ArrowArrayStream stream;
	bool consumed;

	N6kStreamData() : consumed(false) {
		stream.release = nullptr;
	}

	~N6kStreamData() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

struct N6kScanFunctionData : public ArrowScanFunctionData {
	unique_ptr<N6kStreamData> owned_stream_data;

	N6kScanFunctionData(stream_factory_produce_t producer, unique_ptr<N6kStreamData> stream_data)
	    : ArrowScanFunctionData(producer, reinterpret_cast<uintptr_t>(stream_data.get())),
	      owned_stream_data(std::move(stream_data)) {
		// Full materialization at bind can't honor projected column_ids; disable pushdown (must match table fns).
		projection_pushdown_enabled = false;
	}
};

static unique_ptr<ArrowArrayStreamWrapper> N6kProduceStream(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
	auto *stream_data = reinterpret_cast<N6kStreamData *>(factory_ptr);

	auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
	if (stream_data->consumed) {
		wrapper->arrow_array_stream.release = nullptr;
		return wrapper;
	}

	wrapper->arrow_array_stream = stream_data->stream;
	stream_data->stream.release = nullptr;
	stream_data->consumed = true;
	wrapper->number_of_rows = -1;
	return wrapper;
}

} // namespace duckdb
