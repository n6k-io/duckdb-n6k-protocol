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

struct N6kStreamData : public ArrowScanFactory {
	ArrowArrayStream stream;
	bool consumed;

	N6kStreamData() : consumed(false) {
		stream.release = nullptr;
	}

	~N6kStreamData() override {
		if (stream.release) {
			stream.release(&stream);
		}
	}

	void GetSchema(ArrowSchema &schema) override {
		if (!stream.release || stream.get_schema(&stream, &schema) != 0) {
			throw IOException("n6k: failed to read result schema");
		}
	}

	unique_ptr<ArrowArrayStreamWrapper> ProduceStream(ArrowStreamParameters &parameters) override {
		auto wrapper = make_uniq<ArrowArrayStreamWrapper>();
		if (consumed) {
			wrapper->arrow_array_stream.release = nullptr;
			return wrapper;
		}

		wrapper->arrow_array_stream = stream;
		stream.release = nullptr;
		consumed = true;
		return wrapper;
	}
};

struct N6kScanFunctionData : public ArrowScanFunctionData {
	shared_ptr<N6kStreamData> owned_stream_data;

	explicit N6kScanFunctionData(shared_ptr<N6kStreamData> stream_data)
	    : ArrowScanFunctionData(stream_data), owned_stream_data(std::move(stream_data)) {
		// Full materialization at bind can't honor projected column_ids; disable pushdown (must match table fns).
		projection_pushdown_enabled = false;
	}
};

} // namespace duckdb
