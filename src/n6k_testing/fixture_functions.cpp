#include "fixture_functions.hpp"

#include "serve_bind_common.hpp"

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <chrono>
#include <thread>

namespace duckdb {
namespace n6k {

namespace {

// n6k_testing_stream_counter(count[, interval_ms]) -> (n BIGINT)
//
// Emits n = 0, 1, 2, ... and stops after `count` rows; count <= 0 streams forever. The unbounded
// form is the point of it: RPC has to stay usable for a subscription that never ends, which means
// the reactor must pace it on credit and stop it on cancel rather than buffer it. No SQL construct
// produces an endless relation, so this cannot be a macro.
//
// `interval_ms` sleeps between chunks, which is what makes delivery observably incremental instead
// of arriving in one burst once the pipeline fills.
struct StreamCounterBind : public TableFunctionData {
	int64_t count = 0;
	int64_t interval_ms = 0;
};

struct StreamCounterState : public GlobalTableFunctionState {
	int64_t emitted = 0;
};

unique_ptr<FunctionData> StreamCounterBindFn(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	names = {"n"};
	return_types = {LogicalType::BIGINT};

	auto result = make_uniq<StreamCounterBind>();
	if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
		result->count = input.inputs[0].GetValue<int64_t>();
	}
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		result->interval_ms = input.inputs[1].GetValue<int64_t>();
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> StreamCounterInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<StreamCounterState>();
}

void StreamCounterScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<StreamCounterBind>();
	auto &state = data_p.global_state->Cast<StreamCounterState>();

	const bool unbounded = bind_data.count <= 0;
	if (!unbounded && state.emitted >= bind_data.count) {
		output.SetCardinality(0);
		return;
	}
	if (bind_data.interval_ms > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(bind_data.interval_ms));
	}

	// One row per chunk when pacing, so each sleep produces one observable batch. Otherwise fill a
	// vector at a time so an unbounded stream is not needlessly slow.
	idx_t want = bind_data.interval_ms > 0 ? 1 : STANDARD_VECTOR_SIZE;
	if (!unbounded) {
		const int64_t left = bind_data.count - state.emitted;
		if (static_cast<int64_t>(want) > left) {
			want = static_cast<idx_t>(left);
		}
	}

	auto values = FlatVector::GetData<int64_t>(output.data[0]);
	for (idx_t i = 0; i < want; i++) {
		values[i] = state.emitted + static_cast<int64_t>(i);
	}
	state.emitted += static_cast<int64_t>(want);
	output.SetCardinality(want);
}

// n6k_testing_sum_table(TABLE) -> (total BIGINT)
//
// Declares a TABLE parameter, so OP_RPC_TABLE hands it the pushed rows as a sub-select rather than
// as a view name. That branch has no other coverage: a SQL macro cannot declare a table parameter,
// which is exactly why the server decides the calling convention per target.
struct SumTableBind : public TableFunctionData {};

struct SumTableState : public GlobalTableFunctionState {
	int64_t total = 0;
	bool done = false;
};

unique_ptr<FunctionData> SumTableBindFn(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	names = {"total"};
	return_types = {LogicalType::BIGINT};
	return make_uniq<SumTableBind>();
}

unique_ptr<GlobalTableFunctionState> SumTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SumTableState>();
}

// In-out: called once per input chunk, then once more with the final flag set.
OperatorResultType SumTableInOut(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                 DataChunk &output) {
	auto &state = data_p.global_state->Cast<SumTableState>();
	// Read through Value rather than a typed pointer: the input relation's column type is whatever
	// the caller supplied — Arrow rows arrive as BIGINT, but a plain VALUES list is INTEGER, and
	// asserting one physical type turns a type mismatch into an internal error.
	for (idx_t i = 0; i < input.size() && input.ColumnCount() > 0; i++) {
		const auto value = input.data[0].GetValue(i);
		if (!value.IsNull()) {
			state.total += value.GetValue<int64_t>();
		}
	}
	output.SetCardinality(0);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType SumTableFinal(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<SumTableState>();
	if (state.done) {
		output.SetCardinality(0);
		return OperatorFinalizeResultType::FINISHED;
	}
	state.done = true;
	FlatVector::GetData<int64_t>(output.data[0])[0] = state.total;
	output.SetCardinality(1);
	return OperatorFinalizeResultType::FINISHED;
}

// n6k_testing_served_catalogs() -> (catalog VARCHAR)
//
// What a zero-argument `CALL n6k_serve_*()` would serve, without serving anything. The rule decides
// whether a session is single-catalog or multiplexed, which changes the wire, so it needs a test —
// and it cannot be asserted through the serve functions themselves because they block once bound.
struct ServedCatalogsBind : public TableFunctionData {
	vector<std::string> catalogs;
};

struct ServedCatalogsState : public GlobalTableFunctionState {
	idx_t emitted = 0;
};

unique_ptr<FunctionData> ServedCatalogsBindFn(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	names = {"catalog"};
	return_types = {LogicalType::VARCHAR};

	auto result = make_uniq<ServedCatalogsBind>();
	result->catalogs = ResolveServedCatalogs(context, input.inputs, 0, "n6k_testing_served_catalogs");
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ServedCatalogsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ServedCatalogsState>();
}

void ServedCatalogsScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ServedCatalogsBind>();
	auto &state = data_p.global_state->Cast<ServedCatalogsState>();

	idx_t count = 0;
	while (state.emitted < bind_data.catalogs.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(bind_data.catalogs[state.emitted]));
		state.emitted++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterN6kTestingFixtures(ExtensionLoader &loader) {
	TableFunction counter("n6k_testing_stream_counter", {LogicalType::BIGINT}, StreamCounterScan, StreamCounterBindFn,
	                      StreamCounterInit);
	counter.varargs = LogicalType::BIGINT;
	loader.RegisterFunction(CreateTableFunctionInfo(std::move(counter)));

	TableFunction sum_table("n6k_testing_sum_table", {LogicalType::TABLE}, nullptr, SumTableBindFn, SumTableInit);
	sum_table.in_out_function = SumTableInOut;
	sum_table.in_out_function_final = SumTableFinal;
	loader.RegisterFunction(CreateTableFunctionInfo(std::move(sum_table)));

	TableFunction served_catalogs("n6k_testing_served_catalogs", {}, ServedCatalogsScan, ServedCatalogsBindFn,
	                              ServedCatalogsInit);
	// Same shape as the serve functions: no arguments enumerates, names select.
	served_catalogs.varargs = LogicalType::VARCHAR;
	loader.RegisterFunction(CreateTableFunctionInfo(std::move(served_catalogs)));
}

} // namespace n6k
} // namespace duckdb
