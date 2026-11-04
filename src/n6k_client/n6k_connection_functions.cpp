#include "n6k_connection_functions.hpp"

#include "n6k_served.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#ifndef WASM_LOADABLE_EXTENSIONS
#include "connection_hub.hpp"
#endif

namespace duckdb {
namespace {

static constexpr const char *DISCONNECT_FN_NAME = "n6k_disconnect";
static constexpr const char *RECONNECT_FN_NAME = "n6k_reconnect";
static constexpr const char *SEND_TEXT_FN_NAME = "n6k_send_text";
static constexpr const char *CONNECTIONS_FN_NAME = "n6k_connections";

struct ConnectionArgBindData : public TableFunctionData {
	hugeint_t connection_id;
	string text;
};

struct OnceState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<GlobalTableFunctionState> InitOnce(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<OnceState>();
}

static void ThrowIfWasm(const char *fn_name) {
#ifdef WASM_LOADABLE_EXTENSIONS
	throw NotImplementedException("%s is only supported in the native build", fn_name);
#else
	(void)fn_name;
#endif
}

static hugeint_t BindConnectionId(TableFunctionBindInput &input, const char *fn_name) {
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s requires the connection_id n6k_connect() returned", fn_name);
	}
	auto &value = input.inputs[0];
	if (value.type().id() == LogicalTypeId::UUID) {
		return value.GetValue<hugeint_t>();
	}
	hugeint_t id;
	if (!UUID::FromString(value.GetValue<string>(), id)) {
		throw BinderException("%s: '%s' is not a connection id", fn_name, value.GetValue<string>());
	}
	return id;
}

#ifndef WASM_LOADABLE_EXTENSIONS
static std::shared_ptr<n6k::ConnectionHub> FindOrThrow(ClientContext &context, const hugeint_t &id,
                                                       const char *fn_name) {
	auto hub = n6k::ConnectionRegistry::Get(DatabaseInstance::GetDatabase(context))->Find(id);
	if (!hub) {
		throw InvalidInputException("%s: no open connection %s", fn_name, UUID::ToString(id));
	}
	return hub;
}
#endif

unique_ptr<FunctionData> DisconnectBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"disconnected"};
	return_types = {LogicalType::BOOLEAN};
	n6k::ThrowIfServed(context, DISCONNECT_FN_NAME);
	ThrowIfWasm(DISCONNECT_FN_NAME);
	auto result = make_uniq<ConnectionArgBindData>();
	result->connection_id = BindConnectionId(input, DISCONNECT_FN_NAME);
	return std::move(result);
}

void DisconnectScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<OnceState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
#ifndef WASM_LOADABLE_EXTENSIONS
	auto &id = data_p.bind_data->Cast<ConnectionArgBindData>().connection_id;
	// The connection stays up while an ATTACH still rides it.
	if (!n6k::ConnectionRegistry::Get(DatabaseInstance::GetDatabase(context))
	         ->ReleaseConnectLease(id, "connection " + UUID::ToString(id) + " was disconnected")) {
		throw InvalidInputException("%s: no open connection %s", DISCONNECT_FN_NAME, UUID::ToString(id));
	}
#else
	(void)context;
#endif
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
}

unique_ptr<FunctionData> ReconnectBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"capabilities"};
	return_types = {LogicalType::LIST(LogicalType::VARCHAR)};
	n6k::ThrowIfServed(context, RECONNECT_FN_NAME);
	ThrowIfWasm(RECONNECT_FN_NAME);
	auto result = make_uniq<ConnectionArgBindData>();
	result->connection_id = BindConnectionId(input, RECONNECT_FN_NAME);
	return std::move(result);
}

void ReconnectScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<OnceState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
	vector<Value> capabilities;
#ifndef WASM_LOADABLE_EXTENSIONS
	auto hub = FindOrThrow(context, data_p.bind_data->Cast<ConnectionArgBindData>().connection_id, RECONNECT_FN_NAME);
	hub->Reconnect([&context] { return context.IsInterrupted(); });
	for (auto &cap : hub->HelloCapabilities()) {
		capabilities.emplace_back(cap);
	}
#else
	(void)context;
#endif
	output.SetValue(0, 0, Value::LIST(LogicalType::VARCHAR, std::move(capabilities)));
	output.SetCardinality(1);
}

unique_ptr<FunctionData> SendTextBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"sent"};
	return_types = {LogicalType::BOOLEAN};
	n6k::ThrowIfServed(context, SEND_TEXT_FN_NAME);
	ThrowIfWasm(SEND_TEXT_FN_NAME);
	auto result = make_uniq<ConnectionArgBindData>();
	result->connection_id = BindConnectionId(input, SEND_TEXT_FN_NAME);
	if (input.inputs.size() < 2 || input.inputs[1].IsNull()) {
		throw BinderException("%s requires the text to send", SEND_TEXT_FN_NAME);
	}
	result->text = input.inputs[1].GetValue<string>();
	return std::move(result);
}

void SendTextScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<OnceState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
#ifndef WASM_LOADABLE_EXTENSIONS
	auto &bind_data = data_p.bind_data->Cast<ConnectionArgBindData>();
	auto hub = FindOrThrow(context, bind_data.connection_id, SEND_TEXT_FN_NAME);
	if (!hub->IsAlive() || !hub->SendText(bind_data.text)) {
		throw IOException("%s: connection %s is not open", SEND_TEXT_FN_NAME, hub->IdString());
	}
#else
	(void)context;
#endif
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
}

struct ConnectionsState : public GlobalTableFunctionState {
	vector<vector<Value>> rows;
	idx_t offset = 0;
};

unique_ptr<FunctionData> ConnectionsBind(ClientContext &context, TableFunctionBindInput &,
                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"connection_id", "alive", "capabilities", "catalogs", "leases"};
	return_types = {LogicalType::UUID, LogicalType::BOOLEAN, LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::BIGINT, LogicalType::BIGINT};
	n6k::ThrowIfServed(context, CONNECTIONS_FN_NAME);
	ThrowIfWasm(CONNECTIONS_FN_NAME);
	return make_uniq<TableFunctionData>();
}

unique_ptr<GlobalTableFunctionState> ConnectionsInit(ClientContext &context, TableFunctionInitInput &) {
	auto state = make_uniq<ConnectionsState>();
#ifndef WASM_LOADABLE_EXTENSIONS
	for (auto &listed : n6k::ConnectionRegistry::Get(DatabaseInstance::GetDatabase(context))->List()) {
		auto &hub = listed.hub;
		vector<Value> capabilities;
		for (auto &cap : hub->HelloCapabilities()) {
			capabilities.emplace_back(cap);
		}
		state->rows.push_back({Value::UUID(hub->Id()), Value::BOOLEAN(hub->IsAlive()),
		                       Value::LIST(LogicalType::VARCHAR, std::move(capabilities)),
		                       Value::BIGINT(static_cast<int64_t>(hub->SessionCount())),
		                       Value::BIGINT(static_cast<int64_t>(listed.leases))});
	}
#else
	(void)context;
#endif
	return std::move(state);
}

void ConnectionsScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ConnectionsState>();
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset];
		for (idx_t col = 0; col < row.size(); col++) {
			output.SetValue(col, count, row[col]);
		}
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterN6kConnectionFunctions(ExtensionLoader &loader) {
	// The id as UUID or as its text, which is how most hosts carry it.
	TableFunctionSet disconnect(DISCONNECT_FN_NAME);
	TableFunctionSet reconnect(RECONNECT_FN_NAME);
	TableFunctionSet send_text(SEND_TEXT_FN_NAME);
	for (auto &id_type : {LogicalType::UUID, LogicalType::VARCHAR}) {
		disconnect.AddFunction(TableFunction({id_type}, DisconnectScan, DisconnectBind, InitOnce));
		reconnect.AddFunction(TableFunction({id_type}, ReconnectScan, ReconnectBind, InitOnce));
		send_text.AddFunction(TableFunction({id_type, LogicalType::VARCHAR}, SendTextScan, SendTextBind, InitOnce));
	}
	loader.RegisterFunction(disconnect);
	loader.RegisterFunction(reconnect);
	loader.RegisterFunction(send_text);
	loader.RegisterFunction(TableFunction(CONNECTIONS_FN_NAME, {}, ConnectionsScan, ConnectionsBind, ConnectionsInit));
}

} // namespace duckdb
