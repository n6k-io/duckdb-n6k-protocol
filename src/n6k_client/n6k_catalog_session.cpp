// A catalog session on a ConnectionHub, the same on native and wasm.

#include "connection_hub.hpp"
#include "hub_session.hpp"
#include "n6k_body_builders.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_err_throw.hpp"
#include "n6k_fetch.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_str_utils.hpp"
#include "n6k_ws_arrow_stream.hpp"
#include "ws_client.hpp"
#include "ws_json.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace duckdb {

using n6k::FLAG_END_OF_STREAM;
using n6k::FLAG_IS_ARROW_IPC;
using n6k::FrameType;
using n6k::JsonGetBool;
using n6k::JsonGetStr;
using n6k::JsonGetUint;
using n6k::ThrowN6kError;

namespace {

// The request is already on the wire when these wait, so an expired deadline is not "no result" -- the
// server may well have run the statement. Every collector below therefore has to tell a timeout apart
// from a clean end-of-stream and throw, or a stalled INSERT/EXEC reports "0 rows, no error".
constexpr std::chrono::seconds kResponseTimeout {30};
constexpr std::chrono::seconds kSchemaListTimeout {10};

void SendRequestAndCollectResponse(n6k::WsClient &client, const std::string &catalog, uint8_t op,
                                   const std::string &body, std::string &out_arrow, std::string &out_json_chunk,
                                   std::string &out_end_body, const StreamCancelCheck &should_cancel) {
	auto state = client.AwaitReadySessionAndSendRequest(op, body, should_cancel);
	n6k::Frame f;
	bool saw_end = false;
	while (state->Next(f, kResponseTimeout)) {
		if (f.type == FrameType::ERR) {
			ThrowN6kError(catalog, op, f.payload);
		}
		if (f.type == FrameType::RESP_SCHEMA && (f.flags & FLAG_IS_ARROW_IPC)) {
			out_arrow.append(f.payload);
		} else if (f.type == FrameType::RESP_CHUNK) {
			if (f.flags & FLAG_IS_ARROW_IPC) {
				out_arrow.append(f.payload);
			} else if (out_json_chunk.empty()) {
				out_json_chunk = f.payload;
			}
		} else if (f.type == FrameType::RESP_END) {
			out_end_body = f.payload;
		}
		if ((f.flags & FLAG_END_OF_STREAM) != 0) {
			saw_end = true;
			break;
		}
	}
	if (!saw_end) {
		throw IOException("n6k[%s] %s: server did not finish responding within %llu seconds", catalog, n6k::OpName(op),
		                  static_cast<uint64_t>(kResponseTimeout.count()));
	}
}

void SendRequestAndReadArrow(n6k::WsClient &client, const std::string &catalog, uint8_t op, const std::string &body,
                             ArrowArrayStream *out, const StreamCancelCheck &should_cancel) {
	std::string arrow, json_chunk, end_body;
	SendRequestAndCollectResponse(client, catalog, op, body, arrow, json_chunk, end_body, should_cancel);
	if (arrow.empty()) {
		throw IOException("n6k[%s] %s: server returned no Arrow payload", catalog, n6k::OpName(op));
	}
	auto *buf = static_cast<uint8_t *>(std::malloc(arrow.size()));
	if (!buf) {
		throw IOException("n6k[%s] %s: OOM allocating Arrow buffer (%lu bytes)", catalog, n6k::OpName(op),
		                  static_cast<uint64_t>(arrow.size()));
	}
	// NOLINTNEXTLINE(bugprone-not-null-terminated-result)
	std::memcpy(buf, arrow.data(), arrow.size());
	AdoptIpcBufferAsArrayStream(buf, static_cast<int64_t>(arrow.size()), out);
}

int64_t SendRequestAndReadRowcount(n6k::WsClient &client, const std::string &catalog, uint8_t op,
                                   const std::string &body, const StreamCancelCheck &should_cancel) {
	std::string arrow, json_chunk, end_body;
	SendRequestAndCollectResponse(client, catalog, op, body, arrow, json_chunk, end_body, should_cancel);
	n6k::JsonDoc doc(end_body);
	auto *rowcount = doc.Parsed() ? duckdb_yyjson::yyjson_obj_get(doc.Root(), "rowcount") : nullptr;
	if (!rowcount || !duckdb_yyjson::yyjson_is_int(rowcount)) {
		throw IOException("n6k[%s] %s: server response has no rowcount", catalog, n6k::OpName(op));
	}
	return duckdb_yyjson::yyjson_get_sint(rowcount);
}

} // namespace

class WsCatalogSession : public CatalogSession {
public:
	// `on_detach` runs once, after the client closes.
	WsCatalogSession(std::shared_ptr<n6k::WsClient> client, string catalog_name, string server_catalog,
	                 const void *socket_group, std::function<void()> on_detach = nullptr)
	    : client_(std::move(client)), catalog_name_(std::move(catalog_name)),
	      server_catalog_(std::move(server_catalog)), socket_group_(socket_group), on_detach_(std::move(on_detach)) {
	}

	const string &ServerCatalog() const override {
		return server_catalog_;
	}
	const void *SocketGroup() const override {
		return socket_group_;
	}

	vector<string> ListSchemas() override {
		auto state = client_->AwaitReadySessionAndSendRequest(n6k::OP_SCHEMA_LIST, std::string(), StreamCancelCheck());
		vector<string> schemas;
		n6k::Frame f;
		bool saw_end = false;
		while (state->Next(f, kSchemaListTimeout)) {
			if (f.type == FrameType::ERR) {
				ThrowN6kError(catalog_name_, n6k::OP_SCHEMA_LIST, f.payload);
			}
			if (f.type == FrameType::RESP_CHUNK) {
				n6k::JsonDoc doc(f.payload);
				if (!doc.Parsed() ||
				    !duckdb_yyjson::yyjson_is_arr(duckdb_yyjson::yyjson_obj_get(doc.Root(), "schemas"))) {
					throw IOException("n6k[%s] %s: server returned no \"schemas\" list", catalog_name_,
					                  n6k::OpName(n6k::OP_SCHEMA_LIST));
				}
				auto chunk = n6k::JsonGetStrArray(doc.Root(), "schemas");
				schemas.insert(schemas.end(), chunk.begin(), chunk.end());
			}
			if ((f.flags & FLAG_END_OF_STREAM) != 0) {
				saw_end = true;
				break;
			}
		}
		// An empty vector otherwise reads as "this catalog has no schemas", and the caller caches that.
		if (!saw_end) {
			throw IOException("n6k[%s] %s: server did not finish responding within %llu seconds", catalog_name_,
			                  n6k::OpName(n6k::OP_SCHEMA_LIST), static_cast<uint64_t>(kSchemaListTimeout.count()));
		}
		return schemas;
	}

	n6k::WsTableList ListTables(int64_t txn) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_TABLES_LIST, n6k_body::TablesList(txn), arrow,
		                              json_chunk, end_body, StreamCancelCheck());
		n6k::WsTableList out;
		n6k::JsonDoc doc(json_chunk);
		auto *arr = doc.Parsed() ? duckdb_yyjson::yyjson_obj_get(doc.Root(), "tables") : nullptr;
		if (!arr || !duckdb_yyjson::yyjson_is_arr(arr)) {
			throw IOException("n6k[%s] %s: server returned no \"tables\" list", catalog_name_,
			                  n6k::OpName(n6k::OP_TABLES_LIST));
		}
		size_t idx, n;
		duckdb_yyjson::yyjson_val *item;
		yyjson_arr_foreach(arr, idx, n, item) {
			if (!duckdb_yyjson::yyjson_is_obj(item)) {
				throw IOException("n6k[%s] %s: malformed table entry", catalog_name_, n6k::OpName(n6k::OP_TABLES_LIST));
			}
			n6k::WsTableRow row;
			row.schema = JsonGetStr(item, "schema");
			row.name = JsonGetStr(item, "name");
			row.verbs = n6k::JsonGetStrArray(item, "verbs");
			row.primary_keys = n6k::JsonGetStrArray(item, "primary_keys");
			out.tables.emplace_back(std::move(row));
		}
		auto *schemas = duckdb_yyjson::yyjson_obj_get(doc.Root(), "schemas");
		if (!schemas || !duckdb_yyjson::yyjson_is_arr(schemas)) {
			throw IOException("n6k[%s] %s: server returned no \"schemas\" list", catalog_name_,
			                  n6k::OpName(n6k::OP_TABLES_LIST));
		}
		yyjson_arr_foreach(schemas, idx, n, item) {
			if (!duckdb_yyjson::yyjson_is_obj(item)) {
				throw IOException("n6k[%s] %s: malformed schema entry", catalog_name_,
				                  n6k::OpName(n6k::OP_TABLES_LIST));
			}
			n6k::WsSchemaRow row;
			row.name = JsonGetStr(item, "name");
			row.verbs = n6k::JsonGetStrArray(item, "verbs");
			out.schemas.emplace_back(std::move(row));
		}
		return out;
	}

	void FetchTableSchema(const string &schema, const string &table, int64_t txn, ArrowArrayStream *out) override {
		SendRequestAndReadArrow(*client_, catalog_name_, n6k::OP_TABLE_SCHEMA,
		                        n6k_body::SchemaTable(schema, table, txn), out, StreamCancelCheck());
	}

	bool HasFunction(const string &name, const vector<string> &args) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_HAS_FUNCTION, n6k_body::HasFunction(name, args),
		                              arrow, json_chunk, end_body, StreamCancelCheck());
		n6k::JsonDoc doc(end_body);
		auto *exists = doc.Parsed() ? duckdb_yyjson::yyjson_obj_get(doc.Root(), "exists") : nullptr;
		if (!exists || !duckdb_yyjson::yyjson_is_bool(exists)) {
			throw IOException("n6k[%s] %s: server response has no \"exists\"", catalog_name_,
			                  n6k::OpName(n6k::OP_HAS_FUNCTION));
		}
		return duckdb_yyjson::yyjson_get_bool(exists);
	}

	std::shared_ptr<n6k::RequestState> StartOpRequest(uint8_t op, const string &body_json,
	                                                  const StreamCancelCheck &should_cancel) override {
		return client_->AwaitReadySessionAndSendRequest(op, body_json, should_cancel);
	}

	n6k::NegotiatedCapabilities GetCapabilities() override {
		return n6k::NegotiatedCapabilities {client_->GetHelloCapabilities(), client_->GetOpenAck().capabilities};
	}

	int64_t Insert(const string &schema, const string &table, const uint8_t *data, size_t len,
	               const StreamCancelCheck &should_cancel) override {
		std::string body = n6k_body::SchemaTable(schema, table);
		body.push_back('\n');
		body.append(reinterpret_cast<const char *>(data), len);
		return SendRequestAndReadRowcount(*client_, catalog_name_, n6k::OP_INSERT, body, should_cancel);
	}

	int64_t Exec(const string &sql, const StreamCancelCheck &should_cancel) override {
		return SendRequestAndReadRowcount(*client_, catalog_name_, n6k::OP_EXEC, n6k_body::Sql(sql), should_cancel);
	}

	int64_t TxnBegin() override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_TXN_BEGIN, "{}", arrow, json_chunk, end_body,
		                              StreamCancelCheck());
		n6k::JsonDoc doc(end_body);
		if (!doc.Parsed()) {
			throw IOException("n6k[%s] TXN_BEGIN: server returned no transaction id", catalog_name_);
		}
		auto *txn = duckdb_yyjson::yyjson_obj_get(doc.Root(), "txn");
		if (!txn || !duckdb_yyjson::yyjson_is_int(txn)) {
			throw IOException("n6k[%s] TXN_BEGIN: server returned no transaction id", catalog_name_);
		}
		return duckdb_yyjson::yyjson_get_sint(txn);
	}

	void TxnEnd(int64_t txn, bool commit) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, commit ? n6k::OP_TXN_COMMIT : n6k::OP_TXN_ROLLBACK,
		                              n6k_body::Txn(txn), arrow, json_chunk, end_body, StreamCancelCheck());
	}

	void Query(const string &sql, ArrowArrayStream *out, StreamCancelCheck should_cancel) override {
		auto req = client_->AwaitReadySessionAndSendRequest(n6k::OP_QUERY, n6k_body::Sql(sql), should_cancel);
		n6k::StartStreamingArrowFromRequestState(catalog_name_, n6k::OP_QUERY, std::move(req), out,
		                                         std::move(should_cancel));
	}

	void RpcScalar(const string &function, const string &args_json, ArrowArrayStream *out,
	               StreamCancelCheck should_cancel) override {
		auto req = client_->AwaitReadySessionAndSendRequest(n6k::OP_RPC_SCALAR, n6k_body::Rpc(function, args_json),
		                                                    should_cancel);
		n6k::StartStreamingArrowFromRequestState(catalog_name_, n6k::OP_RPC_SCALAR, std::move(req), out,
		                                         std::move(should_cancel));
	}

	void RpcTable(const string &function, const string &args_json, const uint8_t *data, size_t len,
	              ArrowArrayStream *out, StreamCancelCheck should_cancel) override {
		std::string body = n6k_body::Rpc(function, args_json);
		body.push_back('\n');
		body.append(reinterpret_cast<const char *>(data), len);
		auto req = client_->AwaitReadySessionAndSendRequest(n6k::OP_RPC_TABLE, body, should_cancel);
		n6k::StartStreamingArrowFromRequestState(catalog_name_, n6k::OP_RPC_TABLE, std::move(req), out,
		                                         std::move(should_cancel));
	}

	void CreateTable(const string &schema, const string &name, const string &columns_json,
	                 const StreamCancelCheck &should_cancel) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_CREATE_TABLE,
		                              n6k_body::CreateTable(schema, name, columns_json), arrow, json_chunk, end_body,
		                              should_cancel);
	}

	void AlterTable(const string &schema, const string &table, const string &kind, const string &details_json,
	                const StreamCancelCheck &should_cancel) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, n6k::OP_ALTER_TABLE,
		                              n6k_body::AlterTable(schema, table, kind, details_json), arrow, json_chunk,
		                              end_body, should_cancel);
	}

	void RunOp(uint8_t op, const string &body_json, const StreamCancelCheck &should_cancel) override {
		std::string arrow, json_chunk, end_body;
		SendRequestAndCollectResponse(*client_, catalog_name_, op, body_json, arrow, json_chunk, end_body,
		                              should_cancel);
	}

	void SetPushHandler(PushHandler handler) override {
		client_->SetPushHandler(std::move(handler));
	}

	void MarkDisconnectedIfConnectionReplaced() override {
		client_->MarkDisconnectedIfConnectionReplaced();
	}

	void Detach() override {
		if (detached_) {
			return;
		}
		detached_ = true;
		client_->Close();
		if (on_detach_) {
			on_detach_();
		}
	}

private:
	std::shared_ptr<n6k::WsClient> client_;
	string catalog_name_;
	string server_catalog_;
	const void *socket_group_;
	std::function<void()> on_detach_;
	bool detached_ = false;
};

std::shared_ptr<CatalogSession>
CatalogSession::CreateOnConnection(const std::shared_ptr<n6k::ConnectionHub> &hub, std::function<void()> on_detach,
                                   const string &catalog_name, const string &server_catalog,
                                   const StreamCancelCheck &should_abort, shared_ptr<Logger> logger) {
	const string hello_catalog = server_catalog.empty() ? catalog_name : server_catalog;

	n6k::WsClientOptions opts;
	opts.logger = std::move(logger);
	opts.connect_timeout = std::chrono::seconds(5);
	opts.catalog = hello_catalog;
	opts.open_channel = [hub](uint64_t ns, const n6k::AbortCheck &abort) {
		hub->EnsureConnected(abort);
		return std::unique_ptr<n6k::Reactor>(new n6k::HubSession(hub, ns));
	};
	opts.hello_capabilities = [hub]() {
		return hub->HelloCapabilities();
	};

	auto client = n6k::WsClient::Create(opts);
	client->Connect(should_abort);
	return std::make_shared<WsCatalogSession>(std::move(client), catalog_name, hello_catalog, hub.get(),
	                                          std::move(on_detach));
}

} // namespace duckdb
