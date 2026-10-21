// Transport-agnostic catalog session shared by native and wasm; only the Create factory differs by platform.

#include "n6k_body_builders.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_err_throw.hpp"
#include "n6k_fetch.hpp"
#include "n6k_io_thread.hpp" // shared I/O thread doorbell (wasm-only)
#include "n6k_protocol_generated.hpp"
#include "n6k_str_utils.hpp"
#include "n6k_wasm_main_thread.hpp"
#include "n6k_ws_arrow_stream.hpp"
#include "ws_client.hpp"
#include "ws_fd_hub.hpp"
#include "ws_json.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <set>

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

// Interned per host socket, so its address outlives every session that names it.
static const void *HostSocketGroup(const string &key) {
	static std::mutex mu;
	static std::set<string> keys;
	std::lock_guard<std::mutex> guard(mu);
	return &*keys.insert(key).first;
}

class WsCatalogSession : public CatalogSession {
public:
	WsCatalogSession(std::shared_ptr<n6k::WsClient> client, string catalog_name, string server_catalog,
	                 const void *socket_group)
	    : client_(std::move(client)), catalog_name_(std::move(catalog_name)),
	      server_catalog_(std::move(server_catalog)), socket_group_(socket_group) {
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

	void PollPushEvents() override {
		client_->MarkDisconnectedIfChannelReset();
	}

	// Release the WebSocket on DETACH, else transport threads keep the WsClient alive forever.
	void Detach() override {
		if (detached_) {
			return;
		}
		detached_ = true;
		client_->Close();
	}

private:
	std::shared_ptr<n6k::WsClient> client_;
	string catalog_name_;
	string server_catalog_;
	const void *socket_group_;
	bool detached_ = false;
};

#ifdef WASM_LOADABLE_EXTENSIONS

// Bytes for one vsock channel: two 1 MiB SPSC byte-rings, each prefixed by 16 B of
// control words (HEAD/TAIL/CLOSED/EPOCH). MUST equal channelBytes(DEFAULT_CHANNEL_LAYOUT)
// in packages/npm/src/virtual-socket/channel.ts = 2 * ((1<<20) + 16). Both sides build
// Int32Array/Uint8Array views over these exact bytes, so any mismatch corrupts the rings.
static constexpr size_t N6K_VSOCK_CHANNEL_BYTES = 2 * ((size_t(1) << 20) + 16);

// Wasm: the ws-worker owns the socket; Create allocates the vsock channel + worker via JS before Connect handshakes.
std::shared_ptr<CatalogSession> CatalogSession::Create(const string &base_url, const string &token,
                                                       const string &catalog_name, const string &server_catalog,
                                                       const string &ws_id, int ws_fd,
                                                       const StreamCancelCheck &should_abort) {
	(void)ws_fd;
	const string hello_catalog = server_catalog.empty() ? catalog_name : server_catalog;
	// Per-attach session id lets ONE socket carry many catalogs.
	static std::atomic<uint64_t> g_next_ns {1};
	const uint64_t ns = g_next_ns.fetch_add(1);

	// Reserve the channel's ring region in duckdb's shared wasm heap so the pump ws-worker — and
	// later compute pthreads — address the same bytes with no copy. Zero it so the ring control
	// words start empty+open. Ownership then passes to JS: it frees the region (via the wasm
	// `free` export) on detach, once the pump ws-worker has released the ring — C++ must NOT
	// free it (that races the pump's async teardown and would double-free).
	void *ring = std::malloc(N6K_VSOCK_CHANNEL_BYTES);
	if (!ring) {
		throw IOException("n6k: failed to allocate %llu-byte vsock ring region",
		                  static_cast<unsigned long long>(N6K_VSOCK_CHANNEL_BYTES));
	}
	std::memset(ring, 0, N6K_VSOCK_CHANNEL_BYTES);
	const uintptr_t ring_ptr = reinterpret_cast<uintptr_t>(ring);

	// Shared doorbell address forwarded to the pump so it wakes the one I/O thread after publishing an
	// inbound frame.
	const uintptr_t doorbell_ptr = reinterpret_cast<uintptr_t>(n6k::N6kIoThread::Instance().DoorbellAddress());

	std::string setup_js;
	if (!ws_id.empty()) {
		setup_js = "n6k.vsockAttachWs('" + n6k::EscapeSingleQuotedJsLiteral(catalog_name) + "', '" +
		           n6k::EscapeSingleQuotedJsLiteral(ws_id) + "', " + std::to_string(ns) + ", " +
		           std::to_string(ring_ptr) + ", " + std::to_string(doorbell_ptr) + ")";
	} else {
		// Pass the raw ws spec; JS resolves it (incl. a relative base_url the C++ side can't).
		string ws_spec = base_url + "/ws?catalog=" + hello_catalog;
		setup_js = "n6k.vsockAttach('" + n6k::EscapeSingleQuotedJsLiteral(catalog_name) + "', '" +
		           n6k::EscapeSingleQuotedJsLiteral(ws_spec) + "', '" + n6k::EscapeSingleQuotedJsLiteral(token) +
		           "', " + std::to_string(ring_ptr) + ", " + std::to_string(doorbell_ptr) + ")";
	}
	// Run the vsockAttach glue on the main runtime thread: under threads>1 duckdb can execute this
	// ATTACH on a compute pthread, whose JS scope has no globalThis.n6k (see n6k_wasm_main_thread.hpp).
	std::string setup_res = n6k::RunScriptStringOnMain(setup_js);
	if (setup_res.empty()) {
		setup_res = "ERROR:null from vsock attach";
	}
	if (setup_res != "OK") {
		std::free(ring);
		throw IOException("n6k: vsock attach failed: %s", setup_res);
	}

	n6k::WsClientOptions opts;
	opts.bearer_token = token;
	opts.connect_timeout = std::chrono::seconds(5);
	opts.catalog = hello_catalog;
	opts.channel_key = catalog_name;
	opts.ns = ns;
	opts.send_hello = ws_id.empty();
	opts.ring_ptr = ring_ptr;
	if (!ws_id.empty()) {
		opts.host_hello_capabilities = [ws_id]() {
			auto joined =
			    n6k::RunScriptStringOnMain("n6k.helloCapabilities('" + n6k::EscapeSingleQuotedJsLiteral(ws_id) + "')");
			std::vector<std::string> caps;
			for (auto &cap : StringUtil::Split(joined, ',')) {
				caps.push_back(cap);
			}
			return caps;
		};
	}

	auto client = n6k::WsClient::Create(opts);
	try {
		client->Connect(should_abort);
	} catch (...) {
		// Connect throws on any failure and no session is returned -- so nothing will ever call Detach,
		// and the channel JS built above (pump worker and the 2 MiB ring region it now owns) would
		// leak for the life of the page. Ask JS to close it, which is the same release path Detach
		// takes. NOT std::free(ring): the pump's teardown is async and owns those views, so freeing
		// from here races it.
		n6k::RunScriptStringOnMain("n6k.vsockClose('" + n6k::EscapeSingleQuotedJsLiteral(catalog_name) + "')");
		throw;
	}
	return std::make_shared<WsCatalogSession>(std::move(client), catalog_name, hello_catalog,
	                                          ws_id.empty() ? nullptr : HostSocketGroup("ws_id:" + ws_id));
}

#else // native

std::shared_ptr<CatalogSession> CatalogSession::Create(const string &base_url, const string &token,
                                                       const string &catalog_name, const string &server_catalog,
                                                       const string &ws_id, int ws_fd,
                                                       const StreamCancelCheck &should_abort) {
	(void)ws_id;
	const string hello_catalog = server_catalog.empty() ? catalog_name : server_catalog;

	// Per-attach session id; an inherited wsFd may carry several sessions. Never 0: the server refuses it.
	static std::atomic<uint64_t> g_next_ns {1};

	n6k::WsClientOptions opts;
	opts.bearer_token = token;
	opts.connect_timeout = std::chrono::seconds(5);
	opts.catalog = hello_catalog;
	opts.ns = g_next_ns.fetch_add(1);
	opts.send_hello = ws_fd < 0;

	if (ws_fd >= 0) {
		opts.ws_fd = ws_fd;
#ifndef _WIN32
		opts.host_hello_capabilities = [ws_fd]() {
			return n6k::FdHub::HelloCapabilities(ws_fd);
		};
#endif
	} else {
		string ws_query = "?catalog=" + hello_catalog;
		if (StringUtil::StartsWith(base_url, "https://")) {
			opts.url = "wss://" + base_url.substr(8) + "/ws" + ws_query;
		} else if (StringUtil::StartsWith(base_url, "http://")) {
			opts.url = "ws://" + base_url.substr(7) + "/ws" + ws_query;
		} else {
			throw IOException("n6k: base URL must be http(s):// or n6k(s)://, got %s", base_url);
		}
	}

	auto client = n6k::WsClient::Create(opts);
	client->Connect(should_abort);

	return std::make_shared<WsCatalogSession>(std::move(client), catalog_name, hello_catalog,
	                                          ws_fd < 0 ? nullptr : HostSocketGroup("ws_fd:" + std::to_string(ws_fd)));
}

#endif // WASM_LOADABLE_EXTENSIONS

} // namespace duckdb
