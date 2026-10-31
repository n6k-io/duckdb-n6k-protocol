#pragma once

#include "duckdb.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct ArrowArrayStream;

namespace duckdb {

class Logger;

namespace n6k {

struct WsTableRow {
	string schema;
	string name;
	vector<string> verbs;
	vector<string> primary_keys;
};

struct WsSchemaRow {
	string name;
	vector<string> verbs;
};

struct WsTableList {
	vector<WsTableRow> tables;
	vector<WsSchemaRow> schemas;
};

class RequestState;

// `hello`: negotiated once per socket (HELLO_ACK). `open`: negotiated for this catalog (OPEN_ACK).
struct NegotiatedCapabilities {
	std::vector<std::string> hello;
	std::vector<std::string> open;
};

} // namespace n6k

// Every call polls it during a reconnect it runs. Only the streaming calls (Query, RpcScalar, RpcTable) also poll
// it between waits; the others cannot be cancelled once the request is sent. Empty = never cancel. Must stay valid
// for as long as the stream it is passed with.
using StreamCancelCheck = std::function<bool()>;

class CatalogSession {
public:
	virtual ~CatalogSession() = default;

	virtual vector<string> ListSchemas() = 0;
	// `txn` >= 0 lists and describes inside that server transaction.
	virtual n6k::WsTableList ListTables(int64_t txn) = 0;
	virtual void FetchTableSchema(const string &schema, const string &table, int64_t txn, ArrowArrayStream *out) = 0;
	// Start any op with a pre-serialized JSON body and hand back the raw RequestState so a source can
	// drive it frame-by-frame (TryNext / SetOnFrame / Credit). The request is already in flight on
	// return.
	virtual std::shared_ptr<n6k::RequestState> StartOpRequest(uint8_t op, const string &body_json,
	                                                          const StreamCancelCheck &should_cancel) = 0;
	// Asked per attached catalog, because one duckdb instance can hold several n6k servers of
	// different vintages, and because the aggregate-pushdown rule must decide while planning -- a
	// run-time NotSupported comes too late to fall back.
	virtual n6k::NegotiatedCapabilities GetCapabilities() = 0;
	// A capability name is defined at exactly one level, so either list answers it.
	bool HasCapability(const string &name) {
		auto caps = GetCapabilities();
		for (auto *level : {&caps.hello, &caps.open}) {
			if (std::find(level->begin(), level->end(), name) != level->end()) {
				return true;
			}
		}
		return false;
	}
	virtual int64_t Insert(const string &schema, const string &table, const uint8_t *data, size_t len,
	                       const StreamCancelCheck &should_cancel) = 0;
	virtual int64_t TxnBegin() = 0;
	virtual void TxnEnd(int64_t txn, bool commit) = 0;
	virtual int64_t Exec(const string &sql, const StreamCancelCheck &should_cancel) = 0;
	virtual void Query(const string &sql, ArrowArrayStream *out, StreamCancelCheck should_cancel) = 0;
	virtual void RpcScalar(const string &function, const string &args_json, ArrowArrayStream *out,
	                       StreamCancelCheck should_cancel) = 0;
	virtual void RpcTable(const string &function, const string &args_json, const uint8_t *data, size_t len,
	                      ArrowArrayStream *out, StreamCancelCheck should_cancel) = 0;
	virtual void CreateTable(const string &schema, const string &name, const string &columns_json,
	                         const StreamCancelCheck &should_cancel) = 0;
	virtual void AlterTable(const string &schema, const string &table, const string &kind, const string &details_json,
	                        const StreamCancelCheck &should_cancel) = 0;
	// An op answered by a bare RESP_END {ok}.
	virtual void RunOp(uint8_t op, const string &body_json, const StreamCancelCheck &should_cancel) = 0;

	// Handler for server-initiated PUSH frames (opcode, JSON body).
	using PushHandler = std::function<void(uint8_t op, const std::string &body)>;
	virtual void SetPushHandler(PushHandler handler) = 0;

	// Synchronously drain queued PUSH events to the handler.
	virtual void PollPushEvents() = 0;

	// Idempotently tear down the connection on DETACH (not when the last shared_ptr drops).
	virtual void Detach() = 0;

	// The server's name for this catalog: what OPEN sent.
	virtual const string &ServerCatalog() const = 0;
	// Equal for every session riding one host socket (wsFd / wsId); null for a socket of the session's own.
	virtual const void *SocketGroup() const = 0;

	// catalog_name = ATTACH alias; server_catalog defaults to it; ws_id/ws_fd ride a host socket.
	static std::shared_ptr<CatalogSession> Create(const string &base_url, const string &token,
	                                              const string &catalog_name, const string &server_catalog,
	                                              const string &ws_id, int ws_fd, const StreamCancelCheck &should_abort,
	                                              shared_ptr<Logger> logger = nullptr);
};

} // namespace duckdb
