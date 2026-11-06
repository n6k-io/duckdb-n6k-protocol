#pragma once

#include "n6k_protocol_generated.hpp"
#include "ws_transport.hpp"

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace duckdb {

class DatabaseInstance;
class Logger;

namespace n6k {

// One connection to an n6k server, shared by every catalog session riding it.
//
// The transport only moves messages; the hub owns what is connection-scoped: HELLO, PING, JSON-RPC
// text, the one writer, and routing each session-scoped frame to the session named by its `ns`. Sessions
// attach through a HubSession. A connection-scoped frame (no `ns`) reaches no session.
class ConnectionHub {
public:
	using MessageFn = Transport::MessageFn;
	using CloseFn = Transport::CloseFn;
	using AbortCheck = std::function<bool()>;

	struct Options {
		// Builds a fresh, not yet started transport. Called again on each redial.
		std::function<std::unique_ptr<Transport>()> dial;
		// A URL can be dialed again after a drop, and a registered socket bridged again once its host replaces
		// it; a wsFd cannot.
		bool can_redial = false;
		std::string token;
		// Replaces `token` before each redial; may block on a token refresh. Empty = every dial sends `token`.
		std::function<std::string()> resolve_token;
		// Offer jsonrpc/mcp in HELLO, i.e. this side takes JSON-RPC text.
		bool offer_app_capabilities = false;
		// Host UDFs: on_text(connection_id UUID, text VARCHAR) -> reply VARCHAR (NULL or empty sends nothing),
		// on_close(connection_id UUID, reason VARCHAR). Empty = none.
		std::string on_text;
		std::string on_close;
		// Names the endpoint in errors: "wsFd 7", a URL.
		std::string endpoint;
		std::chrono::milliseconds connect_timeout {5000};
		// Once the server answers HELLO with HELLO_PENDING, it fails only when this long passes with no
		// further pending, ACK or ERR.
		std::chrono::milliseconds hello_pending_stall {OPEN_PENDING_STALL_MS};
		// Receives a WsLogType row per frame the hub itself sends or consumes; null logs nothing.
		shared_ptr<Logger> logger;
		// Set when the socket's owner already ran HELLO: the hub skips it and asks this for the
		// HELLO_ACK capabilities on every (re)dial.
		std::function<std::vector<std::string>()> host_handshake;
		// A transport whose Send can block gets a writer thread of the hub's; one that never blocks is
		// sent to inline.
		bool queue_sends = true;
	};

	// `db` may be null when there are no host UDFs to call.
	static std::shared_ptr<ConnectionHub> Create(const shared_ptr<DatabaseInstance> &db, Options opts);
	// As Create, under an id a host already handed out.
	static std::shared_ptr<ConnectionHub> Create(const shared_ptr<DatabaseInstance> &db, Options opts,
	                                             const hugeint_t &id);
	~ConnectionHub();
	ConnectionHub(const ConnectionHub &) = delete;
	ConnectionHub &operator=(const ConnectionHub &) = delete;

	const hugeint_t &Id() const {
		return id_;
	}
	std::string IdString() const;

	// Dial (or redial) and run HELLO; returns at once while the connection is up. Throws when the
	// endpoint is unusable, the hub was disconnected, or a dropped socket cannot be redialed; throws
	// InterruptException once should_abort returns true.
	void EnsureConnected(const AbortCheck &should_abort);
	// Drop the current socket, if any, and dial again. Sessions bound to an open connection get on_close here,
	// others already got it at the drop; all reopen on their next request. The host's on_close UDF is not called.
	// Throws as EnsureConnected does, and for a hub that cannot redial.
	void Reconnect(const AbortCheck &should_abort);
	bool IsAlive();
	std::vector<std::string> HelloCapabilities();
	size_t SessionCount();
	// Bumped on every dial, on a Reconnect of an open connection, and on Disconnect.
	uint64_t Generation();

	// Returns the generation the session bound on. A session bound while the connection is down gets
	// on_close before this returns.
	uint64_t BindSession(uint64_t ns, MessageFn on_message, CloseFn on_close);
	// Returns once no callback for `ns` is running. From within one of its own callbacks it deadlocks.
	void UnbindSession(uint64_t ns);
	// Connection-scoped requests (e.g. OP_CATALOG_LIST) carry no `ns`; their RESP/ERR frames are routed
	// by the id returned here.
	uint32_t BindRequest(MessageFn on_message, CloseFn on_close);
	// Does not wait for a running callback, so what the callbacks capture must outlive this call.
	void UnbindRequest(uint32_t req_id);

	// Hands `frame` to the writer, or with queue_sends off to the transport inline (see Transport::Send for what
	// it may throw). Never blocks. False when the connection is not open, or is no longer `generation`; a queued
	// frame is dropped if the generation moves before it is written. True does not mean sent: a transport's
	// refusal drops the connection instead, and with queue_sends off runs every on_close on this thread.
	bool SendOnGeneration(uint64_t generation, const std::string &frame);
	bool Send(const std::string &frame);
	bool SendText(const std::string &text);

	// Permanent: the hub never dials again. While the connection is up, every bound session and request gets
	// on_close here; otherwise each already got it at the drop or when it bound.
	void Disconnect(const std::string &reason);
	void LogError(const std::exception &e);
	bool IsDisconnected();
	// Disconnected, or dropped with no way to redial.
	bool IsDead();
	bool HasHostCallbacks() const {
		return !opts_.on_text.empty() || !opts_.on_close.empty();
	}

private:
	struct Subscriber {
		MessageFn on_message;
		CloseFn on_close;
		// Callbacks running now; guarded by mu_.
		int in_flight = 0;
	};
	using SubscriberPtr = std::shared_ptr<Subscriber>;
	struct Outbound {
		uint64_t generation;
		bool binary;
		std::string bytes;
	};
	struct HostEvent {
		bool is_close;
		std::string payload;
	};
	// Shared with the detached worker, which may outlive the hub.
	struct HostQueue {
		std::mutex mu;
		std::condition_variable cv;
		std::deque<HostEvent> events;
		bool stop = false;
	};

	ConnectionHub(const shared_ptr<DatabaseInstance> &db, Options opts, const hugeint_t &id);

	void AwaitHello(uint64_t gen, const AbortCheck &should_abort);
	void LogFrame(const char *direction, const std::string &frame);
	void OnOpen(uint64_t gen);
	void OnMessage(uint64_t gen, const std::string &bytes, bool binary);
	void OnTransportClose(uint64_t gen, const std::string &reason);
	void OnBadFrame(uint64_t gen, Transport &transport, const std::string &error);
	void Route(const std::string &frame, int64_t ns, int64_t type, int64_t req_id);
	// Under mu_: counts the subscriber's callback as in flight until Dispatch returns.
	static void HoldLocked(std::vector<SubscriberPtr> &held, const SubscriberPtr &sub);
	void Dispatch(std::vector<SubscriberPtr> &held, const std::function<void(Subscriber &)> &fn);
	void Enqueue(uint64_t generation, bool binary, std::string bytes);
	void WriterLoop();
	void Write(const Outbound &out);
	void PostHostEvent(bool is_close, std::string payload);
	void StopHostWorker();
	static void HostWorkerLoop(std::shared_ptr<HostQueue> queue, const std::weak_ptr<ConnectionHub> &hub,
	                           const weak_ptr<DatabaseInstance> &db, hugeint_t id, const std::string &on_text,
	                           const std::string &on_close);

	const Options opts_;
	const weak_ptr<DatabaseInstance> db_;
	hugeint_t id_;
	// Set once by Create. Weak, and safe to copy on a transport thread even while the destructor runs.
	std::weak_ptr<ConnectionHub> self_;

	// Serializes dialing, so two sessions never both redial.
	std::mutex connect_mu_;
	// Guarded by connect_mu_.
	std::string token_;

	std::mutex mu_;
	std::condition_variable cv_;
	// See Generation(). OnOpen, OnTransportClose and HELLO frames from an older transport are ignored, and the PONG
	// to its PING is never written. Until its Stop returns, its other frames are still delivered, and a reply to
	// its text goes out on the current socket.
	uint64_t gen_ = 0;
	std::shared_ptr<Transport> transport_;
	bool opened_ = false;
	bool open_ = false;
	bool dropped_ = false;
	bool disconnected_ = false;
	std::string drop_reason_;
	bool hello_arrived_ = false;
	int protocol_version_ = 0;
	std::string hello_error_;
	uint64_t hello_pending_beats_ = 0;
	std::string hello_pending_message_;
	std::vector<std::string> capabilities_;
	std::map<uint64_t, SubscriberPtr> subs_;
	std::map<uint32_t, SubscriberPtr> requests_;
	uint32_t next_req_id_ = 1;
	// Notified whenever a subscriber's in_flight drops.
	std::condition_variable idle_cv_;

	std::mutex out_mu_;
	std::condition_variable out_cv_;
	std::deque<Outbound> out_q_;
	bool writer_stop_ = false;
	std::thread writer_;

	std::shared_ptr<HostQueue> host_queue_;
	std::thread host_worker_;
};

// The connections open on one DatabaseInstance. Releasing a connection's last lease disconnects it.
class ConnectionRegistry : public ObjectCacheEntry {
public:
	static shared_ptr<ConnectionRegistry> Get(DatabaseInstance &db);
	// The hubs log through the database's LogManager, which dies right after this: their threads must be done first.
	~ConnectionRegistry() override;

	static string ObjectType() {
		return "n6k_connections";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

	struct Acquired {
		std::shared_ptr<ConnectionHub> hub;
		// `make` built it: nothing has dialed it yet.
		bool created;
	};
	// A lease on the live connection keyed `key`, or on one `make` builds when there is none.
	Acquired Acquire(const std::string &key, const std::function<std::shared_ptr<ConnectionHub>()> &make);
	// A lease on connection `id`; null for an unknown or dead one.
	std::shared_ptr<ConnectionHub> AcquireById(const hugeint_t &id);
	// Drops one lease; the last disconnects the connection. False for an unknown id.
	bool Release(const hugeint_t &id, const std::string &reason);
	// Marks one of the caller's leases as n6k_connect's, the only kind n6k_disconnect may drop.
	void AddConnectLease(const hugeint_t &id);
	// Release for such a lease. False for an unknown id; throws when the connection has none.
	bool ReleaseConnectLease(const hugeint_t &id, const std::string &reason);
	// Null for an unknown id.
	std::shared_ptr<ConnectionHub> Find(const hugeint_t &id);

	struct Listed {
		std::shared_ptr<ConnectionHub> hub;
		size_t leases;
	};
	std::vector<Listed> List();

private:
	struct Entry {
		std::shared_ptr<ConnectionHub> hub;
		std::string key;
		size_t leases;
		// How many of `leases` are n6k_connect's.
		size_t connect_leases;
	};

	std::mutex mu_;
	std::map<hugeint_t, Entry> hubs_;
};

// One lease, released by Release() or on destruction, whichever comes first.
class ConnectionLease {
public:
	ConnectionLease(shared_ptr<ConnectionRegistry> registry, std::shared_ptr<ConnectionHub> hub);
	~ConnectionLease();
	ConnectionLease(const ConnectionLease &) = delete;
	ConnectionLease &operator=(const ConnectionLease &) = delete;

	const std::shared_ptr<ConnectionHub> &Hub() const {
		return hub_;
	}
	void Release();

private:
	shared_ptr<ConnectionRegistry> registry_;
	std::shared_ptr<ConnectionHub> hub_;
	std::atomic<bool> released_ {false};
};

// The registry key of a WebSocket connection: the URL exactly as dialed, and the token its HELLO carries.
std::string WebSocketKey(const std::string &ws_url, const std::string &token);

// ws(s)://… for an http(s):// base URL, with `/ws` and `query` appended.
std::string WsUrlFromBaseUrl(const std::string &base_url, const std::string &query);

#ifndef WASM_LOADABLE_EXTENSIONS
ConnectionHub::Options WebSocketOptions(const std::string &ws_url, const std::string &token);
#endif

// The id an ATTACH option or function argument names; throws on a malformed one.
hugeint_t ParseConnectionId(const std::string &text, const char *who);

} // namespace n6k
} // namespace duckdb
