#pragma once

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "serve_transport.hpp"
#include "uds_transport.hpp"

// duckdb::shared_ptr is its own type, not an alias for std::shared_ptr.
#include "duckdb/common/shared_ptr.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace duckdb {

class DatabaseInstance;
class ClientContext;
class Connection;

namespace n6k {

// One session opened by an OPEN: the `ns` every response for it is stamped with, and the catalog its
// ops are qualified against.
struct SessionRef {
	int64_t ns = 0;
	std::string catalog;
};

// Per-request state; the worker waits on cv, the reader signals it.
struct RequestSlot {
	std::atomic<bool> cancel {false};
	std::mutex mu;
	std::condition_variable cv;
	int credits = DEFAULT_BATCH_CREDITS;
	// The worker's own DuckDB context, published once it has one so shutdown can interrupt a query
	// that is not going to notice `cancel` on its own. Guarded by `mu`.
	shared_ptr<ClientContext> context;
	// Set before the slot is published; never changes.
	std::string catalog;
	// Other catalogs the request's plan names, as AdmitPlanCatalog spelled them. Guarded by the
	// reactor's map_mu_.
	std::unordered_set<std::string> plan_catalogs;
};

// A paused catalog's drain state. Until `holding`, the reader may still admit REQs for it, so `running`
// can still grow. After it, a running request's AdmitPlanCatalog can still briefly add to `running`
// before it fails on the pause.
struct InflightState {
	bool holding;
	int64_t running;
};

// Requests are keyed by (ns, req_id), not req_id alone: each session owns its own req-id space, so two
// sessions may both have req_id 1 in flight without colliding.
struct InflightKey {
	int64_t ns;
	uint32_t req_id;

	bool operator==(const InflightKey &other) const {
		return ns == other.ns && req_id == other.req_id;
	}
};

struct InflightKeyHash {
	size_t operator()(const InflightKey &key) const {
		return std::hash<int64_t> {}(key.ns) ^ (std::hash<uint32_t> {}(key.req_id) << 1);
	}
};

struct PinnedTxn {
	std::mutex mu;
	std::unique_ptr<Connection> conn;
	// Set when its session closes. The reader takes `mu` only after workers drain (a worker can hold it
	// across a credit wait); until then the last holder destroys `conn`.
	std::atomic<bool> dropped {false};
};

struct TxnKey {
	int64_t ns;
	int64_t txn;

	bool operator==(const TxnKey &other) const {
		return ns == other.ns && txn == other.txn;
	}
};

struct TxnKeyHash {
	size_t operator()(const TxnKey &key) const {
		return std::hash<int64_t> {}(key.ns) ^ (std::hash<int64_t> {}(key.txn) << 1);
	}
};

// A catalog accepted before it is attached: an OPEN for it parks until ResolvePending settles it.
// `message` is the status carried on every FT_OPEN_PENDING until then.
struct PendingCatalogSpec {
	std::string catalog;
	std::string message;
};

// A lazy name's rank when the host gives none: it loses to every upstream with a lower rank; a tie goes
// to the earliest added.
static constexpr int64_t NO_RANK = std::numeric_limits<int64_t>::max();

// The host must not share an upstream between connections: `ns` and req ids are not rewritten.
struct Upstream {
	// The host's fd number for it.
	int id = -1;
	UdsConnection conn;

	std::mutex out_mu;
	std::condition_variable out_cv;
	std::deque<std::string> out_q;
	bool out_stop = false;
	std::thread writer;
	std::thread reader;
	// Set by the reactor to end it; `broken` by its writer when a write fails.
	std::atomic<bool> stop {false};
	std::atomic<bool> broken {false};

	// What is still owed an answer if it goes away: OPENs sent but not yet ACKed or refused, and
	// requests sent but not yet ended.
	std::mutex track_mu;
	std::unordered_set<int64_t> opening;
	std::unordered_set<InflightKey, InflightKeyHash> inflight;
	// The reader's copy of the reactor's `forwarded_` for this upstream: a frame on any other ns is dropped.
	std::unordered_set<int64_t> forwarded;
	// Why it stopped: set by whichever of its reader or writer noticed first.
	std::string reason;

	void Send(std::string frame);
};

struct UpstreamStatus {
	bool open;
	std::string reason;
};

// Serve engine: reader routes frames, a writer serializes output, each REQ gets its own worker thread.
class ServeReactor {
public:
	ServeReactor(ServeTransport &conn, DatabaseInstance &db, ClientContext &context, std::vector<std::string> catalogs,
	             bool hello_already_read, std::vector<PendingCatalogSpec> pending = {}, int serve_fd = -1);
	~ServeReactor();

	ServeReactor(const ServeReactor &) = delete;
	ServeReactor &operator=(const ServeReactor &) = delete;

	// Run the reader loop until EOF, interrupt, a write failure, a read or parse error, or a second HELLO;
	// returns the number of REQs admitted to local workers, or 0 if the handshake fails.
	int64_t Run();

	// Overrides the interval the ctor read from PING_INTERVAL_ENV (the ctor throws if that is invalid);
	// <= 0 disables pinging. Must be called before Run().
	void SetPingIntervalMs(int ms) {
		ping_interval_ms_ = ms;
	}

	// The FT_HELLO capabilities when the host already read HELLO (hello_already_read); must be called
	// before Run(). Unset means the client offered none.
	void SetHelloCapabilities(std::vector<std::string> offered) {
		hello_offered_caps_ = std::move(offered);
	}

	// The APP_HELLO_CAPS the host serves on this socket. Must be called before Run().
	void SetAppCapabilities(std::vector<std::string> granted) {
		app_caps_ = std::move(granted);
	}

	// The host already sent HELLO_ACK (n6k_hello_ack), so Run() does not. Must be called before Run().
	void SetHelloAcked(bool acked) {
		hello_acked_ = acked;
	}

	// Must be called before Run(); defaults to OPEN_PENDING_INTERVAL_MS.
	void SetOpenPendingIntervalMs(int ms) {
		open_pending_interval_ms_ = ms;
	}

	// An OPEN for a catalog that is not served, pending, upstream or failed parks instead of failing, and the
	// host gets a StreamFrameType::HOST {"resolve": catalog} (pump.py `_Resolver.receive`) to settle it with
	// Resolved. Must be called before Run().
	void SetResolveUnknown(bool on) {
		resolve_unknown_ = on;
	}

	// Names listed before anything serves them, each with its rank (see AddLazyCatalog); opening one
	// resolves it like any unknown name. Must be called before Run(); needs resolve on (checked only by the
	// n6k_serve_fd bind).
	void SetLazyCatalogs(const std::vector<std::pair<std::string, int64_t>> &names);

	// The socket fd this reactor serves, or -1. The registry addresses a reactor by it.
	int ServeFd() const {
		return serve_fd_;
	}

	// Settle a pending catalog: ok ACKs every parked OPEN (the catalog must be attached by now), !ok
	// OPEN_ERRs them with `message` and every later OPEN too. Throws if `catalog` was not declared
	// pending or was already settled. Safe to call from any thread; the reader applies it.
	void ResolvePending(const std::string &catalog, bool ok, const std::string &message);

	// Serve an attached catalog from now on. A local catalog outranks every lazy name and upstream, so an
	// upstream serving it loses it (its sessions on it fail). Throws if it is already served or pending.
	// Safe to call from any thread; the reader applies it.
	void AddCatalog(const std::string &catalog);

	// List `catalog` from now on without serving it, until AddCatalog or RemoveCatalog. Against upstreams that
	// report it, it is a claim of `rank`: the lowest (rank, then earliest added) wins, so an upstream with a
	// lower rank serves it and one with a higher rank never does. Throws unless resolve is on, or if it is already
	// served, pending or lazy. Safe to call from any thread; the reader applies it.
	void AddLazyCatalog(const std::string &catalog, int64_t rank);

	// The host finished resolving `catalog`: the reader OPEN_ERRs every OPEN still parked on it (an ADD or an
	// upstream may already have taken them) with `error`, or "not found" when empty. Throws if `catalog` is
	// not being resolved. Safe to call from any thread; the reader applies it.
	void Resolved(const std::string &catalog, const std::string &error);

	// Forward to the n6k server on `up_fd`, whose HELLO is done (see n6k_upstream_hello), the `catalogs` it
	// reports and any it reports later, each one it wins. A local catalog always wins; against lazy names and
	// other upstreams the lowest (rank, then earliest added) wins, re-decided on every change, and a name
	// that changes hands fails its sessions on the loser. Returns the reported names it does not win. Throws
	// if `up_fd` cannot be adopted (see UdsConnection::AdoptBlockingDupOfFd) or is an open upstream here. Safe to call
	// from any thread; the reader applies it.
	std::vector<std::string> AddUpstream(int up_fd, const std::vector<std::string> &catalogs, int64_t rank);

	// Change an open upstream's rank and re-decide every name it reports. Returns the reported names it does
	// not win. Throws if `up_fd` is not an open upstream here. Safe to call from any thread.
	std::vector<std::string> SetUpstreamRank(int up_fd, int64_t rank);

	// Stop forwarding to `up_fd`: its sessions fail as if it had closed, with "removed by host". Throws if
	// it is not an open upstream here. Safe to call from any thread; the reader applies it.
	void RemoveUpstream(int up_fd);

	// Whether `up_fd` is still forwarded to, and why it stopped if not. Throws if it never was an
	// upstream here. Safe to call from any thread.
	UpstreamStatus GetUpstreamStatus(int up_fd);

	// Stop serving or listing a served, pending or lazy catalog. A served or pending one has its sessions
	// closed (in-flight requests ERR, their queries interrupted) and its parked OPENs fail. Throws if it is
	// none of these (naming the upstream if one serves it). Safe to call from any thread; the reader applies it.
	void RemoveCatalog(const std::string &catalog);

	// Hold new REQs for `catalog` until Resume. CREDIT, CANCEL and PING still flow, so in-flight streams can
	// finish. Throws if `catalog` is not served or pending, or is already paused. Safe to call from any
	// thread; the reader applies it.
	void Pause(const std::string &catalog);

	// Admit the REQs held for `catalog`, in arrival order. Throws if it is not paused. Safe to call from any
	// thread; the reader applies it.
	void Resume(const std::string &catalog);

	// Whether the reader holds new REQs for the paused `catalog`, and how many requests run against it.
	// Throws if it is not paused. Safe to call from any thread.
	InflightState Inflight(const std::string &catalog);

	// Cancel and interrupt every request running against `catalog`, including statements in its sessions'
	// open transactions. Returns how many requests were running. Safe to call from any thread.
	int64_t CancelInflight(const std::string &catalog);

	void SendFrame(std::string frame);
	void SendJsonRpc(std::string message);
	void SendHost(std::string message);

	// Consume one credit; false if cancelled or shutting down (caller stops streaming).
	bool AcquireCredit(RequestSlot &slot);

	void AddCredits(RequestSlot &slot, int n);

	// Push OP_CATALOG_INVALIDATED to every session on this connection bound to `catalog`, each
	// stamped with its own ns. Returns how many were notified; empty `schemas` returns 0 and pushes
	// nothing. Safe to call from any thread.
	int NotifyInvalidate(const std::string &catalog, const std::vector<std::string> &schemas);

	// A plan on `slot`'s session names `catalog`. Throws unless a session on this connection has it
	// open and it is not paused; from then on Inflight and CancelInflight count the request against it.
	void AdmitPlanCatalog(RequestSlot &slot, const std::string &catalog);

	// A transaction pinned to its own Connection for the life of (ns, txn). Every request that names
	// the txn runs on that Connection under its mutex.
	int64_t BeginTxn(int64_t ns);
	std::shared_ptr<PinnedTxn> FindTxn(int64_t ns, int64_t txn);
	std::shared_ptr<PinnedTxn> TakeTxn(int64_t ns, int64_t txn);

	// Backpressure and cancellation are invisible from outside: a correctly paced stream and one
	// that never filled its window look identical on the wire.
	int64_t CreditPauses() const {
		return credit_pauses_.load();
	}
	int64_t Cancels() const {
		return cancels_.load();
	}
	int64_t RequestsHandled() const {
		return reqs_handled_.load();
	}

private:
	void WriterLoop();
	// Emits an FT_PING every ping_interval_ms until shutdown. Keepalive only: no PONG is awaited and
	// no deadline is enforced, so a briefly stalled client is never dropped for a late reply.
	void PingLoop();
	// Signal the pinger to stop and join it. Idempotent, so Run() and ~ServeReactor may both call it.
	void StopPinger();
	// Reader thread only.
	void HandleOpen(const msgpack::object &header, const std::string &frame);
	// Reader thread only.
	void CloseSessionAndCancelItsRequests(int64_t ns);
	// Reader thread only.
	const std::string *LookupSessionCatalog(int64_t ns) const;
	// Reader thread only. Pushes CATALOGS_CHANGED when the listed set changed.
	void ApplyCatalogChanges();
	// Reader thread only. Served, pending, upstream, then unclaimed lazy: what CATALOG_LIST and
	// CATALOGS_CHANGED report.
	std::vector<std::string> ListedCatalogs() const;
	// Reader thread only.
	void ApplyRemove(const std::string &catalog);
	// Reader thread only. ERRs every in-flight request on `ns` with `reason`, closes the session, and
	// interrupts its workers.
	void EvictSession(int64_t ns, const std::string &reason);
	// Reader thread only.
	void SendDuePendingBeats();
	// Reader thread only.
	bool IsParked(int64_t ns) const;
	// Reader thread only.
	void BindSession(int64_t ns, const std::string &catalog, const std::vector<std::string> &capabilities);
	// Reader thread only.
	void ForwardOpen(Upstream &up, const std::string &catalog, int64_t ns, const std::string &frame);
	// Reader thread only.
	void ForwardUnresolved(Upstream &up, const std::string &catalog);
	// Reactor reader thread only.
	void ApplyUpstreamAdd(const std::shared_ptr<Upstream> &up);
	void ApplyUpstreamOwner(const std::string &catalog, int previous, int owner);
	void ApplyUpstreamGone(int id, const std::string &reason);
	// Reactor reader thread only. Fails every session forwarded to `up` on `catalog` with `reason` and closes
	// it upstream too.
	void EvictForwarded(Upstream &up, const std::string &catalog, const std::string &reason);
	void UpstreamReaderLoop(Upstream &up);
	void UpstreamWriterLoop(Upstream &up);
	static void StopUpstream(Upstream &up);

	void AdmitAndSpawnRequestWorker(const SessionRef &sess, uint32_t req_id, uint8_t op, std::string body);
	// Spawned on its own thread. std::thread decay-copies these, so `sess` stays valid even when the
	// reader closes that session mid-request — the worker must never reach back into sessions_.
	void WorkerMain(const SessionRef &sess, uint32_t req_id, const std::shared_ptr<RequestSlot> &slot, uint8_t op,
	                const std::string &body);
	std::shared_ptr<RequestSlot> FindSlot(int64_t ns, uint32_t req_id);
	// Interrupt every in-flight worker's DuckDB context. Called only from the shutdown path. A worker
	// that has not yet published its context has nothing to interrupt and will see `stopping_` instead.
	void InterruptWorkers();
	// Reader thread only.
	void TrackSession(int64_t ns, const std::string &catalog);
	void UntrackSession(int64_t ns);

	ServeTransport &conn_;
	DatabaseInstance &db_;
	ClientContext &context_;
	// Catalogs this connection serves: bind order, then appended/removed as changed. Reader thread only.
	std::vector<std::string> served_;
	std::unordered_set<std::string> served_set_;
	bool hello_already_read_;
	std::vector<std::string> hello_offered_caps_;
	std::vector<std::string> app_caps_;
	bool hello_acked_ = false;
	int serve_fd_;

	struct ParkedOpen {
		int64_t ns;
		std::vector<std::string> capabilities;
		std::string frame;
	};
	struct PendingOpen {
		std::string message;
		std::vector<ParkedOpen> parked;
	};
	// Unresolved pending catalogs and catalogs that resolved as failed (catalog -> message). Reader
	// thread only; other threads reach them through `catalog_changes_`.
	std::vector<std::string> pending_order_;
	std::unordered_map<std::string, PendingOpen> pending_;
	std::unordered_map<std::string, std::string> failed_;
	int open_pending_interval_ms_;
	std::chrono::steady_clock::time_point next_pending_beat_;

	bool resolve_unknown_ = false;
	// Catalogs the host is resolving and the OPENs parked on each. An entry outlives its OPENs (they may be
	// cancelled, ACKed by an ADD, or forwarded to an upstream) until Resolved, so a second OPEN never starts a
	// second resolve. Reader thread only.
	std::unordered_map<std::string, std::vector<ParkedOpen>> unresolved_;
	// Reader thread only.
	void SettleResolved(const std::string &catalog, const std::string &error);
	// Lazy names in the order they were listed; one also served, pending or upstream is listed as that.
	// Reader thread only.
	std::vector<std::string> lazy_;

	// Who wins a name no local catalog holds, decided under `resolve_mu_` from the lazy names and every open
	// upstream's reported catalogs. Reassign queues UPSTREAM_OWNER whenever the winning upstream changes.
	struct Claim {
		int64_t rank;
		uint64_t seq;
	};
	struct UpstreamClaim {
		Claim claim;
		std::vector<std::string> reported;
		std::unordered_set<std::string> reported_set;
	};
	std::unordered_map<std::string, Claim> lazy_claims_;
	std::unordered_map<int, UpstreamClaim> upstream_claims_;
	std::unordered_map<std::string, int> upstream_owner_;
	uint64_t next_claim_seq_ = 0;
	// Under `resolve_mu_`.
	void Reassign(const std::string &catalog);
	std::vector<std::string> NotWon(int up_fd, const std::vector<std::string> &reported) const;
	// Under `resolve_mu_`. Drops `up_fd`'s claims after it has gone, re-deciding the names it reported.
	void DropUpstreamClaim(int up_fd);
	// Under `resolve_mu_`. The upstream's reader saw its CATALOGS_CHANGED.
	void ReportUpstreamCatalogs(int up_fd, std::vector<std::string> names);

	struct CatalogChange {
		enum class Kind {
			READY,
			FAILED,
			ADD,
			REMOVE,
			PAUSE,
			RESUME,
			RESOLVED,
			UPSTREAM_ADD,
			UPSTREAM_OWNER,
			UPSTREAM_GONE,
			FORWARD_DROP,
			LAZY_ADD,
			LAZY_REMOVE
		};
		Kind kind;
		std::string catalog;
		std::string message;
		std::shared_ptr<Upstream> upstream;
		int upstream_id = -1;
		// UPSTREAM_OWNER: the upstream that won `catalog` before, or -1.
		int previous_upstream_id = -1;
		int64_t ns = 0;
	};
	// The caller-side view, ahead of the reader's: `resolvable_` is still pending, `listed_` is served or
	// pending once every queued change applies.
	std::mutex resolve_mu_;
	std::unordered_set<std::string> resolvable_;
	std::unordered_set<std::string> listed_;
	std::vector<CatalogChange> catalog_changes_;
	// `unresolved_` as callers see it: inserted by the reader when it asks the host, erased by Resolved.
	std::unordered_set<std::string> resolving_;
	// Every upstream's status, as callers see it.
	std::unordered_map<int, UpstreamStatus> upstream_status_;
	// Paused as callers see it, and paused as the reader has applied it.
	std::unordered_set<std::string> paused_;
	std::unordered_set<std::string> pause_applied_;

	struct HeldReq {
		SessionRef sess;
		uint32_t req_id;
		uint8_t op;
		std::string body;
	};
	// REQs for paused catalogs, in arrival order. Reader thread only.
	std::unordered_map<std::string, std::vector<HeldReq>> held_;
	size_t held_count_ = 0;
	// Reader thread only.
	std::vector<HeldReq> TakeHeldOf(int64_t ns);
	// Reader thread only. True if (ns, req_id) was held; it is dropped and answered as cancelled.
	bool CancelHeld(int64_t ns, uint32_t req_id);

	// Upstreams by id, the catalogs each wins as UPSTREAM_OWNER applied it (in that order, for
	// CATALOG_LIST), and the sessions forwarded to them. Reader thread only.
	struct Forwarded {
		Upstream *up;
		std::string catalog;
	};
	std::unordered_map<int, std::shared_ptr<Upstream>> upstreams_;
	std::vector<std::string> upstream_order_;
	std::unordered_map<std::string, int> upstream_catalogs_;
	std::unordered_map<int64_t, Forwarded> forwarded_;

	// Open sessions by ns. Written and read ONLY by the reader thread — workers copy the SessionRef
	// they need, so no lock is required here.
	std::unordered_map<int64_t, std::string> sessions_;

	// A locked mirror of `sessions_` for readers off the reader thread. Separate so the request path
	// keeps lock-free access to `sessions_`; written only on session open/close.
	mutable std::mutex push_mu_;
	std::unordered_map<int64_t, std::string> push_sessions_;

	std::mutex out_mu_;
	std::condition_variable out_cv_;
	std::deque<std::pair<StreamFrameType, std::string>> out_q_;
	bool writer_stop_ = false;
	std::thread writer_;

	// Keepalive. Its own mutex/cv rather than the writer's, so that a connection under load does not
	// wake the pinger on every enqueued frame.
	std::mutex ping_mu_;
	std::condition_variable ping_cv_;
	bool ping_stop_ = false;
	std::thread pinger_;
	int ping_interval_ms_ = 0;

	std::mutex map_mu_;
	std::unordered_map<InflightKey, std::shared_ptr<RequestSlot>, InflightKeyHash> inflight_;

	std::mutex txn_mu_;
	int64_t next_txn_ = 1;
	std::unordered_map<TxnKey, std::shared_ptr<PinnedTxn>, TxnKeyHash> txns_;
	void DropTxnsOf(int64_t ns);
	void DropAllTxns();

	// Detached workers; counter lets Run() wait for drain at shutdown.
	std::mutex workers_mu_;
	std::condition_variable workers_cv_;
	int active_workers_ = 0;

	std::atomic<int64_t> reqs_handled_ {0};
	std::atomic<int64_t> credit_pauses_ {0};
	std::atomic<int64_t> cancels_ {0};
	std::atomic<bool> stopping_ {false};
};

// Live reactors on one DatabaseInstance.
//
// An invalidation originates outside the connection it must reach: whatever mutated the data holds
// its own DuckDB connection and has no handle on the sockets currently serving that catalog. So
// reactors register themselves for the duration of Run(), and a push is addressed by
// (DatabaseInstance, catalog) rather than by connection.
//
// Non-owning and multi-valued (several reactors per DatabaseInstance). No registration without a
// matching removal.
//
// A registered reactor is reachable from any thread, so a push may arrive at any moment it is in the
// registry — and must never reach one that is tearing down. Scoping the registration to this guard
// is what makes that unmissable: it holds for exactly the window Run() can serve, and Release()
// closes that window early, before the workers drain.
class ServeReactorRegistration {
public:
	ServeReactorRegistration(DatabaseInstance &db, ServeReactor &reactor);
	~ServeReactorRegistration();

	ServeReactorRegistration(const ServeReactorRegistration &) = delete;
	ServeReactorRegistration &operator=(const ServeReactorRegistration &) = delete;

	// Deregister now rather than at scope exit. Idempotent.
	void Release();

private:
	ServeReactor *reactor_;
};

// Push an invalidation to every reactor on `db` serving `catalog`. Returns the number of sessions
// notified across all of them, which is what makes a no-op distinguishable from a delivery.
int BroadcastInvalidate(DatabaseInstance &db, const std::string &catalog, const std::vector<std::string> &schemas);

// Settle `catalog` pending on the reactor serving `fd` on `db`. Waits briefly for that reactor to
// register, since the host may finish its ATTACH before the serve CALL reaches Run().
void ResolvePendingOpen(DatabaseInstance &db, int fd, const std::string &catalog, bool ok, const std::string &message);

// Add or remove a served catalog on the reactor serving `fd` on `db`; same registration wait as
// ResolvePendingOpen.
void AddServedCatalog(DatabaseInstance &db, int fd, const std::string &catalog);
void RemoveServedCatalog(DatabaseInstance &db, int fd, const std::string &catalog);
void AddServedLazyCatalog(DatabaseInstance &db, int fd, const std::string &catalog, int64_t rank);

void ResolveUnknownOpen(DatabaseInstance &db, int fd, const std::string &catalog, const std::string &error);

std::vector<std::string> AddServedUpstream(DatabaseInstance &db, int fd, int up_fd,
                                           const std::vector<std::string> &catalogs, int64_t rank);
std::vector<std::string> SetServedUpstreamRank(DatabaseInstance &db, int fd, int up_fd, int64_t rank);
void RemoveServedUpstream(DatabaseInstance &db, int fd, int up_fd);
UpstreamStatus ServedUpstreamStatus(DatabaseInstance &db, int fd, int up_fd);

void PauseServedCatalog(DatabaseInstance &db, int fd, const std::string &catalog);
void ResumeServedCatalog(DatabaseInstance &db, int fd, const std::string &catalog);
InflightState ServedCatalogInflight(DatabaseInstance &db, int fd, const std::string &catalog);
int64_t CancelServedCatalogInflight(DatabaseInstance &db, int fd, const std::string &catalog);

// Aggregate counters over every reactor currently serving `db`.
struct ServeStats {
	int64_t connections = 0;
	int64_t requests_handled = 0;
	int64_t credit_pauses = 0;
	int64_t cancels = 0;
};
ServeStats CollectServeStats(DatabaseInstance &db);

} // namespace n6k
} // namespace duckdb
