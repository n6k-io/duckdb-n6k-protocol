#pragma once

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "parked_stream.hpp"
#include "serve_handshake.hpp"
#include "serve_options.hpp"
#include "serve_transport.hpp"
#include "uds_transport.hpp"

// duckdb::shared_ptr is its own type, not an alias for std::shared_ptr.
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/hugeint.hpp"

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

struct AuthResult;

// One session opened by an OPEN: the `ns` every response for it is stamped with, and the catalog its
// ops are qualified against.
struct SessionRef {
	int64_t ns = 0;
	std::string catalog;
};

// Per-request state, shared by the reader and whichever worker carries the request.
struct RequestSlot {
	// Set before the slot is published; never change.
	int64_t ns = 0;
	uint32_t req_id = 0;
	std::atomic<bool> cancel {false};
	std::mutex mu;
	// Its terminal frame went out: nothing more is sent for it. Guarded by `mu`.
	bool ended = false;
	int credits = DEFAULT_BATCH_CREDITS;
	// The response while it waits for credit, on no thread; whoever calls QueueParked takes it and queues
	// it. Guarded by `mu`, as is when it parked.
	std::unique_ptr<ParkedStream> parked;
	std::chrono::steady_clock::time_point parked_at;
	// The request's own Connection's context, published once it has one so another thread can interrupt a
	// query that will not notice `cancel` on its own. Guarded by `mu`.
	shared_ptr<ClientContext> context;
	// Immutable, as `ns` and `req_id`.
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

inline int64_t SteadyNowMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

struct PinnedTxn {
	// A worker holds it for as long as its statement runs. ExpireIdle only try_locks it; DropAllTxns blocks on
	// it after the workers drain.
	std::mutex mu;
	// When a request last let go of it (or it began), for idle_txn_timeout_ms.
	std::atomic<int64_t> last_used_ms {SteadyNowMs()};
	void Touch() {
		last_used_ms.store(SteadyNowMs());
	}
	std::unique_ptr<Connection> conn;
	// Set when its session closes; `conn` is then interrupted without `mu` and destroyed by the last holder.
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

// One connection's traffic since it started. Frames and payload bytes as the transport delivers them (no WebSocket
// framing); frames_in excludes the HELLO.
struct SessionStats {
	int64_t frames_in;
	int64_t bytes_in;
	int64_t frames_out;
	int64_t bytes_out;
	int64_t requests_handled;
	int64_t duration_ms;
	int64_t authenticate_ms;
	// Worker threads started.
	int64_t peak_workers;
};

// Serve engine: reader routes frames, a writer serializes output, worker threads run admitted REQs.
class ServeReactor {
public:
	// `session_id` is what every host callback receives first, or 0 for a session the host cannot address.
	ServeReactor(ServeTransport &conn, DatabaseInstance &db, ClientContext &context, std::vector<std::string> catalogs,
	             ServeOptions options = {}, hugeint_t session_id = hugeint_t(0));
	~ServeReactor();

	ServeReactor(const ServeReactor &) = delete;
	ServeReactor &operator=(const ServeReactor &) = delete;

	// Read HELLO, authenticate, then serve until the connection ends; returns the number of REQs admitted to
	// local workers, or 0 if the handshake fails.
	int64_t Run();

	hugeint_t SessionId() const {
		return session_id_;
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

	// The resolve callback returned for `catalog`: the reader OPEN_ERRs every OPEN still parked on it (an ADD or an
	// upstream may already have taken them) with `error`, or "not found" when empty. Throws if `catalog` is
	// not being resolved. Safe to call from any thread; the reader applies it.
	void Resolved(const std::string &catalog, const std::string &error);

	// A JSON-RPC message to the client, after anything already queued; dropped once the out queue has overflowed.
	// Throws unless the client offered one of APP_HELLO_CAPS and the host's app capabilities, if it gave any,
	// include it. Safe to call from any thread.
	void SendText(std::string text);

	// Send the client a close with `code` and `reason` after what is already queued, and end the connection.
	// Safe to call from any thread.
	void Close(int code, std::string reason);

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

	// Every frame of an admitted request goes through these, so it gets at most one terminal frame and
	// nothing after it. EndRequest also sets `cancel`. Never called with `slot.mu` held.
	void SendForRequest(RequestSlot &slot, std::string frame);
	void EndRequest(RequestSlot &slot, std::string terminal);

	enum class CreditResult { TAKEN, EMPTY, STOPPED };
	// Consume one credit without waiting. EMPTY: return PARKED. STOPPED: cancelled or shutting down, so end
	// the stream.
	CreditResult TryAcquireCredit(RequestSlot &slot);

	// Reader thread only. Queues the response again if it was parked for credit.
	void AddCredits(const std::shared_ptr<RequestSlot> &slot, int n);

	bool HasWaitingRequests();

	// Push OP_CATALOG_INVALIDATED to every session on this connection bound to `catalog`, each
	// stamped with its own ns. Returns how many were notified; empty `schemas` returns 0 and pushes
	// nothing. Safe to call from any thread.
	int NotifyInvalidate(const std::string &catalog, const std::vector<std::string> &schemas);

	// A plan on `slot`'s session names `catalog`. Throws unless a session on this connection has it
	// open and it is not paused; from then on Inflight and CancelInflight count the request against it.
	void AdmitPlanCatalog(RequestSlot &slot, const std::string &catalog);

	// A transaction pinned to its own Connection for the life of (ns, txn). Every request that names
	// the txn runs on that Connection under its mutex. Throws once `slot` is cancelled, so a session's
	// transactions cannot outlive its close.
	int64_t BeginTxn(RequestSlot &slot);
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
	SessionStats Stats() const;

private:
	// CLOSE carries the reason as its payload and is the last thing written.
	struct OutFrame {
		StreamFrameType type;
		std::string payload;
		int close_code;
	};

	// Reader thread, writer running. Runs authenticate, beating HELLO_PENDING after hello_pending_first_ms and then
	// every open_pending_interval_ms while it does, then HELLO_ACK, or HELLO_ERR and Close and false.
	bool Authenticate(const HelloRequest &hello);
	void ApplyAuthResult(const AuthResult &result);
	void Reject(const std::string &reason, int code);
	void SetLazyCatalogs(const std::vector<std::pair<std::string, int64_t>> &names);
	void TextWorkerLoop();
	// Reader thread only. Queues `catalog` for the resolver thread, which calls resolve, then Resolved.
	void StartResolve(const std::string &catalog);
	void ResolverLoop();
	void StopCallbackThreads();
	void StopWriter();
	// Also the keepalive: once StartPinging, at most one FT_PING per ping_interval_ms, sent when the queue is empty
	// at or after the due time. No PONG is awaited and no deadline is enforced, so a briefly stalled client is never
	// dropped for a late reply.
	void WriterLoop();
	void StartPinging();
	// Under out_mu_. False, and the frame dropped, once the client has stopped reading.
	bool EnqueueOut(OutFrame frame);
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
	// Reader thread only. Applies idle_stream_timeout_ms and idle_txn_timeout_ms.
	void ExpireIdle();
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

	// The task owns copies, so `sess` stays valid even when the reader closes that session mid-request — the
	// worker must never reach back into sessions_.
	struct RequestTask {
		enum class Kind { START, RESUME, CANCEL };
		Kind kind;
		SessionRef sess;
		uint32_t req_id;
		std::shared_ptr<RequestSlot> slot;
		uint8_t op;
		std::string body;
		// RESUME and CANCEL: the response taken off the slot.
		std::unique_ptr<ParkedStream> stream;
	};
	void AdmitAndQueueRequest(const SessionRef &sess, uint32_t req_id, uint8_t op, std::string body);
	void QueueTask(RequestTask task);
	void QueueParked(const std::shared_ptr<RequestSlot> &slot, RequestTask::Kind kind);
	void WorkerLoop();
	// Once stopping_, a START is retired without running or sending a terminal frame, and a RESUME is
	// cancelled instead.
	void RunRequest(RequestTask &task);
	// A stream that did not finish: parked on its slot, or queued again if it yielded or if credit, a cancel
	// or a shutdown already arrived.
	void ParkOrQueue(RequestTask task, PumpResult result);
	// Joins every worker. Idempotent, so Run() and ~ServeReactor may both call it.
	void StopWorkers();
	std::shared_ptr<RequestSlot> FindSlot(int64_t ns, uint32_t req_id);
	// Interrupt every in-flight request's own Connection. Does not reach a request in a pinned transaction,
	// which runs on PinnedTxn::conn. A request that has not yet published its context has nothing to interrupt;
	// only a streamed response sees `stopping_` later, so a non-streamed op already past RunRequest's check runs
	// to completion.
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
	ServeOptions options_;
	// Set by Authenticate before the reactor registers; read-only after.
	std::vector<std::string> app_caps_;
	bool accepts_text_ = false;
	hugeint_t session_id_;

	std::mutex text_mu_;
	std::condition_variable text_cv_;
	std::deque<std::string> text_q_;
	bool text_stop_ = false;
	std::thread text_worker_;

	std::mutex resolve_q_mu_;
	std::condition_variable resolve_q_cv_;
	std::deque<std::string> resolve_q_;
	bool resolve_stop_ = false;
	std::thread resolver_;

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
	std::deque<OutFrame> out_q_;
	// Payload bytes in `out_q_`.
	size_t out_q_bytes_ = 0;
	bool out_overflowed_ = false;
	bool writer_stop_ = false;
	bool pinging_ = false;
	std::thread writer_;
	int ping_interval_ms_ = 0;

	std::mutex map_mu_;
	std::unordered_map<InflightKey, std::shared_ptr<RequestSlot>, InflightKeyHash> inflight_;

	std::mutex txn_mu_;
	int64_t next_txn_ = 1;
	std::unordered_map<TxnKey, std::shared_ptr<PinnedTxn>, TxnKeyHash> txns_;
	void DropTxnsOf(int64_t ns);
	void DropAllTxns();

	// Workers start only while the queue outgrows the idle ones, up to options_.workers. active_workers_
	// counts admitted requests not yet retired (queued, running or parked); Run() waits on it to drain.
	std::mutex workers_mu_;
	std::condition_variable workers_cv_;
	std::condition_variable tasks_cv_;
	std::deque<RequestTask> tasks_;
	std::vector<std::thread> worker_threads_;
	int idle_workers_ = 0;
	bool workers_stop_ = false;
	int active_workers_ = 0;
	std::atomic<int64_t> peak_workers_ {0};

	std::atomic<int64_t> reqs_handled_ {0};
	std::atomic<int64_t> frames_in_ {0};
	std::atomic<int64_t> bytes_in_ {0};
	std::atomic<int64_t> frames_out_ {0};
	std::atomic<int64_t> bytes_out_ {0};
	std::chrono::steady_clock::time_point started_;
	// Set before the reactor registers; read-only after.
	int64_t authenticate_ms_ = 0;
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

// A session for the host to serve `fd` under; returns the UUID every control function and host callback
// addresses it by. Unlike an fd number, an id is never reused.
hugeint_t CreateServeSession(DatabaseInstance &db, int fd);

// Held by the serve of one session for its whole lifetime; on release the id addresses nothing.
class ServeSessionClaim {
public:
	// Throws if `id` was not created on `db`, or another serve already holds it.
	ServeSessionClaim(DatabaseInstance &db, hugeint_t id);
	~ServeSessionClaim();

	ServeSessionClaim(const ServeSessionClaim &) = delete;
	ServeSessionClaim &operator=(const ServeSessionClaim &) = delete;

	int Fd() const {
		return fd_;
	}

private:
	hugeint_t id_;
	int fd_;
};

// Push an invalidation to every reactor on `db` serving `catalog`. Returns the number of sessions
// notified across all of them, which is what makes a no-op distinguishable from a delivery.
int BroadcastInvalidate(DatabaseInstance &db, const std::string &catalog, const std::vector<std::string> &schemas);

// Each function below that takes `session_id`, except ServedSessionStats, acts on the reactor serving that
// session on `db`, first waiting for it to register (see AwaitServe), since the host may finish its ATTACH
// before the serve CALL reaches Run().

// Settle `catalog` pending on that reactor.
void ResolvePendingOpen(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog, bool ok,
                        const std::string &message);

void AddServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog);
void RemoveServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog);
void AddServedLazyCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog, int64_t rank);

void SendServedText(DatabaseInstance &db, hugeint_t session_id, const std::string &text);
void CloseServed(DatabaseInstance &db, hugeint_t session_id, int code, const std::string &reason);

std::vector<std::string> AddServedUpstream(DatabaseInstance &db, hugeint_t session_id, int up_fd,
                                           const std::vector<std::string> &catalogs, int64_t rank);
std::vector<std::string> SetServedUpstreamRank(DatabaseInstance &db, hugeint_t session_id, int up_fd, int64_t rank);
void RemoveServedUpstream(DatabaseInstance &db, hugeint_t session_id, int up_fd);
UpstreamStatus ServedUpstreamStatus(DatabaseInstance &db, hugeint_t session_id, int up_fd);

void PauseServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog);
void ResumeServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog);
InflightState ServedCatalogInflight(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog);
int64_t CancelServedCatalogInflight(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog);

// Aggregate counters over every reactor currently serving `db`.
struct ServeStats {
	int64_t connections = 0;
	int64_t requests_handled = 0;
	int64_t credit_pauses = 0;
	int64_t cancels = 0;
};
ServeStats CollectServeStats(DatabaseInstance &db);

// The connection serving `session_id` on `db`. Throws at once if none is registered; unlike the functions
// above, it does not wait.
SessionStats ServedSessionStats(DatabaseInstance &db, hugeint_t session_id);

} // namespace n6k
} // namespace duckdb
