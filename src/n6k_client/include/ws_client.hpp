#pragma once

#include "n6k_protocol_generated.hpp"
#include "ws_reactor.hpp"

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

class DatabaseInstance;
class Logger;

namespace n6k {

class N6kLinkState;

struct Frame {
	FrameType type;
	uint8_t flags;
	uint32_t req_id;
	std::string payload;
};

struct OpenAck {
	std::vector<std::string> capabilities;
};

// Polled once an OPEN_PENDING has arrived (and by the connection while it dials); true throws
// InterruptException, sending CANCEL {ns, id:0} for a pending OPEN. Called only during the call it is passed
// to; empty never aborts.
using AbortCheck = std::function<bool()>;

// Never 0: the server refuses it. Process-wide, so no two sessions on one connection ever share one.
uint64_t NextSessionNs();

struct WsClientOptions {
	// SERVER-side catalog name; sent in the FT_OPEN frame.
	std::string catalog;
	std::chrono::milliseconds connect_timeout {5000};
	// Once the server answers OPEN with OPEN_PENDING it has no overall deadline; it fails only when this
	// long passes with no further pending, ACK or ERR.
	std::chrono::milliseconds open_pending_stall {OPEN_PENDING_STALL_MS};
	// Called once per OPEN attempt with a fresh ns; returns an unstarted channel for it, or throws when the
	// connection beneath cannot be (re)established.
	std::function<std::unique_ptr<Reactor>(uint64_t ns, const AbortCheck &)> open_channel;
	// The HELLO_ACK capabilities of the connection beneath, asked on every (re)open.
	std::function<std::vector<std::string>()> hello_capabilities;
	// Receives a WsLogType row per frame sent and received; null logs nothing.
	shared_ptr<Logger> logger;
};

class WsClient;

class RequestState {
public:
	RequestState(uint32_t req_id, std::weak_ptr<WsClient> owner);

	bool Next(Frame &out, std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

	// Non-blocking peek: pops one frame if ready, else returns false immediately (never waits).
	// Like Next, returns false both when no frame is buffered yet and when the stream is finished;
	// callers distinguish via Exhausted().
	bool TryNext(Frame &out);

	// Register a "frame arrived" callback, fired (outside the request lock) each time a frame is
	// pushed onto this request. The async scan uses it to fire interrupt_state.Callback(). The
	// callback must be safe to run on the reactor's inbound thread. Dormant until set.
	void SetOnFrame(std::function<void()> cb);

	// Idempotent best-effort CANCEL; server responds with a terminal RESP_END{cancelled}.
	void Cancel();

	void Credit(uint32_t n);

	bool Exhausted() const;
	const std::string &ErrorMessage() const {
		return error_;
	}

private:
	friend class WsClient;

	void Push(Frame &&f);
	void DeliverSyntheticError(const std::string &msg);

	uint32_t req_id_;
	std::weak_ptr<WsClient> owner_;

	mutable std::mutex mu_;
	// Per-request wake (paired with mu_): a delivery thread notifies it from Push/DeliverSyntheticError so a
	// blocked Next wakes for THIS request only — no thundering herd across N concurrent requests.
	std::condition_variable cv_;
	std::deque<Frame> q_;
	bool finished_ = false;
	bool cancelled_ = false;
	std::string error_;
	// Fired (a copy, outside mu_) after each Push. Empty unless the async scan set it.
	std::function<void()> on_frame_;
};

class WsClient : public std::enable_shared_from_this<WsClient> {
public:
	static std::shared_ptr<WsClient> Create(WsClientOptions opts);
	~WsClient();

	// Opens a channel and blocks until OPEN_ACK (indefinitely while the server keeps sending OPEN_PENDING);
	// throws on failure/timeout/stall.
	OpenAck Connect(const AbortCheck &should_abort);

	// `should_abort` guards the reconnect this call runs when the connection is down.
	std::shared_ptr<RequestState> AwaitReadySessionAndSendRequest(uint8_t op, const std::string &body,
	                                                              const AbortCheck &should_abort);

	void Credit(uint32_t req_id, uint32_t n);

	void SendCancel(uint32_t req_id);

	// One far transaction. `should_abort` guards the reconnect this call runs when the connection is down. A link
	// never survives the connection: a drop fails it.
	std::shared_ptr<N6kLinkState> OpenLink(DatabaseInstance &db, idx_t streams, const AbortCheck &should_abort);
	void CloseLink(N6kLinkState &link);

	// Server-initiated PUSH dispatch; invoked on the thread delivering inbound frames. A PUSH that arrives while
	// no handler is set is dropped.
	using PushHandler = std::function<void(uint8_t op, const std::string &body)>;
	void SetPushHandler(PushHandler h);

	bool IsConnected() const;

	OpenAck GetOpenAck() const;
	std::vector<std::string> GetHelloCapabilities() const;

	// Proactive teardown (idempotent): fail in-flight reqs + close the channel. From an inbound callback it deadlocks.
	void Close();

	// Non-blocking.
	void MarkDisconnectedIfConnectionReplaced();

private:
	//! OPEN_PENDING frames seen before the final reply, and the status the last one carried.
	struct PendingState {
		uint64_t beats = 0;
		std::string message;
	};

	// The whole handshake state of one session, shared by the channel's inbound thread and requesting
	// threads under state_mu_.
	struct ConnState {
		std::vector<std::string> hello_capabilities;
		//! The OPEN wait is over: an OPEN_ACK, an OPEN_ERR or a recorded handshake error.
		bool open_arrived = false;
		OpenAck open;
		std::string open_error;
		PendingState open_pending;
		//! An OPEN_ACK landed and no drop has been observed since.
		bool connected = false;
	};

	explicit WsClient(WsClientOptions opts);
	WsClient(const WsClient &) = delete;
	WsClient &operator=(const WsClient &) = delete;

	// Handshake state transitions. Each takes state_mu_ for its whole update, so a reader never sees
	// half of one.
	void RecordOpenAck(OpenAck ack);
	void RecordOpenError(const std::string &msg);
	void RecordOpenPending(const std::string &msg);
	void RecordHandshakeError(const std::string &msg);
	void ResetHandshakeForReconnect();

	// Block until ready() or deadline. ready() must self-lock state_mu_ only.
	bool WaitProgress(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline);
	void NotifyProgress();

	// Predicates for WaitProgress.
	bool OpenArrived() const;
	uint64_t OpenPendingBeats() const;

	// Wait for OPEN_ACK/ERR: the first reply within connect_timeout, then each pending frame extends the
	// deadline by open_pending_stall.
	void AwaitOpenReply(const AbortCheck &should_abort);

	// Never stops the reactor, so it is safe from an inbound callback.
	void FailConnectionAndAllInFlightRequests(const std::string &reason);
	// Stops the reactor (idempotent). From an inbound callback it deadlocks.
	void ShutdownFromOwner(const std::string &reason);
	void OnMessage(const std::string &bytes, bool binary);
	void OnClose(const std::string &reason);

	// Re-establish after a drop: close the old channel, open one under a fresh ns, and OPEN again. Throws if
	// the peer is unusable, so the caller sees why.
	void Reconnect(const AbortCheck &should_abort);
	// Reconnect if the connection beneath dropped or was replaced; throw if the client still has none.
	void EnsureConnected(const AbortCheck &should_abort);

	// Open a channel, run the handshake on it, and throw the reason it is unusable. `answered` is true once
	// the OPEN wait ends with an OPEN_ACK, an OPEN_ERR or a recorded handshake error; false for a timeout, a
	// stall, an abort, a stopping client, or a channel that could not open.
	void OpenChannelAndHandshake(bool &answered, const AbortCheck &should_abort);

	void SendFrame(const std::string &frame_bytes);

	// A strong ref, so a caller keeps the channel alive while Reconnect swaps in a new one.
	std::shared_ptr<Reactor> CurrentReactor() const;
	uint64_t CurrentNs() const;

	WsClientOptions opts_;
	// reactor_mu_ guards reactor_ and ns_ together; reconnect_mu_ serializes the whole Reconnect body
	// (with a double-checked state_.connected) so two threads can't both reopen.
	mutable std::mutex reactor_mu_;
	std::mutex reconnect_mu_;
	std::shared_ptr<Reactor> reactor_;
	uint64_t ns_ = 0;

	// Deliberately outside ConnState: a one-way latch, set by ShutdownFromOwner and read on the
	// send path without a lock. It answers "is this object being torn down", which is true regardless
	// of what the handshake state says.
	std::atomic<bool> stopping_ {false};
	// Never reset across reopens, so a late CREDIT/CANCEL for an old request can't name a new one.
	std::atomic<uint32_t> next_req_id_ {1};
	// Reactor::Generation() at the last OPEN_ACK.
	std::atomic<uint64_t> connected_generation_ {0};

	mutable std::mutex state_mu_;
	ConnState state_;

	// Inbound callbacks notify after each dispatch; WaitProgress re-checks state under state_mu_.
	std::mutex progress_mu_;
	std::condition_variable progress_cv_;

	std::mutex reqs_mu_;
	std::unordered_map<uint32_t, std::shared_ptr<RequestState>> reqs_;

	std::atomic<uint32_t> next_link_id_ {1};
	std::mutex links_mu_;
	std::unordered_map<uint32_t, std::shared_ptr<N6kLinkState>> links_;

	std::mutex push_handler_mu_;
	PushHandler push_handler_;
};

} // namespace n6k
} // namespace duckdb
