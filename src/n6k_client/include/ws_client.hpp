#pragma once

#include "n6k_protocol_generated.hpp"
#include "ws_reactor.hpp"

#include "duckdb/common/shared_ptr.hpp"

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

class Logger;

namespace n6k {

struct Frame {
	FrameType type;
	uint8_t flags;
	uint32_t req_id;
	std::string payload;
};

struct OpenAck {
	std::vector<std::string> capabilities;
};

// Polled once a PENDING has arrived for HELLO or OPEN; true throws InterruptException, sending
// CANCEL {ns, id:0} only for OPEN. Called only during the call it is passed to; empty never aborts.
using AbortCheck = std::function<bool()>;

struct WsClientOptions {
	std::string url;
	std::string bearer_token;
	// SERVER-side catalog name; sent in the ?catalog= dial query and the FT_OPEN frame.
	std::string catalog;
	// Wasm only: vsock channel key = LOCAL ATTACH alias (unique per duckdb instance).
	std::string channel_key;
	// Session tag so ONE socket carries many catalog sessions; never 0.
	uint64_t ns = 0;
	// The client owns the socket and so its FT_HELLO. False when riding a host socket (wsFd /
	// wsid), whose owner already sent it.
	bool send_hello = true;
	// Wasm only: heap address of the vsock channel's ring region, so the reactor
	// reads/writes the rings directly with native atomics.
	uintptr_t ring_ptr = 0;
	std::chrono::milliseconds connect_timeout {5000};
	// Once the server answers HELLO / OPEN with HELLO_PENDING / OPEN_PENDING that step has no overall
	// deadline; it fails only when this long passes with no further pending, ACK or ERR.
	std::chrono::milliseconds open_pending_stall {OPEN_PENDING_STALL_MS};
	// >= 0: ride this already-connected socket fd instead of dialing url. Native only.
	int ws_fd = -1;
	// !send_hello: the HELLO_ACK capabilities the socket owner negotiated, asked on every (re)connect.
	std::function<std::vector<std::string>()> host_hello_capabilities;
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

	// Opens the socket, blocks until OPEN_ACK (indefinitely while the server keeps sending HELLO_PENDING /
	// OPEN_PENDING);
	// throws on failure/timeout/stall/version mismatch.
	OpenAck Connect(const AbortCheck &should_abort);

	// `should_abort` guards the reconnect this call runs when the connection is down.
	std::shared_ptr<RequestState> AwaitReadySessionAndSendRequest(uint8_t op, const std::string &body,
	                                                              const AbortCheck &should_abort);

	void Credit(uint32_t req_id, uint32_t n);

	void SendCancel(uint32_t req_id);

	// Server-initiated PUSH dispatch; invoked on the WS callback thread. Set before Connect().
	using PushHandler = std::function<void(uint8_t op, const std::string &body)>;
	void SetPushHandler(PushHandler h);

	bool IsConnected() const;

	OpenAck GetOpenAck() const;
	std::vector<std::string> GetHelloCapabilities() const;

	// Proactive teardown (idempotent): fail in-flight reqs + stop reactor. Never from an inbound callback.
	void Close();

	// Block until ready() or deadline. ready() must self-lock and hold no client mutex (Reactor contract).
	bool WaitProgress(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline);

	// Non-blocking: mark the connection down if the host reset the transport channel. Native: no-op.
	void MarkDisconnectedIfChannelReset();

private:
	enum class HandshakeStep { HELLO, OPEN };

	//! HELLO_PENDING / OPEN_PENDING frames seen before that step's final reply, and the status the last
	//! one carried.
	struct PendingState {
		uint64_t beats = 0;
		std::string message;
	};

	// The whole handshake state of one connection, under one lock: written by the reactor's inbound
	// thread and read by requesting threads, so a reader never sees a combination that never existed.
	struct ConnState {
		//! A HELLO_ACK or a HELLO_ERR has been seen.
		bool hello_arrived = false;
		int protocol_version = 0;
		std::vector<std::string> hello_capabilities;
		std::string hello_error;
		//! An OPEN_ACK or an OPEN_ERR has been seen.
		bool open_arrived = false;
		OpenAck open;
		std::string open_error;
		PendingState hello_pending;
		PendingState open_pending;
		//! An OPEN_ACK landed and no drop has been observed since.
		bool connected = false;
	};

	explicit WsClient(WsClientOptions opts);
	WsClient(const WsClient &) = delete;
	WsClient &operator=(const WsClient &) = delete;

	// Inbound-thread state transitions. Each takes state_mu_ for its whole update, so a reader never
	// sees half of one.
	void RecordHelloAck(int protocol_version, std::vector<std::string> capabilities);
	void RecordOpenAck(OpenAck ack);
	void RecordOpenError(const std::string &msg);
	void RecordPending(HandshakeStep step, const std::string &msg);
	void RecordHandshakeError(const std::string &msg);
	void ResetHandshakeForReconnect();

	// Predicates for WaitProgress. Each self-locks and must be called with no other client mutex
	// held (Reactor contract: the wasm reactor dispatches inbound frames between checks).
	bool HelloArrived() const;
	bool OpenArrived() const;
	uint64_t PendingBeats(HandshakeStep step) const;

	// Wait for that step's ACK/ERR: the first reply within connect_timeout, then each pending frame
	// extends the deadline by open_pending_stall.
	void AwaitReply(HandshakeStep step, const AbortCheck &should_abort);

	// From an inbound callback: fail in-flight reqs, record error, mark disconnected. No reactor stop.
	void FailConnectionAndAllInFlightRequests(const std::string &reason);
	// Stops the reactor (idempotent). From the owner (dtor/Close) only, never an inbound callback.
	void ShutdownFromOwner(const std::string &reason);
	void OnMessage(const std::string &bytes, bool binary);
	void OnClose(const std::string &reason);

	// Re-establish after a drop: reset handshake, rebuild reactor, re-run HELLO (when owned) and OPEN.
	// Throws if the peer is unusable, so the caller sees why.
	void Reconnect(const AbortCheck &should_abort);

	// Start a reactor, run the handshake on it, and throw the reason it is unusable. `answered` reports
	// whether the server replied at all, so a bare timeout can be told from a refusal.
	void StartReactorAndHandshake(bool &answered, const AbortCheck &should_abort);

	void SendFrame(const std::string &frame_bytes);

	// Snapshot the current reactor as a strong ref so a caller keeps it alive
	// while Reconnect concurrently swaps in a new one — avoids the use-after-free of a bare pointer.
	std::shared_ptr<Reactor> CurrentReactor() const;

	WsClientOptions opts_;
	// reactor_ is hot-swapped across a reconnect and read concurrently by compute pthreads under
	// threads>1. reactor_mu_ guards the pointer itself; reconnect_mu_ serializes the whole Reconnect
	// body (with a double-checked state_.connected) so two threads can't both rebuild + spawn service threads.
	mutable std::mutex reactor_mu_;
	std::mutex reconnect_mu_;
	std::shared_ptr<Reactor> reactor_;

	// Deliberately outside ConnState: a one-way latch, set by the owner (dtor/Close) and read on the
	// send path without a lock. It answers "is this object being torn down", which is true regardless
	// of what the handshake state says.
	std::atomic<bool> stopping_ {false};
	std::atomic<uint32_t> next_req_id_ {1};
	// Transport channel epoch at last (re)connect; if the reactor moved past it the host reset us (wasm only).
	// Atomic rather than part of ConnState: it is compared against Reactor::Epoch(), which is read
	// without this lock, so pairing it with the handshake fields would buy no extra consistency.
	std::atomic<uint64_t> connected_epoch_ {0};

	// The reactor signals progress and WaitProgress re-checks the state under this one lock.
	mutable std::mutex state_mu_;
	ConnState state_;

	std::mutex reqs_mu_;
	std::unordered_map<uint32_t, std::shared_ptr<RequestState>> reqs_;

	std::mutex push_handler_mu_;
	PushHandler push_handler_;
};

} // namespace n6k
} // namespace duckdb
