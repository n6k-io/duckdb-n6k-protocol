#include "ws_client.hpp"
#include "ws_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_ws_log.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/logging/logger.hpp"

#include <algorithm>
#include <stdexcept>

namespace duckdb {
namespace n6k {

uint64_t NextSessionNs() {
	static std::atomic<uint64_t> next {1};
	return next.fetch_add(1);
}

RequestState::RequestState(uint32_t req_id, std::weak_ptr<WsClient> owner) : req_id_(req_id), owner_(std::move(owner)) {
}

void RequestState::Push(Frame &&f) {
	std::function<void()> cb;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (finished_) {
			return;
		}
		bool end = (f.flags & FLAG_END_OF_STREAM) != 0 || f.type == FrameType::ERR;
		if (f.type == FrameType::ERR) {
			error_ = f.payload;
		}
		q_.push_back(std::move(f));
		if (end) {
			finished_ = true;
		}
		cb = on_frame_;
	}
	cv_.notify_all();
	// Fire outside the lock: the callback may re-enter this RequestState (TryNext) or reschedule the
	// pipeline (interrupt_state.Callback()); holding mu_ across it would risk deadlock.
	if (cb) {
		cb();
	}
}

void RequestState::DeliverSyntheticError(const std::string &msg) {
	std::function<void()> cb;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (finished_) {
			return;
		}
		error_ = msg;
		Frame f;
		f.type = FrameType::ERR;
		f.flags = FLAG_END_OF_STREAM;
		f.req_id = req_id_;
		f.payload = msg;
		q_.push_back(std::move(f));
		finished_ = true;
		cb = on_frame_;
	}
	cv_.notify_all();
	// A transport failure is terminal — wake any blocked scan so it observes the ERR and finishes.
	if (cb) {
		cb();
	}
}

bool RequestState::TryNext(Frame &out) {
	std::lock_guard<std::mutex> lk(mu_);
	if (!q_.empty()) {
		out = std::move(q_.front());
		q_.pop_front();
		return true;
	}
	return false;
}

void RequestState::SetOnFrame(std::function<void()> cb) {
	std::lock_guard<std::mutex> lk(mu_);
	on_frame_ = std::move(cb);
}

bool RequestState::Next(Frame &out, std::chrono::milliseconds timeout) {
	auto deadline = std::chrono::steady_clock::now() + timeout;
	auto owner = owner_.lock();
	std::unique_lock<std::mutex> lk(mu_);
	for (;;) {
		if (!q_.empty()) {
			out = std::move(q_.front());
			q_.pop_front();
			return true;
		}
		if (finished_) {
			return false;
		}
		if (!owner || std::chrono::steady_clock::now() >= deadline) {
			return false;
		}
		// A delivery thread Pushes+notifies cv_; park on it directly (no broadcast herd).
		cv_.wait_until(lk, deadline, [this] { return !q_.empty() || finished_; });
	}
}

bool RequestState::Exhausted() const {
	std::lock_guard<std::mutex> lk(mu_);
	return finished_ && q_.empty();
}

void RequestState::Credit(uint32_t n) {
	if (auto owner = owner_.lock()) {
		owner->Credit(req_id_, n);
	}
}

void RequestState::Cancel() {
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (cancelled_ || finished_) {
			return;
		}
		cancelled_ = true;
	}
	if (auto owner = owner_.lock()) {
		owner->SendCancel(req_id_);
	}
}

std::shared_ptr<WsClient> WsClient::Create(WsClientOptions opts) {
	return std::shared_ptr<WsClient>(new WsClient(std::move(opts)));
}

WsClient::WsClient(WsClientOptions opts) : opts_(std::move(opts)) {
}

WsClient::~WsClient() {
	ShutdownFromOwner("client shutdown");
}

void WsClient::RecordOpenAck(OpenAck ack) {
	std::lock_guard<std::mutex> lk(state_mu_);
	state_.open = std::move(ack);
	state_.open_arrived = true;
	state_.connected = true;
}

void WsClient::RecordOpenError(const std::string &msg) {
	std::lock_guard<std::mutex> lk(state_mu_);
	if (!state_.open_arrived) {
		state_.open_error = msg;
		state_.open_arrived = true;
	}
}

void WsClient::RecordOpenPending(const std::string &msg) {
	std::lock_guard<std::mutex> lk(state_mu_);
	if (!state_.open_arrived) {
		state_.open_pending.beats++;
		state_.open_pending.message = msg;
	}
}

void WsClient::RecordHandshakeError(const std::string &msg) {
	std::lock_guard<std::mutex> lk(state_mu_);
	// An OPEN that already has an answer keeps it: the first reason recorded is the most specific one.
	if (!state_.open_arrived) {
		state_.open_error = msg;
		if (state_.open_pending.beats > 0) {
			state_.open_error +=
			    " (catalog \"" + opts_.catalog + "\" was pending: " + state_.open_pending.message + ")";
		}
		state_.open_arrived = true;
	}
}

void WsClient::ResetHandshakeForReconnect() {
	std::lock_guard<std::mutex> lk(state_mu_);
	state_ = ConnState();
}

bool WsClient::OpenArrived() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.open_arrived;
}

uint64_t WsClient::OpenPendingBeats() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.open_pending.beats;
}

void WsClient::AwaitOpenReply(const AbortCheck &should_abort) {
	auto deadline = std::chrono::steady_clock::now() + opts_.connect_timeout;
	uint64_t seen = 0;
	while (true) {
		// While pending, wake at least every 250ms to poll should_abort.
		auto wake = deadline;
		if (seen > 0 && should_abort) {
			wake = std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(250));
		}
		WaitProgress([this, seen] { return OpenArrived() || OpenPendingBeats() != seen; }, wake);
		if (OpenArrived()) {
			return;
		}
		if (seen > 0 && should_abort && should_abort()) {
			SendFrame(PackIdOnlyFrame(static_cast<int>(FrameType::CANCEL), 0, CurrentNs()));
			throw InterruptException();
		}
		const auto beats = OpenPendingBeats();
		if (beats != seen) {
			seen = beats;
			deadline = std::chrono::steady_clock::now() + opts_.open_pending_stall;
			continue;
		}
		const auto now = std::chrono::steady_clock::now();
		// WaitProgress returns early with nothing new only once the client is stopping.
		if (stopping_.load() || now < wake) {
			throw std::runtime_error("n6k ws client closed while opening catalog \"" + opts_.catalog + "\"");
		}
		if (now < deadline) {
			continue;
		}
		if (seen == 0) {
			throw std::runtime_error("n6k ws connect timeout");
		}
		std::string status;
		{
			std::lock_guard<std::mutex> lk(state_mu_);
			status = state_.open_pending.message;
		}
		throw std::runtime_error("n6k: catalog \"" + opts_.catalog + "\" open stalled: no FT_OPEN_PENDING for " +
		                         std::to_string(opts_.open_pending_stall.count()) + "ms (last status: " + status + ")");
	}
}

bool WsClient::IsConnected() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.connected;
}

std::shared_ptr<Reactor> WsClient::CurrentReactor() const {
	std::lock_guard<std::mutex> lk(reactor_mu_);
	return reactor_;
}

uint64_t WsClient::CurrentNs() const {
	std::lock_guard<std::mutex> lk(reactor_mu_);
	return ns_;
}

void WsClient::SendFrame(const std::string &frame_bytes) {
	if (auto r = CurrentReactor()) {
		r->Send(frame_bytes);
		if (opts_.logger) {
			DUCKDB_LOG(opts_.logger, WsLogType, "send", frame_bytes);
		}
	}
}

bool WsClient::WaitProgress(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) {
	std::unique_lock<std::mutex> lk(progress_mu_);
	progress_cv_.wait_until(lk, deadline, [&] { return ready() || stopping_.load(); });
	return ready();
}

void WsClient::NotifyProgress() {
	{ std::lock_guard<std::mutex> lk(progress_mu_); }
	progress_cv_.notify_all();
}

void WsClient::MarkDisconnectedIfChannelReset() {
	if (stopping_.load()) {
		return;
	}
	auto r = CurrentReactor();
	if (!r) {
		return;
	}
	if (r->Generation() != connected_generation_.load()) {
		std::lock_guard<std::mutex> lk(state_mu_);
		state_.connected = false;
	}
}

void WsClient::OnMessage(const std::string &bytes, bool binary) {
	if (opts_.logger) {
		DUCKDB_LOG(opts_.logger, WsLogType, "recv", bytes);
	}
	if (!binary) {
		RecordHandshakeError("unexpected text frame from server");
		return;
	}
	// Bytes after the msgpack header map are the raw Arrow body. Unguarded: ConnectionHub::OnMessage delivers
	// only frames whose header parses to a map.
	size_t off = 0;
	msgpack::object_handle oh = msgpack::unpack(bytes.data(), bytes.size(), off);
	const msgpack::object &h = oh.get();
	const auto type = static_cast<FrameType>(GetInt(h, "t", -1));
	const auto req_id = static_cast<uint32_t>(GetInt(h, "id", 0));
	const char *body_ptr = bytes.data() + off;
	const size_t body_len = bytes.size() - off;

	if (type == FrameType::OPEN_ACK) {
		OpenAck ack;
		ack.capabilities = GetStrArray(h, "capabilities");
		RecordOpenAck(std::move(ack));
		return;
	}

	if (type == FrameType::OPEN_PENDING) {
		RecordOpenPending(GetStr(h, "message"));
		return;
	}

	if (type == FrameType::OPEN_ERR) {
		const std::string etype = GetStr(h, "exception_type");
		const std::string emsg = GetStr(h, "exception_message");
		RecordOpenError(etype.empty() ? emsg : etype + ": " + emsg);
		return;
	}

	if (type == FrameType::PING) {
		SendFrame(PackPong(static_cast<int>(FrameType::PONG), HasKey(h, "id"), req_id));
		return;
	}

	if (type == FrameType::PUSH) {
		PushHandler hdl;
		{
			std::lock_guard<std::mutex> lk(push_handler_mu_);
			hdl = push_handler_;
		}
		if (hdl) {
			std::string payload;
			try {
				payload = HeaderPayloadJsonExcludingRouting(h);
			} catch (const std::exception &e) {
				FailConnectionAndAllInFlightRequests(std::string("malformed PUSH frame from server: ") + e.what());
				return;
			}
			hdl(static_cast<uint8_t>(GetInt(h, "op")), payload);
		}
		return;
	}

	// Rebuild the Frame: Arrow bodies stay raw; everything else reconstructs to JSON from the header.
	const bool is_arrow = (type == FrameType::RESP_SCHEMA) || HasKey(h, "arrow");
	const bool terminal = (type == FrameType::RESP_END) || (type == FrameType::ERR);

	Frame f;
	f.type = type;
	f.req_id = req_id;
	f.flags = 0;
	if (is_arrow) {
		f.flags |= FLAG_IS_ARROW_IPC;
		f.payload.assign(body_ptr, body_len);
	} else {
		try {
			f.payload = HeaderPayloadJsonExcludingRouting(h);
		} catch (const std::exception &e) {
			FailConnectionAndAllInFlightRequests(std::string("malformed frame from server: ") + e.what());
			return;
		}
	}
	if (terminal) {
		f.flags |= FLAG_END_OF_STREAM;
	}

	std::shared_ptr<RequestState> st;
	{
		std::lock_guard<std::mutex> lk(reqs_mu_);
		auto it = reqs_.find(req_id);
		if (it == reqs_.end()) {
			return;
		}
		st = it->second;
		if (terminal) {
			reqs_.erase(it);
		}
	}
	st->Push(std::move(f));
}

void WsClient::OnClose(const std::string &reason) {
	// Runs on a transport thread or inside a caller's hub call (ConnectionHub::BindSession, SendOnGeneration,
	// Reconnect, Disconnect): must not close the channel.
	FailConnectionAndAllInFlightRequests(reason.empty() ? "socket closed" : reason);
}

void WsClient::OpenChannelAndHandshake(bool &answered, const AbortCheck &should_abort) {
	answered = false;
	const uint64_t ns = NextSessionNs();
	std::shared_ptr<Reactor> r(opts_.open_channel(ns, should_abort));
	{
		std::lock_guard<std::mutex> lk(reactor_mu_);
		reactor_ = r;
		ns_ = ns;
	}
	// A strong self in the callbacks keeps the client alive through any in flight. The channel may hold them
	// until it is destroyed, so dropping reactor_ is what breaks the cycle.
	auto self = shared_from_this();
	r->Start(
	    [self](const std::string &bytes, bool binary) {
		    self->OnMessage(bytes, binary);
		    self->NotifyProgress();
	    },
	    [self](const std::string &reason) {
		    self->OnClose(reason);
		    self->NotifyProgress();
	    });

	if (opts_.hello_capabilities) {
		auto caps = opts_.hello_capabilities();
		std::lock_guard<std::mutex> lk(state_mu_);
		state_.hello_capabilities = std::move(caps);
	}

	SendFrame(PackOpen(static_cast<int>(FrameType::OPEN), ns, opts_.catalog));
	AwaitOpenReply(should_abort);
	answered = true;
	{
		std::lock_guard<std::mutex> lk(state_mu_);
		if (!state_.open_error.empty()) {
			throw std::runtime_error(state_.open_error);
		}
	}
	connected_generation_.store(r->Generation());
}

OpenAck WsClient::Connect(const AbortCheck &should_abort) {
	bool answered;
	try {
		OpenChannelAndHandshake(answered, should_abort);
	} catch (const std::exception &e) {
		ShutdownFromOwner(e.what());
		throw;
	}
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.open;
}

void WsClient::Reconnect(const AbortCheck &should_abort) {
	// Serialize the whole rebuild: under threads>1 several compute pthreads can observe a disconnected
	// state and all call Reconnect. The first rebuilds; the rest re-check under this lock and bail, so
	// we never race the reactor_ swap.
	std::lock_guard<std::mutex> reconnect_lk(reconnect_mu_);
	if (IsConnected() || stopping_.load()) {
		return;
	}
	ResetHandshakeForReconnect();
	// Releases the old ns on the server before a fresh one opens.
	if (auto old = CurrentReactor()) {
		old->Close();
	}
	bool answered;
	try {
		OpenChannelAndHandshake(answered, should_abort);
	} catch (const std::exception &e) {
		{
			std::lock_guard<std::mutex> lk(state_mu_);
			state_.connected = false;
		}
		// An OPEN_ERR, a drop during the reopen or a bad frame stops the client for good: only a re-ATTACH
		// recovers. Otherwise (timeout, stall, abort, failed dial) a later request reconnects again.
		if (answered) {
			ShutdownFromOwner(e.what());
		}
		throw;
	}
}

std::shared_ptr<RequestState> WsClient::AwaitReadySessionAndSendRequest(uint8_t op, const std::string &body,
                                                                        const AbortCheck &should_abort) {
	// Lazily re-establish a dropped (not closed) connection: reopen + re-handshake so a server drop recovers with no
	// re-ATTACH. A moved generation means the connection beneath was replaced: clear `connected` to force a fresh OPEN.
	if (IsConnected() && !stopping_.load()) {
		auto r = CurrentReactor();
		if (r && r->Generation() != connected_generation_.load()) {
			std::lock_guard<std::mutex> lk(state_mu_);
			state_.connected = false;
		}
	}
	if (!IsConnected() && !stopping_.load()) {
		Reconnect(should_abort);
	}
	if (!IsConnected() || stopping_.load()) {
		throw std::runtime_error("ws client not connected");
	}
	uint32_t req_id = next_req_id_.fetch_add(1);
	std::weak_ptr<WsClient> weak_self = shared_from_this();
	auto st = std::make_shared<RequestState>(req_id, weak_self);
	{
		std::lock_guard<std::mutex> lk(reqs_mu_);
		reqs_[req_id] = st;
	}
	// These ops append a raw Arrow body after a `json-header\n` prefix; split it out here.
	const char *args = body.data();
	size_t args_len = body.size();
	const char *arrow = nullptr;
	size_t arrow_len = 0;
	if (op == OP_INSERT || op == OP_RPC_TABLE || op == OP_WRITE_PLAN) {
		size_t nl = body.find('\n');
		if (nl != std::string::npos) {
			args_len = nl;
			arrow = body.data() + nl + 1;
			arrow_len = body.size() - nl - 1;
		}
	}
	SendFrame(
	    PackReqFrame(static_cast<int>(FrameType::REQ), req_id, op, args, args_len, arrow, arrow_len, CurrentNs()));
	return st;
}

void WsClient::SetPushHandler(PushHandler h) {
	std::lock_guard<std::mutex> lk(push_handler_mu_);
	push_handler_ = std::move(h);
}

void WsClient::Credit(uint32_t req_id, uint32_t n) {
	if (stopping_.load()) {
		return;
	}
	SendFrame(PackCredit(static_cast<int>(FrameType::CREDIT), req_id, n, CurrentNs()));
}

void WsClient::SendCancel(uint32_t req_id) {
	if (stopping_.load()) {
		return;
	}
	SendFrame(PackIdOnlyFrame(static_cast<int>(FrameType::CANCEL), req_id, CurrentNs()));
}

void WsClient::FailConnectionAndAllInFlightRequests(const std::string &reason) {
	// The OPEN gate opens with the drop reason before any request is failed, so a thread woken by the
	// synthetic error below already sees why. RecordHandshakeError leaves an OPEN that already has an
	// answer alone.
	{
		std::lock_guard<std::mutex> lk(state_mu_);
		state_.connected = false;
	}
	RecordHandshakeError(reason);

	std::unordered_map<uint32_t, std::shared_ptr<RequestState>> reqs;
	{
		std::lock_guard<std::mutex> lk(reqs_mu_);
		reqs.swap(reqs_);
	}
	// Outside every client lock: DeliverSyntheticError fires the request's on_frame callback, which
	// reschedules a parked pipeline and must not run with state_mu_ held.
	for (auto &kv : reqs) {
		kv.second->DeliverSyntheticError(reason);
	}
}

void WsClient::Close() {
	ShutdownFromOwner("client closed");
}

OpenAck WsClient::GetOpenAck() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.open;
}

std::vector<std::string> WsClient::GetHelloCapabilities() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.hello_capabilities;
}

void WsClient::ShutdownFromOwner(const std::string &reason) {
	bool expected = false;
	if (!stopping_.compare_exchange_strong(expected, true)) {
		return;
	}

	FailConnectionAndAllInFlightRequests(reason);
	NotifyProgress();

	std::shared_ptr<Reactor> r;
	{
		std::lock_guard<std::mutex> lk(reactor_mu_);
		r = std::move(reactor_);
	}
	if (r) {
		r->Close();
	}
}

} // namespace n6k
} // namespace duckdb
