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

void WsClient::RecordHelloAck(int protocol_version, std::vector<std::string> capabilities) {
	std::lock_guard<std::mutex> lk(state_mu_);
	state_.protocol_version = protocol_version;
	state_.hello_capabilities = std::move(capabilities);
	state_.hello_arrived = true;
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

void WsClient::RecordPending(HandshakeStep step, const std::string &msg) {
	std::lock_guard<std::mutex> lk(state_mu_);
	const bool hello = step == HandshakeStep::HELLO;
	if (!(hello ? state_.hello_arrived : state_.open_arrived)) {
		auto &pending = hello ? state_.hello_pending : state_.open_pending;
		pending.beats++;
		pending.message = msg;
	}
}

void WsClient::RecordHandshakeError(const std::string &msg) {
	std::lock_guard<std::mutex> lk(state_mu_);
	// A gate that already has an answer keeps it: the first reason recorded is the most specific one.
	if (!state_.hello_arrived) {
		state_.hello_error = msg;
		if (state_.hello_pending.beats > 0) {
			state_.hello_error += " (handshake was pending: " + state_.hello_pending.message + ")";
		}
		state_.hello_arrived = true;
	}
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

bool WsClient::HelloArrived() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.hello_arrived;
}

bool WsClient::OpenArrived() const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return state_.open_arrived;
}

uint64_t WsClient::PendingBeats(HandshakeStep step) const {
	std::lock_guard<std::mutex> lk(state_mu_);
	return step == HandshakeStep::HELLO ? state_.hello_pending.beats : state_.open_pending.beats;
}

void WsClient::AwaitReply(HandshakeStep step, const AbortCheck &should_abort) {
	const bool hello = step == HandshakeStep::HELLO;
	auto arrived = [this, hello] {
		return hello ? HelloArrived() : OpenArrived();
	};
	auto deadline = std::chrono::steady_clock::now() + opts_.connect_timeout;
	uint64_t seen = 0;
	while (true) {
		// While pending, wake at least every 250ms to poll should_abort.
		auto wake = deadline;
		if (seen > 0 && should_abort) {
			wake = std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds(250));
		}
		WaitProgress([this, &arrived, step, seen] { return arrived() || PendingBeats(step) != seen; }, wake);
		if (arrived()) {
			return;
		}
		if (seen > 0 && should_abort && should_abort()) {
			if (!hello) {
				SendFrame(PackIdOnlyFrame(static_cast<int>(FrameType::CANCEL), 0, opts_.ns));
			}
			throw InterruptException();
		}
		const auto beats = PendingBeats(step);
		if (beats != seen) {
			seen = beats;
			deadline = std::chrono::steady_clock::now() + opts_.open_pending_stall;
			continue;
		}
		const auto now = std::chrono::steady_clock::now();
		// WaitUntil returns early with nothing new only once the reactor is stopping.
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
			status = hello ? state_.hello_pending.message : state_.open_pending.message;
		}
		const std::string what = hello ? "handshake" : "catalog \"" + opts_.catalog + "\" open";
		throw std::runtime_error("n6k: " + what + " stalled: no FT_" + (hello ? "HELLO" : "OPEN") + "_PENDING for " +
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

void WsClient::SendFrame(const std::string &frame_bytes) {
	if (auto r = CurrentReactor()) {
		r->Send(frame_bytes);
		if (opts_.logger) {
			DUCKDB_LOG(opts_.logger, WsLogType, "send", frame_bytes);
		}
	}
}

bool WsClient::WaitProgress(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) {
	auto r = CurrentReactor();
	if (!r) {
		return ready();
	}
	return r->WaitUntil(ready, deadline);
}

void WsClient::MarkDisconnectedIfChannelReset() {
	if (stopping_.load()) {
		return;
	}
	auto r = CurrentReactor();
	if (!r) {
		return;
	}
	// A moved epoch means the host reset the transport channel: this reactor belongs to the OLD
	// generation. Mark disconnected so the lazy Reconnect runs.
	// Not gated on state_.connected, so every idle poll in the reset window trips. Native reactors report
	// epoch 0 == connected_epoch_, so this never trips off-wasm.
	if (r->Epoch() != connected_epoch_.load()) {
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
	// Bytes after the msgpack header map are the raw Arrow body.
	//
	// An unparseable frame carries no req_id, so there is no request to fail individually and a
	// silent drop hangs it to its deadline. Version skew or corruption is a connection-level fault:
	// fail every in-flight request, clear state_.connected, and let the lazy Reconnect rebuild.
	msgpack::object_handle oh;
	size_t off = 0;
	try {
		oh = msgpack::unpack(bytes.data(), bytes.size(), off);
	} catch (const std::exception &e) {
		FailConnectionAndAllInFlightRequests(std::string("malformed frame from server (msgpack): ") + e.what());
		return;
	}
	const msgpack::object &h = oh.get();
	if (h.type != msgpack::type::MAP) {
		FailConnectionAndAllInFlightRequests("malformed frame from server: header is not a msgpack map");
		return;
	}
	const auto type = static_cast<FrameType>(GetInt(h, "t", -1));
	const auto req_id = static_cast<uint32_t>(GetInt(h, "id", 0));
	const char *body_ptr = bytes.data() + off;
	const size_t body_len = bytes.size() - off;

	if (type == FrameType::HELLO_ACK) {
		RecordHelloAck(static_cast<int>(GetInt(h, "protocol_version")), GetStrArray(h, "capabilities"));
		return;
	}

	if (type == FrameType::OPEN_ACK) {
		OpenAck ack;
		ack.capabilities = GetStrArray(h, "capabilities");
		RecordOpenAck(std::move(ack));
		return;
	}

	if (type == FrameType::OPEN_PENDING || type == FrameType::HELLO_PENDING) {
		RecordPending(type == FrameType::HELLO_PENDING ? HandshakeStep::HELLO : HandshakeStep::OPEN,
		              GetStr(h, "message"));
		return;
	}

	if (type == FrameType::HELLO_ERR || type == FrameType::OPEN_ERR) {
		const std::string etype = GetStr(h, "exception_type");
		const std::string emsg = GetStr(h, "exception_message");
		const std::string msg = etype.empty() ? emsg : etype + ": " + emsg;
		if (type == FrameType::HELLO_ERR) {
			RecordHandshakeError(msg);
		} else {
			RecordOpenError(msg);
		}
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
	// Runs on the transport callback thread: must not stop the socket or join the writer.
	FailConnectionAndAllInFlightRequests(reason.empty() ? "socket closed" : reason);
}

void WsClient::StartReactorAndHandshake(bool &answered, const AbortCheck &should_abort) {
	// Capture a strong self in reactor callbacks so the client outlives in-flight callbacks; ShutdownFromOwner breaks
	// the cycle.
	auto self = shared_from_this();
	auto r = std::shared_ptr<Reactor>(CreateReactor(opts_));
	r->Start([self](const std::string &bytes, bool binary) { self->OnMessage(bytes, binary); },
	         [self](const std::string &reason) { self->OnClose(reason); });
	{
		std::lock_guard<std::mutex> lk(reactor_mu_);
		reactor_ = r;
	}

	answered = false;
	if (opts_.send_hello) {
		// Offers no APP_HELLO_CAPS: OnMessage treats any text frame as a handshake error.
		SendFrame(PackHello(static_cast<int>(FrameType::HELLO), opts_.bearer_token,
		                    std::array<const char *, 1> {{CAP_CROSSING}}));
		AwaitReply(HandshakeStep::HELLO, should_abort);
		answered = true;
		std::lock_guard<std::mutex> lk(state_mu_);
		if (!state_.hello_error.empty()) {
			throw std::runtime_error(state_.hello_error);
		}
		if (state_.protocol_version != N6K_PROTOCOL_VERSION) {
			throw std::runtime_error("server protocol_version=" + std::to_string(state_.protocol_version) +
			                         ", expected " + std::to_string(N6K_PROTOCOL_VERSION));
		}
	} else if (opts_.host_hello_capabilities) {
		auto caps = opts_.host_hello_capabilities();
		std::lock_guard<std::mutex> lk(state_mu_);
		state_.hello_capabilities = std::move(caps);
	}

	SendFrame(PackOpen(static_cast<int>(FrameType::OPEN), opts_.ns, opts_.catalog));
	AwaitReply(HandshakeStep::OPEN, should_abort);
	answered = true;
	{
		std::lock_guard<std::mutex> lk(state_mu_);
		if (!state_.open_error.empty()) {
			throw std::runtime_error(state_.open_error);
		}
	}
	// Pin the epoch we connected on, so a later host-driven reset shows as a moved epoch.
	connected_epoch_.store(r->Epoch());
}

OpenAck WsClient::Connect(const AbortCheck &should_abort) {
	bool answered;
	try {
		StartReactorAndHandshake(answered, should_abort);
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
	// we never race the reactor_ swap or spawn duplicate service threads.
	std::lock_guard<std::mutex> reconnect_lk(reconnect_mu_);
	if (IsConnected() || stopping_.load()) {
		return;
	}
	ResetHandshakeForReconnect();
	if (auto old = CurrentReactor()) {
		old->RetireKeepingChannelOpen();
	}
	bool answered;
	try {
		StartReactorAndHandshake(answered, should_abort);
	} catch (const std::exception &e) {
		{
			std::lock_guard<std::mutex> lk(state_mu_);
			state_.connected = false;
		}
		// A peer that answered but is unusable will not become usable by retrying, so tear down. A bare
		// timeout leaves the client alive and a later request reconnects again.
		if (answered) {
			ShutdownFromOwner(e.what());
		}
		throw;
	}
}

std::shared_ptr<RequestState> WsClient::AwaitReadySessionAndSendRequest(uint8_t op, const std::string &body,
                                                                        const AbortCheck &should_abort) {
	// Lazily re-establish a dropped (not closed) connection: rebuild + re-handshake so a server drop recovers with no
	// re-ATTACH. A moved epoch means an out-of-band channel swap (wasm replaceWebsocket): clear `connected` to force a
	// fresh OPEN.
	if (IsConnected() && !stopping_.load()) {
		auto r = CurrentReactor();
		if (r && r->Epoch() != connected_epoch_.load()) {
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
	SendFrame(PackReqFrame(static_cast<int>(FrameType::REQ), req_id, op, args, args_len, arrow, arrow_len, opts_.ns));
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
	SendFrame(PackCredit(static_cast<int>(FrameType::CREDIT), req_id, n, opts_.ns));
}

void WsClient::SendCancel(uint32_t req_id) {
	if (stopping_.load()) {
		return;
	}
	SendFrame(PackIdOnlyFrame(static_cast<int>(FrameType::CANCEL), req_id, opts_.ns));
}

void WsClient::FailConnectionAndAllInFlightRequests(const std::string &reason) {
	// Both gates open with the drop reason before any request is failed, so a thread woken by the
	// synthetic error below already sees why. RecordHandshakeError leaves a gate that already has an
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

	// Stop the reactor: joins/releases its transport and destroys the self-holding callbacks, breaking the cycle.
	std::shared_ptr<Reactor> r;
	{
		std::lock_guard<std::mutex> lk(reactor_mu_);
		r = std::move(reactor_);
	}
	if (r) {
		r->Stop();
	}
}

} // namespace n6k
} // namespace duckdb
