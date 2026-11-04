#include "connection_hub.hpp"

#include "n6k_host_callback.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"
#include "n6k_stream_framing.hpp"
#include "n6k_ws_log.hpp"
#include "ws_msgpack.hpp"
#ifndef WASM_LOADABLE_EXTENSIONS
#include "ws_transport_ix.hpp"
#endif

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"

#include <algorithm>
#include <array>

namespace duckdb {
namespace n6k {

static constexpr std::chrono::milliseconds ABORT_POLL {250};

std::shared_ptr<ConnectionHub> ConnectionHub::Create(const shared_ptr<DatabaseInstance> &db, Options opts) {
	return Create(db, std::move(opts), UUID::GenerateRandomUUID());
}

std::shared_ptr<ConnectionHub> ConnectionHub::Create(const shared_ptr<DatabaseInstance> &db, Options opts,
                                                     const hugeint_t &id) {
	std::shared_ptr<ConnectionHub> hub(new ConnectionHub(db, std::move(opts), id));
	hub->self_ = hub;
	return hub;
}

ConnectionHub::ConnectionHub(const shared_ptr<DatabaseInstance> &db, Options opts, const hugeint_t &id)
    : opts_(std::move(opts)), db_(db), id_(id), token_(opts_.token) {
	if (opts_.queue_sends) {
		writer_ = std::thread([this]() { WriterLoop(); });
	}
}

ConnectionHub::~ConnectionHub() {
	Disconnect("connection released");
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		writer_stop_ = true;
	}
	out_cv_.notify_all();
	if (writer_.joinable()) {
		writer_.join();
	}
}

void ConnectionHub::Enqueue(uint64_t generation, bool binary, std::string bytes) {
	if (!opts_.queue_sends) {
		Write(Outbound {generation, binary, std::move(bytes)});
		return;
	}
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		out_q_.push_back(Outbound {generation, binary, std::move(bytes)});
	}
	out_cv_.notify_one();
}

void ConnectionHub::WriterLoop() {
	for (;;) {
		Outbound out;
		{
			std::unique_lock<std::mutex> lk(out_mu_);
			out_cv_.wait(lk, [&] { return writer_stop_ || !out_q_.empty(); });
			if (writer_stop_) {
				return;
			}
			out = std::move(out_q_.front());
			out_q_.pop_front();
		}
		Write(out);
	}
}

void ConnectionHub::Write(const Outbound &out) {
	std::shared_ptr<Transport> transport;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (out.generation != gen_) {
			return;
		}
		transport = transport_;
	}
	if (!transport) {
		return;
	}
	const bool sent = out.binary ? transport->Send(out.bytes) : transport->SendText(out.bytes);
	if (!sent) {
		OnTransportClose(out.generation, "n6k: " + opts_.endpoint + " refused an outbound frame");
	}
}

std::string ConnectionHub::IdString() const {
	return UUID::ToString(id_);
}

bool ConnectionHub::IsAlive() {
	std::lock_guard<std::mutex> lk(mu_);
	return open_;
}

bool ConnectionHub::IsDisconnected() {
	std::lock_guard<std::mutex> lk(mu_);
	return disconnected_;
}

bool ConnectionHub::IsDead() {
	std::lock_guard<std::mutex> lk(mu_);
	return disconnected_ || (dropped_ && !opts_.can_redial);
}

std::vector<std::string> ConnectionHub::HelloCapabilities() {
	std::lock_guard<std::mutex> lk(mu_);
	return capabilities_;
}

size_t ConnectionHub::SessionCount() {
	std::lock_guard<std::mutex> lk(mu_);
	return subs_.size();
}

uint64_t ConnectionHub::Generation() {
	std::lock_guard<std::mutex> lk(mu_);
	return gen_;
}

void ConnectionHub::HoldLocked(std::vector<SubscriberPtr> &held, const SubscriberPtr &sub) {
	sub->in_flight++;
	held.push_back(sub);
}

void ConnectionHub::Dispatch(std::vector<SubscriberPtr> &held, const std::function<void(Subscriber &)> &fn) {
	if (held.empty()) {
		return;
	}
	struct ReleaseOnExit {
		ConnectionHub &hub;
		std::vector<SubscriberPtr> &held;
		~ReleaseOnExit() {
			{
				std::lock_guard<std::mutex> lk(hub.mu_);
				for (auto &sub : held) {
					sub->in_flight--;
				}
			}
			hub.idle_cv_.notify_all();
		}
	} release {*this, held};
	for (auto &sub : held) {
		fn(*sub);
	}
}

void ConnectionHub::EnsureConnected(const AbortCheck &should_abort) {
	std::lock_guard<std::mutex> connect_lk(connect_mu_);
	std::shared_ptr<Transport> old;
	bool redialing;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (disconnected_) {
			throw IOException("n6k: connection %s was disconnected", IdString());
		}
		if (open_) {
			return;
		}
		redialing = gen_ > 0;
		if (redialing && !opts_.can_redial) {
			throw IOException("n6k: connection %s (%s) closed: %s", IdString(), opts_.endpoint, drop_reason_);
		}
		old = std::move(transport_);
	}
	if (old) {
		old->Stop();
	}

	if (redialing && opts_.resolve_token) {
		token_ = opts_.resolve_token();
	}
	std::shared_ptr<Transport> transport(opts_.dial());
	uint64_t gen;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (disconnected_) {
			throw IOException("n6k: connection %s was disconnected", IdString());
		}
		gen = ++gen_;
		opened_ = false;
		dropped_ = false;
		drop_reason_.clear();
		hello_arrived_ = false;
		protocol_version_ = 0;
		hello_error_.clear();
		hello_pending_beats_ = 0;
		hello_pending_message_.clear();
	}
	try {
		// Raw `this`: every path that drops the hub stops this transport first, which joins its callbacks.
		transport->Start([this, gen](const std::string &bytes, bool binary) { OnMessage(gen, bytes, binary); },
		                 [this, gen](const std::string &reason) { OnTransportClose(gen, reason); },
		                 [this, gen]() { OnOpen(gen); });
		// Published only once started, so Disconnect never stops a transport before its Start.
		{
			std::lock_guard<std::mutex> lk(mu_);
			if (gen_ == gen) {
				transport_ = transport;
			}
		}
		AwaitHello(gen, should_abort);
	} catch (...) {
		{
			std::lock_guard<std::mutex> lk(mu_);
			if (gen_ == gen) {
				dropped_ = true;
				transport_.reset();
			}
		}
		transport->Stop();
		throw;
	}
}

void ConnectionHub::Reconnect(const AbortCheck &should_abort) {
	if (!opts_.can_redial) {
		throw InvalidInputException("n6k: connection %s (%s) cannot be redialed; open a new one with n6k_connect()",
		                            IdString(), opts_.endpoint);
	}
	const std::string reason = "connection " + IdString() + " reconnecting";
	std::vector<SubscriberPtr> targets;
	std::shared_ptr<Transport> old;
	{
		std::lock_guard<std::mutex> connect_lk(connect_mu_);
		{
			std::lock_guard<std::mutex> lk(mu_);
			if (disconnected_) {
				throw IOException("n6k: connection %s was disconnected", IdString());
			}
			if (open_) {
				open_ = false;
				dropped_ = true;
				drop_reason_ = reason;
				// OnTransportClose from the old socket is now ignored, so sessions are closed once, below.
				gen_++;
				old = std::move(transport_);
				for (auto &entry : subs_) {
					HoldLocked(targets, entry.second);
				}
				for (auto &entry : requests_) {
					HoldLocked(targets, entry.second);
				}
			}
		}
		cv_.notify_all();
		if (old) {
			old->Stop();
		}
	}
	Dispatch(targets, [&](Subscriber &sub) {
		if (sub.on_close) {
			sub.on_close(reason);
		}
	});
	EnsureConnected(should_abort);
}

void ConnectionHub::AwaitHello(uint64_t gen, const AbortCheck &should_abort) {
	using Clock = std::chrono::steady_clock;
	auto deadline = Clock::now() + opts_.connect_timeout;
	const auto wait_for = [&](const std::function<bool()> &done) {
		std::unique_lock<std::mutex> lk(mu_);
		uint64_t beats = hello_pending_beats_;
		for (;;) {
			if (done()) {
				return;
			}
			if (dropped_) {
				throw IOException("n6k: %s: %s", opts_.endpoint, drop_reason_);
			}
			if (hello_pending_beats_ != beats) {
				beats = hello_pending_beats_;
				deadline = Clock::now() + opts_.hello_pending_stall;
			}
			const auto now = Clock::now();
			if (now >= deadline) {
				if (beats == 0) {
					throw IOException("n6k: %s: no reply within %llu ms", opts_.endpoint,
					                  static_cast<uint64_t>(opts_.connect_timeout.count()));
				}
				throw IOException("n6k: %s: handshake stalled: no FT_HELLO_PENDING for %llu ms (last status: %s)",
				                  opts_.endpoint, static_cast<uint64_t>(opts_.hello_pending_stall.count()),
				                  hello_pending_message_);
			}
			lk.unlock();
			if (should_abort && should_abort()) {
				throw InterruptException();
			}
			lk.lock();
			cv_.wait_until(lk, std::min(deadline, Clock::now() + ABORT_POLL));
		}
	};

	wait_for([&] { return opened_; });
	if (opts_.host_handshake) {
		auto capabilities = opts_.host_handshake();
		std::lock_guard<std::mutex> lk(mu_);
		if (gen_ != gen || dropped_) {
			throw IOException("n6k: %s: %s", opts_.endpoint, drop_reason_);
		}
		capabilities_ = std::move(capabilities);
		open_ = true;
		return;
	}
	const auto hello = opts_.offer_app_capabilities ? PackHello(static_cast<int>(FrameType::HELLO), token_, HELLO_CAPS)
	                                                : PackHello(static_cast<int>(FrameType::HELLO), token_,
	                                                            std::array<const char *, 1> {{CAP_CROSSING}});
	Enqueue(gen, /*binary=*/true, hello);
	LogFrame("send", hello);
	wait_for([&] { return hello_arrived_; });

	std::lock_guard<std::mutex> lk(mu_);
	if (!hello_error_.empty()) {
		auto message = hello_error_;
		if (hello_pending_beats_ > 0) {
			message += " (handshake was pending: " + hello_pending_message_ + ")";
		}
		throw IOException("n6k: %s: %s", opts_.endpoint, message);
	}
	if (protocol_version_ != N6K_PROTOCOL_VERSION) {
		throw IOException("n6k: %s: server protocol_version=%d, expected %d", opts_.endpoint, protocol_version_,
		                  static_cast<int>(N6K_PROTOCOL_VERSION));
	}
	if (gen_ != gen || dropped_) {
		throw IOException("n6k: %s: %s", opts_.endpoint, drop_reason_);
	}
	open_ = true;
}

void ConnectionHub::LogFrame(const char *direction, const std::string &frame) {
	if (opts_.logger) {
		DUCKDB_LOG(opts_.logger, WsLogType, direction, frame);
	}
}

void ConnectionHub::OnOpen(uint64_t gen) {
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (gen != gen_) {
			return;
		}
		opened_ = true;
	}
	cv_.notify_all();
}

void ConnectionHub::OnMessage(uint64_t gen, const std::string &bytes, bool binary) {
	if (!binary) {
		if (!opts_.on_text.empty()) {
			PostHostEvent(false, bytes);
			return;
		}
		auto reply = JsonRpcMethodNotFoundReply(bytes);
		if (!reply.empty()) {
			SendText(reply);
		}
		return;
	}
	// A frame we cannot parse is treated as untagged, so Route drops it: it names no session or request.
	int64_t ns = 0;
	int64_t type = -1;
	int64_t req_id = -1;
	bool has_id = false;
	msgpack::object_handle oh;
	try {
		size_t off = 0;
		oh = msgpack::unpack(bytes.data(), bytes.size(), off);
		if (oh.get().type == msgpack::type::MAP) {
			ns = GetInt(oh.get(), "ns", 0);
			type = GetInt(oh.get(), "t", -1);
			req_id = GetInt(oh.get(), "id", -1);
			has_id = HasKey(oh.get(), "id");
		}
	} catch (...) {
		ns = 0;
		type = -1;
	}

	if (ns == 0) {
		const auto t = static_cast<FrameType>(type);
		if (t == FrameType::PING) {
			LogFrame("recv", bytes);
			const auto pong = PackPong(static_cast<int>(FrameType::PONG), has_id, static_cast<uint32_t>(req_id));
			Enqueue(gen, /*binary=*/true, pong);
			LogFrame("send", pong);
			return;
		}
		if (t == FrameType::HELLO_ACK || t == FrameType::HELLO_PENDING || t == FrameType::HELLO_ERR) {
			std::unique_lock<std::mutex> lk(mu_);
			if (gen == gen_ && !hello_arrived_) {
				LogFrame("recv", bytes);
				if (t == FrameType::HELLO_ACK) {
					protocol_version_ = static_cast<int>(GetInt(oh.get(), "protocol_version", 0));
					capabilities_ = GetStrArray(oh.get(), "capabilities");
					hello_arrived_ = true;
				} else if (t == FrameType::HELLO_PENDING) {
					hello_pending_beats_++;
					hello_pending_message_ = GetStr(oh.get(), "message");
				} else {
					const auto etype = GetStr(oh.get(), "exception_type");
					const auto emsg = GetStr(oh.get(), "exception_message");
					hello_error_ = etype.empty() ? emsg : etype + ": " + emsg;
					hello_arrived_ = true;
				}
				lk.unlock();
				cv_.notify_all();
				return;
			}
		}
	}
	Route(bytes, ns, type, req_id);
}

void ConnectionHub::Route(const std::string &frame, int64_t ns, int64_t type, int64_t req_id) {
	// Only a reply completes a request; another untagged frame may carry an `id` too.
	const bool is_response = type == static_cast<int64_t>(FrameType::RESP_CHUNK) ||
	                         type == static_cast<int64_t>(FrameType::RESP_END) ||
	                         type == static_cast<int64_t>(FrameType::ERR);
	// Called without the lock: a callback runs session code that may bind or unbind.
	std::vector<SubscriberPtr> targets;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (ns == 0) {
			// Untagged and unclaimed (a late reply to an unbound request, a connection-scoped PUSH): no
			// session's request ids are in this space, so handing it to one could complete the wrong request.
			auto request =
			    (is_response && req_id >= 0) ? requests_.find(static_cast<uint32_t>(req_id)) : requests_.end();
			if (request != requests_.end()) {
				HoldLocked(targets, request->second);
			}
		} else {
			auto it = subs_.find(static_cast<uint64_t>(ns));
			if (it != subs_.end()) {
				HoldLocked(targets, it->second);
			}
		}
	}
	Dispatch(targets, [&](Subscriber &sub) { sub.on_message(frame, /*binary=*/true); });
}

void ConnectionHub::OnTransportClose(uint64_t gen, const std::string &reason) {
	std::vector<SubscriberPtr> targets;
	bool was_open;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (gen != gen_ || dropped_) {
			return;
		}
		was_open = open_;
		dropped_ = true;
		open_ = false;
		drop_reason_ = reason.empty() ? "socket closed" : reason;
		for (auto &entry : subs_) {
			HoldLocked(targets, entry.second);
		}
		for (auto &entry : requests_) {
			HoldLocked(targets, entry.second);
		}
	}
	cv_.notify_all();
	Dispatch(targets, [&](Subscriber &sub) {
		if (sub.on_close) {
			sub.on_close(reason);
		}
	});
	if (was_open && !opts_.on_close.empty()) {
		PostHostEvent(true, reason);
	}
}

uint64_t ConnectionHub::BindSession(uint64_t ns, MessageFn on_message, CloseFn on_close) {
	auto sub = std::make_shared<Subscriber>();
	sub->on_message = std::move(on_message);
	sub->on_close = std::move(on_close);
	std::string down;
	uint64_t gen;
	std::vector<SubscriberPtr> held;
	{
		std::lock_guard<std::mutex> lk(mu_);
		gen = gen_;
		subs_[ns] = sub;
		if (!open_) {
			down = disconnected_ ? "connection " + IdString() + " was disconnected" : drop_reason_;
			if (down.empty()) {
				down = "connection " + IdString() + " is not open";
			}
			HoldLocked(held, sub);
		}
	}
	Dispatch(held, [&](Subscriber &s) {
		if (s.on_close) {
			s.on_close(down);
		}
	});
	return gen;
}

void ConnectionHub::UnbindSession(uint64_t ns) {
	std::unique_lock<std::mutex> lk(mu_);
	auto it = subs_.find(ns);
	if (it == subs_.end()) {
		return;
	}
	auto sub = std::move(it->second);
	subs_.erase(it);
	idle_cv_.wait(lk, [&] { return sub->in_flight == 0; });
}

uint32_t ConnectionHub::BindRequest(MessageFn on_message, CloseFn on_close) {
	auto sub = std::make_shared<Subscriber>();
	sub->on_message = std::move(on_message);
	sub->on_close = std::move(on_close);
	uint32_t req_id;
	std::vector<SubscriberPtr> held;
	{
		std::lock_guard<std::mutex> lk(mu_);
		req_id = next_req_id_++;
		requests_[req_id] = sub;
		if (!open_) {
			HoldLocked(held, sub);
		}
	}
	const std::string down = "connection " + IdString() + " is not open";
	Dispatch(held, [&](Subscriber &s) {
		if (s.on_close) {
			s.on_close(down);
		}
	});
	return req_id;
}

void ConnectionHub::UnbindRequest(uint32_t req_id) {
	std::lock_guard<std::mutex> lk(mu_);
	requests_.erase(req_id);
}

bool ConnectionHub::SendOnGeneration(uint64_t generation, const std::string &frame) {
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (!open_ || generation != gen_) {
			return false;
		}
	}
	Enqueue(generation, /*binary=*/true, frame);
	return true;
}

bool ConnectionHub::Send(const std::string &frame) {
	return SendOnGeneration(Generation(), frame);
}

bool ConnectionHub::SendText(const std::string &text) {
	uint64_t gen;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (!open_) {
			return false;
		}
		gen = gen_;
	}
	Enqueue(gen, /*binary=*/false, text);
	return true;
}

void ConnectionHub::Disconnect(const std::string &reason) {
	std::vector<SubscriberPtr> targets;
	std::shared_ptr<Transport> transport;
	{
		std::lock_guard<std::mutex> lk(mu_);
		if (disconnected_) {
			return;
		}
		disconnected_ = true;
		const bool was_up = open_;
		open_ = false;
		dropped_ = true;
		drop_reason_ = reason;
		// A dial in flight sees the bumped generation and unwinds through its own Stop.
		gen_++;
		transport = std::move(transport_);
		if (was_up) {
			for (auto &entry : subs_) {
				HoldLocked(targets, entry.second);
			}
			for (auto &entry : requests_) {
				HoldLocked(targets, entry.second);
			}
		}
	}
	cv_.notify_all();
	if (transport) {
		transport->Stop();
	}
	Dispatch(targets, [&](Subscriber &sub) {
		if (sub.on_close) {
			sub.on_close(reason);
		}
	});
	StopHostWorker();
}

void ConnectionHub::PostHostEvent(bool is_close, std::string payload) {
	std::lock_guard<std::mutex> lk(mu_);
	if (disconnected_) {
		return;
	}
	if (!host_queue_) {
		host_queue_ = std::make_shared<HostQueue>();
		host_worker_ =
		    std::thread(&ConnectionHub::HostWorkerLoop, host_queue_, self_, db_, id_, opts_.on_text, opts_.on_close);
	}
	{
		std::lock_guard<std::mutex> qlk(host_queue_->mu);
		host_queue_->events.push_back(HostEvent {is_close, std::move(payload)});
	}
	host_queue_->cv.notify_one();
}

void ConnectionHub::StopHostWorker() {
	std::shared_ptr<HostQueue> queue;
	{
		std::lock_guard<std::mutex> lk(mu_);
		queue = host_queue_;
	}
	if (!queue) {
		return;
	}
	{
		std::lock_guard<std::mutex> qlk(queue->mu);
		queue->stop = true;
	}
	queue->cv.notify_all();
	// Not joined: a host UDF in flight may wait on a lock this thread holds (Python's GIL during close),
	// or this may be the worker itself dropping the last reference. It touches only what it owns.
	if (host_worker_.joinable()) {
		host_worker_.detach();
	}
}

void ConnectionHub::HostWorkerLoop(std::shared_ptr<HostQueue> queue, const std::weak_ptr<ConnectionHub> &hub,
                                   const weak_ptr<DatabaseInstance> &db, hugeint_t id, const std::string &on_text,
                                   const std::string &on_close) {
	for (;;) {
		HostEvent event;
		{
			std::unique_lock<std::mutex> lk(queue->mu);
			queue->cv.wait(lk, [&] { return queue->stop || !queue->events.empty(); });
			// Stop ends nothing already received: a drop's on_close may be queued right before a disconnect.
			if (queue->events.empty()) {
				return;
			}
			event = std::move(queue->events.front());
			queue->events.pop_front();
		}
		// A fresh Connection per event: one held across events would keep the database open forever.
		auto database = db.lock();
		if (!database) {
			return;
		}
		try {
			HostCallback callback(*database, event.is_close ? on_close : on_text, 2);
			auto reply = callback.Call({Value::UUID(id), Value(std::move(event.payload))});
			if (!event.is_close && !reply.IsNull()) {
				auto message = reply.GetValue<string>();
				if (!message.empty()) {
					if (auto owner = hub.lock()) {
						owner->SendText(message);
					}
				}
			}
		} catch (const std::exception &e) {
			DUCKDB_LOG_WARNING(*database, "%s", ErrorData(e).Message());
		}
	}
}

shared_ptr<ConnectionRegistry> ConnectionRegistry::Get(DatabaseInstance &db) {
	return db.GetObjectCache().GetOrCreate<ConnectionRegistry>(ObjectType());
}

ConnectionRegistry::Acquired ConnectionRegistry::Acquire(const std::string &key,
                                                         const std::function<std::shared_ptr<ConnectionHub>()> &make) {
	std::lock_guard<std::mutex> lk(mu_);
	for (auto &entry : hubs_) {
		if (entry.second.key == key && !entry.second.hub->IsDead()) {
			entry.second.leases++;
			return Acquired {entry.second.hub, false};
		}
	}
	auto hub = make();
	hubs_[hub->Id()] = Entry {hub, key, 1, 0};
	return Acquired {hub, true};
}

std::shared_ptr<ConnectionHub> ConnectionRegistry::AcquireById(const hugeint_t &id) {
	std::lock_guard<std::mutex> lk(mu_);
	auto it = hubs_.find(id);
	if (it == hubs_.end() || it->second.hub->IsDead()) {
		return nullptr;
	}
	it->second.leases++;
	return it->second.hub;
}

bool ConnectionRegistry::Release(const hugeint_t &id, const std::string &reason) {
	std::shared_ptr<ConnectionHub> last;
	{
		std::lock_guard<std::mutex> lk(mu_);
		auto it = hubs_.find(id);
		if (it == hubs_.end()) {
			return false;
		}
		if (--it->second.leases > 0) {
			return true;
		}
		last = std::move(it->second.hub);
		hubs_.erase(it);
	}
	last->Disconnect(reason);
	return true;
}

void ConnectionRegistry::AddConnectLease(const hugeint_t &id) {
	std::lock_guard<std::mutex> lk(mu_);
	auto it = hubs_.find(id);
	if (it != hubs_.end()) {
		it->second.connect_leases++;
	}
}

bool ConnectionRegistry::ReleaseConnectLease(const hugeint_t &id, const std::string &reason) {
	{
		std::lock_guard<std::mutex> lk(mu_);
		auto it = hubs_.find(id);
		if (it == hubs_.end()) {
			return false;
		}
		if (it->second.connect_leases == 0) {
			throw InvalidInputException("n6k: connection %s has no n6k_connect lease left", UUID::ToString(id));
		}
		it->second.connect_leases--;
	}
	return Release(id, reason);
}

std::shared_ptr<ConnectionHub> ConnectionRegistry::Find(const hugeint_t &id) {
	std::lock_guard<std::mutex> lk(mu_);
	auto it = hubs_.find(id);
	return it == hubs_.end() ? nullptr : it->second.hub;
}

std::vector<ConnectionRegistry::Listed> ConnectionRegistry::List() {
	std::lock_guard<std::mutex> lk(mu_);
	std::vector<Listed> out;
	out.reserve(hubs_.size());
	for (auto &entry : hubs_) {
		out.push_back(Listed {entry.second.hub, entry.second.leases});
	}
	return out;
}

ConnectionLease::ConnectionLease(shared_ptr<ConnectionRegistry> registry, std::shared_ptr<ConnectionHub> hub)
    : registry_(std::move(registry)), hub_(std::move(hub)) {
}

ConnectionLease::~ConnectionLease() {
	Release();
}

void ConnectionLease::Release() {
	if (!released_.exchange(true)) {
		registry_->Release(hub_->Id(), "connection " + hub_->IdString() + " released");
	}
}

std::string WebSocketKey(const std::string &ws_url, const std::string &token) {
	return "ws\n" + ws_url + "\n" + token;
}

std::string WsUrlFromBaseUrl(const std::string &base_url, const std::string &query) {
	if (StringUtil::StartsWith(base_url, "https://")) {
		return "wss://" + base_url.substr(8) + "/ws" + query;
	}
	if (StringUtil::StartsWith(base_url, "http://")) {
		return "ws://" + base_url.substr(7) + "/ws" + query;
	}
	throw IOException("n6k: base URL must be http(s)://, got %s", base_url);
}

#ifndef WASM_LOADABLE_EXTENSIONS
ConnectionHub::Options WebSocketOptions(const std::string &ws_url, const std::string &token) {
	ConnectionHub::Options opts;
	opts.dial = [ws_url]() {
		return std::unique_ptr<Transport>(new IxWebSocketTransport(ws_url));
	};
	opts.can_redial = true;
	opts.token = token;
	opts.endpoint = ws_url;
	return opts;
}
#endif

hugeint_t ParseConnectionId(const std::string &text, const char *who) {
	hugeint_t id;
	if (!UUID::FromString(text, id)) {
		throw InvalidInputException("%s: '%s' is not a connection id", who, text);
	}
	return id;
}

} // namespace n6k
} // namespace duckdb
