#include "n6k_io_thread.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS

#include <exception>
#include <vector>

namespace duckdb {
namespace n6k {

N6kIoThread &N6kIoThread::Instance() {
	// Function-local static: thread-safe lazy init (first attach) even if two compute pthreads race
	// here; destroyed at static destruction, which stops + joins the thread.
	static N6kIoThread instance;
	return instance;
}

N6kIoThread::N6kIoThread() {
	thread_ = std::thread([this] { Loop(); });
}

N6kIoThread::~N6kIoThread() {
	{
		std::lock_guard<std::mutex> lk(mu_);
		stop_ = true;
	}
	BumpDoorbell();
	if (thread_.joinable()) {
		thread_.join();
	}
}

void N6kIoThread::BumpDoorbell() {
	// Advance the word so a consumer sampling it observes a change, then wake a parked waiter. The
	// wasm futex notify shares the engine wait queue with JS Atomics.notify, so a pump's JS notify
	// and this C++ notify both wake the I/O thread.
	__atomic_add_fetch(&doorbell_, 1, __ATOMIC_RELEASE);
	__builtin_wasm_memory_atomic_notify(&doorbell_, static_cast<int32_t>(-1));
}

N6kIoThread::Handle N6kIoThread::Register(Registration reg) {
	Handle h;
	{
		std::lock_guard<std::mutex> lk(mu_);
		h = next_handle_++;
		auto ch = std::make_shared<Channel>();
		ch->reg = std::move(reg);
		channels_[h] = std::move(ch);
	}
	// Wake the I/O thread to pick up the new channel and drain any frames the pump delivered before
	// the reactor finished registering (e.g. a fast handshake reply).
	BumpDoorbell();
	return h;
}

void N6kIoThread::Enqueue(Handle h, const std::string &frame) {
	std::shared_ptr<Channel> ch;
	{
		std::lock_guard<std::mutex> lk(mu_);
		auto it = channels_.find(h);
		if (it == channels_.end()) {
			return; // channel gone (torn down / not yet or no longer registered)
		}
		ch = it->second;
	}
	{
		std::lock_guard<std::mutex> lk(ch->out_mu);
		ch->outbound.push_back(frame);
	}
	// Wake the I/O thread to TryWrite it; it is ring0's sole producer, so the requester never blocks.
	BumpDoorbell();
}

void N6kIoThread::UnregisterAndAwaitQuiesce(Handle h) {
	std::unique_lock<std::mutex> lk(mu_);
	auto it = channels_.find(h);
	if (it == channels_.end()) {
		return; // already gone (never registered, or double-unregistered)
	}
	auto ch = it->second; // strong ref keeps Channel alive across the wait even if the map rehashes
	ch->retiring = true;
	// Kick the I/O thread so it processes the retirement promptly, then wait for its quiesce ack: it
	// sets quiesced only once it has re-snapshotted WITHOUT this channel, so on return the I/O thread
	// will never touch ring1 again and the caller may Close + free it.
	BumpDoorbell();
	quiesce_cv_.wait(lk, [&] { return ch->quiesced; });
	channels_.erase(h); // re-lookup by key (do not reuse `it`; the map may have rehashed)
}

void N6kIoThread::RetireChannelWithClose(Channel &ch, const char *reason) {
	// Retire the channel from draining and surface the close to the reactor exactly once. Left in the
	// registry (defunct) until the reactor Unregisters — the reactor owns the ring's lifetime.
	ch.defunct = true;
	if (ch.close_fired) {
		return;
	}
	ch.close_fired = true;
	if (ch.reg.on_close) {
		ch.reg.on_close(reason);
	}
	if (ch.reg.on_progress) {
		ch.reg.on_progress();
	}
}

bool N6kIoThread::DrainOutbound(Channel &ch) {
	if (ch.defunct) {
		std::lock_guard<std::mutex> lk(ch.out_mu);
		ch.outbound.clear(); // closed/reset: the backlog will never reach the peer
		return false;
	}
	std::deque<std::string> pending;
	{
		std::lock_guard<std::mutex> lk(ch.out_mu);
		pending.swap(ch.outbound);
	}
	bool wrote = false;
	while (!pending.empty()) {
		const std::string &f = pending.front();
		if (!ch.reg.ring0.TryWrite(reinterpret_cast<const uint8_t *>(f.data()), static_cast<uint32_t>(f.size()))) {
			break; // ring0 full; re-queue the unsent tail and retry on the next (bounded) wake
		}
		pending.pop_front();
		wrote = true;
	}
	if (!pending.empty()) {
		// Prepend the unsent frames (order-preserving) ahead of any newly enqueued ones.
		std::lock_guard<std::mutex> lk(ch.out_mu);
		for (auto it = pending.rbegin(); it != pending.rend(); ++it) {
			ch.outbound.push_front(std::move(*it));
		}
	}
	return wrote;
}

void N6kIoThread::Loop() {
	std::vector<uint8_t> buf(65536);
	for (;;) {
		// Sample the doorbell BEFORE draining so a bump landing in the drain->park gap makes the park
		// return not-equal immediately (lost-wakeup safe).
		const int32_t seen = __atomic_load_n(&doorbell_, __ATOMIC_ACQUIRE);

		std::vector<std::shared_ptr<Channel>> snapshot;
		bool acked = false;
		{
			std::lock_guard<std::mutex> lk(mu_);
			if (stop_) {
				return;
			}
			for (auto &kv : channels_) {
				auto &ch = kv.second;
				if (ch->retiring) {
					// Excluded from this snapshot => provably untouched hereafter; ack the quiesce.
					ch->quiesced = true;
					acked = true;
					continue;
				}
				if (ch->defunct) {
					continue; // closed/reset; awaiting the reactor's Unregister
				}
				snapshot.push_back(ch);
			}
		}
		if (acked) {
			quiesce_cv_.notify_all();
		}

		bool progress = false;
		for (auto &ch : snapshot) {
			if (ch->reg.ring1.Epoch() != ch->reg.pinned_epoch) {
				RetireChannelWithClose(*ch, "channel reset"); // host swapped the channel; this generation retires
				continue;
			}
			// RX first, then TX, so a PONG queued from an inbound PING goes out on this same pass.
			// try/catch protects the shared thread: a per-channel fault (e.g. TryWrite rejecting an
			// oversized frame) retires that one channel instead of tearing down every catalog's I/O.
			try {
				for (;;) {
					int64_t n = ch->reg.ring1.TryRead(buf.data(), static_cast<uint32_t>(buf.size()));
					if (n == -1) {
						break; // ring empty, still open
					}
					if (n == -3) {
						RetireChannelWithClose(*ch, "socket closed");
						break;
					}
					if (n < -1) {
						buf.resize(static_cast<size_t>(-n)); // frame > buffer; grow to exact size, retry
						continue;
					}
					std::string frame(reinterpret_cast<char *>(buf.data()), static_cast<size_t>(n));
					if (ch->reg.on_frame) {
						ch->reg.on_frame(frame, true);
					}
					if (ch->reg.on_progress) {
						ch->reg.on_progress();
					}
					progress = true;
				}
				if (DrainOutbound(*ch)) {
					progress = true;
				}
			} catch (const std::exception &e) {
				RetireChannelWithClose(*ch, e.what());
			}
		}

		if (!progress) {
			// Park on the doorbell, bounded so a host epoch reset (ringReset does not notify) and a
			// pending retirement are still noticed within the timeout even if a notify is missed.
			__builtin_wasm_memory_atomic_wait32(&doorbell_, seen, static_cast<int64_t>(250) * 1000000);
		}
	}
}

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
