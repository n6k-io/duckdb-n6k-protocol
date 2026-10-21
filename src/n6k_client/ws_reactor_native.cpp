#ifndef WASM_LOADABLE_EXTENSIONS

#include "ws_reactor_native.hpp"
#include "ws_transport_fd.hpp"
#include "ws_transport_ix.hpp"

#include <stdexcept>

namespace duckdb {
namespace n6k {

std::unique_ptr<Reactor> CreateReactor(const WsClientOptions &opts) {
	return std::unique_ptr<Reactor>(new NativeReactor(opts));
}

NativeReactor::NativeReactor(WsClientOptions opts) : opts_(std::move(opts)) {
}

NativeReactor::~NativeReactor() {
	Stop();
}

void NativeReactor::NotifyProgress() {
	std::lock_guard<std::mutex> lk(progress_mu_);
	progress_cv_.notify_all();
}

void NativeReactor::StartWriterThread() {
	writer_ = std::thread([this]() {
		{
			std::unique_lock<std::mutex> lk(send_mu_);
			send_cv_.wait(lk, [&] { return stopping_.load() || writable_.load() || transport_closed_.load(); });
		}
		if (!writable_.load()) {
			// Woken by teardown or a connection that dropped before opening: nothing can be sent.
			return;
		}

		for (;;) {
			std::string frame;
			{
				std::unique_lock<std::mutex> lk(send_mu_);
				send_cv_.wait(lk, [&] { return stopping_.load() || !send_q_.empty(); });
				if (stopping_.load() && send_q_.empty()) {
					return;
				}
				frame = std::move(send_q_.front());
				send_q_.pop_front();
			}
			if (transport_ && !transport_->Send(frame)) {
				// Never swallow a refusal: the frame is gone and the peer will wait for it forever.
				// Report it once as a connection error so in-flight requests fail instead of hanging.
				if (!transport_closed_.exchange(true) && close_cb_) {
					close_cb_("n6k: outbound frame refused by transport (socket not open)");
				}
				return;
			}
		}
	});
}

void NativeReactor::Start(FrameFn on_frame, CloseFn on_close) {
	if (opts_.ws_fd >= 0) {
#ifdef _WIN32
		// Backstop: FdTransport is POSIX-only; wsFd on Windows is rejected at ATTACH.
		throw std::runtime_error("n6k: wsFd is not supported on Windows");
#else
		// ns binds this session's inbound frames on a socket the host may share with other ATTACHes.
		transport_.reset(new FdTransport(opts_.ws_fd, opts_.ns));
#endif
	} else {
		transport_.reset(new IxWebSocketTransport(opts_.url));
	}

	close_cb_ = [this, on_close](const std::string &reason) {
		on_close(reason);
		NotifyProgress();
	};

	StartWriterThread();

	// Wrap the client callbacks so a WaitUntil re-checks after each dispatch (NotifyProgress takes only progress_mu_).
	transport_->Start(
	    [this, on_frame](const std::string &bytes, bool binary) {
		    on_frame(bytes, binary);
		    NotifyProgress();
	    },
	    [this](const std::string &reason) {
		    // Release a writer still parked on the open gate before dispatching, so teardown can't wait on it.
		    {
			    std::lock_guard<std::mutex> lk(send_mu_);
			    transport_closed_.store(true);
		    }
		    send_cv_.notify_all();
		    if (close_cb_) {
			    close_cb_(reason);
		    }
	    },
	    [this]() {
		    // Endpoint is writable: release the writer thread to drain whatever Connect already queued.
		    {
			    std::lock_guard<std::mutex> lk(send_mu_);
			    writable_.store(true);
		    }
		    send_cv_.notify_all();
	    });
}

void NativeReactor::Send(const std::string &frame) {
	{
		std::lock_guard<std::mutex> lk(send_mu_);
		send_q_.push_back(frame);
	}
	send_cv_.notify_one();
}

bool NativeReactor::WaitUntil(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) {
	std::unique_lock<std::mutex> lk(progress_mu_);
	// `ready` self-locks client state; safe under progress_mu_ since the inbound path never holds it while locking
	// client state.
	progress_cv_.wait_until(lk, deadline, [&] { return ready() || stopping_.load(); });
	return ready();
}

void NativeReactor::Stop() {
	bool expected = false;
	if (!stopping_.compare_exchange_strong(expected, true)) {
		return;
	}
	// Wake the writer and any WaitUntil so they observe `stopping_` and exit. Taking send_mu_ before
	// notifying closes a lost-wakeup window: without it the writer can evaluate its predicate (false),
	// miss the notify, and park forever on the open gate — which the join below would then wait on.
	{ std::lock_guard<std::mutex> lk(send_mu_); }
	send_cv_.notify_all();
	NotifyProgress();
	if (writer_.joinable()) {
		writer_.join();
	}
	if (transport_) {
		transport_->Stop();
		transport_.reset();
	}
}

} // namespace n6k
} // namespace duckdb

#endif // !WASM_LOADABLE_EXTENSIONS
