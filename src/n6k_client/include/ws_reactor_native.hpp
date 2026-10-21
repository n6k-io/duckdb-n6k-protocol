#pragma once

#ifndef WASM_LOADABLE_EXTENSIONS

#include "ws_client.hpp"
#include "ws_reactor.hpp"
#include "ws_transport.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

namespace duckdb {
namespace n6k {

class NativeReactor : public Reactor {
public:
	explicit NativeReactor(WsClientOptions opts);
	~NativeReactor() override;

	void Start(FrameFn on_frame, CloseFn on_close) override;
	void Send(const std::string &frame) override;
	bool WaitUntil(const std::function<bool()> &ready, std::chrono::steady_clock::time_point deadline) override;
	void Stop() override;

private:
	void NotifyProgress();
	void StartWriterThread();

	WsClientOptions opts_;
	std::unique_ptr<Transport> transport_;
	std::atomic<bool> stopping_ {false};

	// The transport reported its endpoint writable (ws 101 received / fd handed over). The writer thread
	// parks until this is set: ixwebsocket discards Send before Open.
	std::atomic<bool> writable_ {false};
	// The transport dropped before, or instead of, becoming writable — releases a parked writer so it
	// exits rather than waiting out a connection that will never open.
	std::atomic<bool> transport_closed_ {false};
	// Wrapped on_close, retained so the writer thread can report a refused Send as a connection error.
	CloseFn close_cb_;

	// A WaitUntil parks here; notified after each dispatched inbound frame.
	std::mutex progress_mu_;
	std::condition_variable progress_cv_;

	std::mutex send_mu_;
	std::condition_variable send_cv_;
	std::deque<std::string> send_q_;
	std::thread writer_;
};

} // namespace n6k
} // namespace duckdb

#endif // !WASM_LOADABLE_EXTENSIONS
