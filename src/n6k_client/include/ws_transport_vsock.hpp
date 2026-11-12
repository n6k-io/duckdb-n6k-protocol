#pragma once

#ifdef WASM_LOADABLE_EXTENSIONS

#include "n6k_io_thread.hpp"
#include "n6k_vsock_ring.hpp"
#include "ws_transport.hpp"

#include <atomic>
#include <cstdint>
#include <string>

namespace duckdb {
namespace n6k {

// A connection's bytes over two vsock rings in the shared wasm heap, moved to and from the WebSocket by a JS
// pump: the pump thread for a dialed URL, the page's pump worker for a registered socket. One pair per Start: a redial
// gets fresh rings and a fresh pump. Shutdown is the default no-op: the pump's socket stays open until Stop().
class VsockTransport : public Transport {
public:
	// `connection_id`: the ConnectionHub it carries, so JS can tell which catalogs ride it.
	// `ws_url` dials that URL; otherwise the pump bridges the socket registered under `connection_id`.
	VsockTransport(std::string connection_id, std::string ws_url);
	~VsockTransport() override;

	void Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) override;
	bool Send(const std::string &frame) override;
	bool SendText(const std::string &text) override;
	void Stop() override;

private:
	void StartDial(void *ring, const std::string &args, const std::string &pointers);
	[[noreturn]] void FailStart(void *ring, int32_t *status, const std::string &reason);

	std::string key_;
	std::string connection_id_;
	std::string ws_url_;
	VsockRing ring0_; // duckdb->ws; the shared I/O thread is its sole producer while registered
	VsockRing ring1_; // ws->duckdb; the shared I/O thread is its sole consumer while registered
	N6kIoThread::Handle handle_ = 0;
	bool attached_ = false;
	std::atomic<bool> stopped_ {false};
};

} // namespace n6k
} // namespace duckdb

#endif // WASM_LOADABLE_EXTENSIONS
