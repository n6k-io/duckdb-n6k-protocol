#ifndef WASM_LOADABLE_EXTENSIONS
#ifndef _WIN32

#include "ws_transport_fd.hpp"

namespace duckdb {
namespace n6k {

FdTransport::FdTransport(int fd, uint64_t ns) : hub_(FdHub::Acquire(fd)), ns_(ns) {
}

FdTransport::~FdTransport() {
	Stop();
}

void FdTransport::Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) {
	hub_->BindSessionAndStartReader(ns_, std::move(on_message), std::move(on_close));
	// The fd arrives already connected, so there is no handshake to wait on: writable immediately.
	if (on_open) {
		on_open();
	}
}

bool FdTransport::Send(const std::string &frame) {
	// Must not throw (runs on the writer thread). Returns false on a hard write error; the hub's reader
	// surfaces the same drop as a close, so the two paths agree.
	return hub_->SendLengthPrefixed(StreamFrameType::N6K, frame);
}

void FdTransport::Stop() {
	bool expected = false;
	if (!stopping_.compare_exchange_strong(expected, true)) {
		return;
	}
	hub_->Unsubscribe(ns_);
	// Dropping our reference closes the fd only once the LAST session on it is gone.
	hub_.reset();
}

} // namespace n6k
} // namespace duckdb

#endif // !_WIN32
#endif // !WASM_LOADABLE_EXTENSIONS
