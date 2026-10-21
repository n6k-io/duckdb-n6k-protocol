#pragma once

#ifndef WASM_LOADABLE_EXTENSIONS
#ifndef _WIN32

#include "ws_fd_hub.hpp"
#include "ws_transport.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace duckdb {
namespace n6k {

// Transport over an already-connected socket fd; stream-framed (see Stream framing in
// packages/python/src/n6k_protocol/protocol.py).
// The fd itself is owned by an FdHub shared with every other session on the same fd — this class is
// just this session's view of it, bound to the `ns` its inbound frames are routed by.
class FdTransport : public Transport {
public:
	// ns == 0 for a session that is the sole rider of this fd (and for pre-mux callers).
	explicit FdTransport(int fd, uint64_t ns = 0);
	~FdTransport() override;

	void Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) override;
	bool Send(const std::string &frame) override;
	void Stop() override;

private:
	std::shared_ptr<FdHub> hub_;
	uint64_t ns_;
	std::atomic<bool> stopping_ {false};
};

} // namespace n6k
} // namespace duckdb

#endif // !_WIN32
#endif // !WASM_LOADABLE_EXTENSIONS
