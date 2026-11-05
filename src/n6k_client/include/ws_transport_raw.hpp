#pragma once

#ifndef WASM_LOADABLE_EXTENSIONS
#ifndef _WIN32

#include "n6k_protocol_generated.hpp"
#include "ws_transport.hpp"

#include <atomic>
#include <mutex>
#include <string>
#include <thread>

namespace duckdb {
namespace n6k {

// An already-connected stream socket carrying n6k stream framing: [u32 BE length][u8 type][payload]
// (Stream framing in packages/python/src/n6k_protocol/protocol.py).
class RawSocketTransport : public Transport {
public:
	// Reads and writes a dup() of `fd`; the caller keeps, and closes, its own.
	explicit RawSocketTransport(int fd);
	~RawSocketTransport() override;
	RawSocketTransport(const RawSocketTransport &) = delete;
	RawSocketTransport &operator=(const RawSocketTransport &) = delete;

	void Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) override;
	bool Send(const std::string &frame) override;
	bool SendText(const std::string &text) override;
	void Stop() override;
	void Shutdown() override;

private:
	bool SendFramed(StreamFrameType type, const std::string &payload);
	void ReaderLoop(const MessageFn &on_message, const CloseFn &on_close);

	int fd_ = -1;
	// Written to so a parked poll() wakes on Stop. shutdown() would also wake it, but would end the
	// connection for the host's end too.
	int wake_r_ = -1;
	int wake_w_ = -1;

	std::mutex write_mu_;
	std::thread reader_;
	std::atomic<bool> stopping_ {false};
};

} // namespace n6k
} // namespace duckdb

#endif // !_WIN32
#endif // !WASM_LOADABLE_EXTENSIONS
