#pragma once

#include "serve_transport.hpp"

#include <cstdint>
#include <string>

namespace duckdb {
namespace n6k {

class UdsConnection : public ServeTransport {
public:
	UdsConnection() = default;
	~UdsConnection() override;

	UdsConnection(const UdsConnection &) = delete;
	UdsConnection &operator=(const UdsConnection &) = delete;

	// Take over an already-connected stream fd (e.g. one end of a host's socketpair). Duplicates it,
	// so the host may close its own copy as soon as this returns.
	void AdoptBlockingDupOfFd(int host_fd);

	// Take ownership of a connected stream fd and make it blocking; closes `fd` and throws if that fails.
	// Closes any fd already held.
	void Adopt(int fd);

	bool WaitReadable(int timeout_ms) override;
	bool ReadFrame(std::string &out, StreamFrameType &type) override;
	FrameRead ReadFrameBy(std::string &out, StreamFrameType &type,
	                      std::chrono::steady_clock::time_point deadline) override;
	void WriteFrame(StreamFrameType type, const std::string &frame) override;
	void ShutdownWrite() override;

	// Safe from another thread while one is blocked reading: it wakes with EOF, as from a peer close.
	void CloseInbound();

	void Close();

private:
	int fd_ = -1;
};

class UdsListener {
public:
	explicit UdsListener(const std::string &path);
	~UdsListener();

	UdsListener(const UdsListener &) = delete;
	UdsListener &operator=(const UdsListener &) = delete;

	// A connected fd the caller owns, or -1 when none arrived within timeout_ms.
	int Accept(int timeout_ms);

private:
	std::string path_;
	int fd_ = -1;
};

bool UdsPathFits(const std::string &path);
uint64_t UdsPathMax();

} // namespace n6k
} // namespace duckdb
