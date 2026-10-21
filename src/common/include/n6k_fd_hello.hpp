#pragma once

#if !defined(_WIN32)

#include "n6k_msgpack.hpp"

#include <chrono>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace n6k {

struct FdReplyWait {
	ClientContext &context;
	const char *fn_name;
	std::chrono::steady_clock::time_point deadline;
	std::string on_timeout;
};

void WriteN6kFrameOnFd(int fd, const std::string &frame, const FdReplyWait &wait);

msgpack::object_handle ReadN6kFrameOnFd(int fd, const FdReplyWait &wait);

std::vector<std::string> HelloOnFd(ClientContext &context, int fd, const std::string &token, const char *fn_name);

} // namespace n6k
} // namespace duckdb

#endif
