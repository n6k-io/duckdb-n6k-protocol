#pragma once

#include "serve_transport.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
namespace n6k {

static constexpr int64_t HANDSHAKE_TIMEOUT_MS = 5000;

void SendHelloErrAndShutdownWrite(ServeTransport &conn, const std::string &message);

struct HelloRequest {
	std::string token;
	std::vector<std::string> capabilities;
};

// Rejects (HELLO_ERR, then throws) a timeout, a first frame that is not a parseable FT_HELLO, a
// missing token, or a missing or mismatched protocol_version. Throws without HELLO_ERR on any transport error,
// including the peer closing first.
HelloRequest ReadHello(ServeTransport &conn);

} // namespace n6k
} // namespace duckdb
