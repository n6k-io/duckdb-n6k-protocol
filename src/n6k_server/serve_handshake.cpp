#include "serve_handshake.hpp"

#include "frame_codec.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"

#include "duckdb/common/exception.hpp"

#include <chrono>

namespace duckdb {
namespace n6k {

void SendHelloErrAndShutdownWrite(ServeTransport &conn, const std::string &message) {
	try {
		conn.WriteFrame(StreamFrameType::N6K, PackHelloErr("InvalidInputException", message));
		conn.ShutdownWrite();
	} catch (const std::exception &) {
	}
}

[[noreturn]] static void RejectHandshake(ServeTransport &conn, const std::string &message) {
	SendHelloErrAndShutdownWrite(conn, message);
	throw InvalidInputException("n6k_server: %s", message);
}

HelloRequest ReadHello(ServeTransport &conn) {
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(HANDSHAKE_TIMEOUT_MS);
	std::string frame;
	StreamFrameType type;
	switch (conn.ReadFrameBy(frame, type, deadline)) {
	case FrameRead::FRAME:
		break;
	case FrameRead::CLOSED:
		throw IOException("n6k_server: peer closed before FT_HELLO");
	case FrameRead::TIMED_OUT:
		RejectHandshake(conn, "no FT_HELLO within " + std::to_string(HANDSHAKE_TIMEOUT_MS) + "ms");
	}
	if (type != StreamFrameType::N6K) {
		RejectHandshake(conn, "first frame must be FT_HELLO");
	}
	msgpack::object_handle oh;
	const char *body;
	size_t body_len;
	try {
		ParseFrame(frame, oh, body, body_len);
	} catch (const std::exception &) {
		RejectHandshake(conn, "first frame must be FT_HELLO");
	}
	const msgpack::object &h = oh.get();
	if (static_cast<FrameType>(GetInt(h, "t", -1)) != FrameType::HELLO) {
		RejectHandshake(conn, "first frame must be FT_HELLO");
	}
	const msgpack::object *token = FindKey(h, "token");
	if (!token || token->type != msgpack::type::STR) {
		RejectHandshake(conn, "FT_HELLO requires a string \"token\"");
	}
	const auto version = GetInt(h, "protocol_version", -1);
	if (version != N6K_PROTOCOL_VERSION) {
		RejectHandshake(conn, "client protocol_version=" + std::to_string(version) + ", server speaks " +
		                          std::to_string(N6K_PROTOCOL_VERSION));
	}
	return HelloRequest {std::string(token->via.str.ptr, token->via.str.size), GetStrArray(h, "capabilities")};
}

} // namespace n6k
} // namespace duckdb
