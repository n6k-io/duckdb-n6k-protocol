#pragma once

#include "frame_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <string>

namespace duckdb {

class Connection;

namespace n6k {

class ServeReactor;
struct RequestSlot;
struct SessionRef;

// Dispatch one request; handlers emit frames via reactor and throw on failure (worker emits FT_ERR).
// `sess` supplies both the catalog to qualify against and the ns every reply is stamped with.
void HandleRequest(ServeReactor &reactor, Connection &conn, const SessionRef &sess, uint8_t op, const char *body,
                   size_t body_len, uint32_t req_id, RequestSlot &slot);

// Emit an FT_ERR frame; retriable is set for TransactionException (client may retry).
void SendError(ServeReactor &reactor, const FrameTarget &target, const std::string &type, const std::string &message,
               bool retriable = false);

} // namespace n6k
} // namespace duckdb
