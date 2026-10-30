#pragma once

#include "frame_codec.hpp"
#include "parked_stream.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace duckdb {

class Connection;

namespace n6k {

class ServeReactor;
struct RequestSlot;
struct SessionRef;

// Dispatch one request; handlers emit frames through `slot` (SendForRequest / EndRequest) and throw on failure
// (worker emits FT_ERR). A streamed response comes back unsent past its schema, holding `own`, for the worker to Pump;
// a streamed op may move from `own` even when it then throws. A non-streamed op returns nullptr having sent its whole
// reply, and leaves `own` in place.
std::unique_ptr<ParkedStream> HandleRequest(ServeReactor &reactor, std::unique_ptr<Connection> &own,
                                            const SessionRef &sess, uint8_t op, const char *body, size_t body_len,
                                            RequestSlot &slot);

// Emit an FT_ERR frame for a request with no RequestSlot; an admitted one ends through
// ServeReactor::EndRequest. `retriable` is passed through as given.
void SendError(ServeReactor &reactor, const FrameTarget &target, const std::string &type, const std::string &message,
               bool retriable = false);

} // namespace n6k
} // namespace duckdb
