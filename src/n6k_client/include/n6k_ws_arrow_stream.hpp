#pragma once

#include "duckdb.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

struct ArrowArrayStream;

namespace duckdb {
namespace n6k {

class RequestState;

// Takes shared ownership of req; emits CREDIT as batches consume, CANCEL on release if unfinished;
// should_cancel polled to abort.
void StartStreamingArrowFromRequestState(const std::string &catalog, uint8_t op, std::shared_ptr<RequestState> req,
                                         ArrowArrayStream *out, std::function<bool()> should_cancel = {});

} // namespace n6k
} // namespace duckdb
