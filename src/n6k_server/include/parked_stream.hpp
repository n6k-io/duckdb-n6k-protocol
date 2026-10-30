#pragma once

namespace duckdb {
namespace n6k {

class ServeReactor;
struct RequestSlot;

enum class PumpResult {
	DONE,
	// Out of credit; the worker releases it.
	PARKED,
	// Could keep sending, but another task is waiting for a worker.
	YIELD
};

// A streamed response that outlives the worker that started it. It owns everything its frames
// still read from, so any worker can carry it on.
class ParkedStream {
public:
	virtual ~ParkedStream() = default;
	virtual PumpResult Pump(ServeReactor &reactor, RequestSlot &slot) = 0;
	// Send the cancelled terminal frame and release what the stream holds.
	virtual void Cancel(ServeReactor &reactor, RequestSlot &slot) = 0;
};

} // namespace n6k
} // namespace duckdb
