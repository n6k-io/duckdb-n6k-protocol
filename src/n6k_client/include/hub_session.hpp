#pragma once

#include "connection_hub.hpp"
#include "ws_reactor.hpp"

#include <atomic>
#include <cstdint>
#include <memory>

namespace duckdb {
namespace n6k {

// One catalog session on a ConnectionHub: the frames tagged with its `ns`, on the connection it bound to.
class HubSession : public Reactor {
public:
	HubSession(std::shared_ptr<ConnectionHub> hub, uint64_t ns);
	~HubSession() override;

	void Start(FrameFn on_frame, CloseFn on_close) override;
	void Send(const std::string &frame) override;
	void Close() override;
	uint64_t Generation() override;

private:
	void FireClose(const std::string &reason);

	std::shared_ptr<ConnectionHub> hub_;
	uint64_t ns_;
	std::atomic<uint64_t> bound_generation_ {0};
	CloseFn on_close_;
	std::atomic<bool> close_fired_ {false};
	bool started_ = false;
	std::atomic<bool> closed_ {false};
};

} // namespace n6k
} // namespace duckdb
