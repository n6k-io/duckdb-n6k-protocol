#include "hub_session.hpp"

#include "ws_msgpack.hpp"

namespace duckdb {
namespace n6k {

HubSession::HubSession(std::shared_ptr<ConnectionHub> hub, uint64_t ns) : hub_(std::move(hub)), ns_(ns) {
}

HubSession::~HubSession() {
	Close();
}

void HubSession::Start(FrameFn on_frame, CloseFn on_close) {
	on_close_ = std::move(on_close);
	started_ = true;
	// Raw `this`: Close() unbinds and waits out every callback before this object can go.
	bound_generation_ =
	    hub_->BindSession(ns_, std::move(on_frame), [this](const std::string &reason) { FireClose(reason); });
}

void HubSession::Send(const std::string &frame) {
	if (closed_.load() || close_fired_.load()) {
		return;
	}
	if (!hub_->SendOnGeneration(bound_generation_, frame)) {
		FireClose("n6k: connection " + hub_->IdString() + " is not open");
	}
}

void HubSession::Close() {
	if (closed_.exchange(true) || !started_) {
		return;
	}
	hub_->UnbindSession(ns_);
	// Dropped by the hub if the connection was redialed since. A session left on a registered socket that
	// survived the redial stays open on the server until that socket closes.
	hub_->SendOnGeneration(bound_generation_, PackIdOnlyFrame(static_cast<int>(FrameType::CANCEL), 0, ns_));
}

uint64_t HubSession::Generation() {
	return hub_->Generation();
}

void HubSession::FireClose(const std::string &reason) {
	if (!close_fired_.exchange(true) && on_close_) {
		on_close_(reason);
	}
}

} // namespace n6k
} // namespace duckdb
