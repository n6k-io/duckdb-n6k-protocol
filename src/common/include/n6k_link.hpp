#pragma once

#include "crossing_link.hpp"
#include "n6k_msgpack.hpp"

#include "duckdb/common/error_data.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace duckdb {

class DatabaseInstance;

namespace n6k {

// A crossing frame travels in STREAM_DATA frames of at most this much payload: wasm's rings carry at most
// 1 MiB per frame.
constexpr size_t N6K_STREAM_FRAGMENT_BYTES = size_t(512) * 1024;
// What a peer may have queued on one link, whole or in pieces. Past it the peer is breaking the protocol.
constexpr size_t N6K_LINK_MAX_INBOUND_BYTES = size_t(1) << 30;
constexpr size_t N6K_LINK_MAX_STREAMS = size_t(1) << 20;

bool IsLinkFrame(FrameType type);
std::string PackLinkFrame(FrameType type, int64_t ns, uint32_t link);
std::string PackLinkEnd(int64_t ns, uint32_t link, optional_ptr<const ErrorData> error);

struct N6kLinkSlot;

//! One end of a link. Both ends are the same: Deliver takes the peer's frames on the thread reading the socket,
//! decodes them and hands them to the stream's receiver; a stream not open yet holds them for it.
class N6kLinkState : public CrossingLink, public std::enable_shared_from_this<N6kLinkState> {
public:
	using SendBytes = std::function<void(std::string)>;

	N6kLinkState(DatabaseInstance &db, int64_t ns, uint32_t link, idx_t streams, SendBytes send);
	~N6kLinkState() override;

	idx_t Streams() override {
		return streams;
	}
	unique_ptr<CrossingStream> Open(idx_t id, optional_ptr<CrossingReceiver> receiver) override;

	//! Throws when the peer breaks the protocol; the connection is then unusable.
	void Deliver(FrameType type, const msgpack::object &header, const char *body, size_t body_len);
	//! Ends every stream, present and future, once its queued frames are delivered. An empty error ends them cleanly.
	void Fail(const ErrorData &error);
	bool Ended();

	uint32_t Id() const {
		return link;
	}
	//! Called once as each stream closes, before anything is sent.
	void SetOnClose(std::function<void()> on_close_p);
	void SendFrame(std::string frame);

	//! Since when every control frame this end received has been answered, or 0: far is between statements. Read only
	//! at the end that answers.
	int64_t ControlIdleSinceMs() const {
		return control_idle_ms.load();
	}
	int64_t LastProgressMs() const {
		return last_progress_ms.load();
	}
	//! A Send is waiting for the peer's credit.
	bool SendParked();
	//! Asks the peer to stop what it is running on this link. Sent once.
	void SendCancel();

private:
	friend class N6kLinkStream;

	std::shared_ptr<N6kLinkSlot> SlotOf(idx_t id);
	void DeliverData(idx_t id, const msgpack::object &header, const char *body, size_t body_len);
	void DeliverEnd(idx_t id, const msgpack::object &header);
	void DeliverCredit(idx_t id, const msgpack::object &header);
	//! Hands the slot's queued frames to its receiver, unless another thread is already doing so.
	void Pump(const std::shared_ptr<N6kLinkSlot> &slot);
	void Touch();

	CrossingIo StreamSend(N6kLinkSlot &slot, CrossingFrame &frame, const CrossingWaker &waker);
	void StreamClose(N6kLinkSlot &slot, optional_ptr<const ErrorData> error);
	void StreamDetach(N6kLinkSlot &slot, idx_t open);
	std::string EncodeRows(N6kLinkSlot &slot, DataChunk &rows);
	void DecodeRows(N6kLinkSlot &slot, const std::string &payload, size_t offset, CrossingReceiver &receiver,
	                DataChunk &rows);

	DatabaseInstance &db;
	const int64_t ns;
	const uint32_t link;
	const idx_t streams;
	const SendBytes send;

	std::mutex lock;
	std::condition_variable delivered;
	std::unordered_map<idx_t, std::shared_ptr<N6kLinkSlot>> slots;
	size_t inbound_bytes = 0;
	bool ended = false;
	ErrorData end_error;
	std::function<void()> on_close;
	int64_t control_unanswered = 0;

	std::atomic<bool> cancel_sent {false};
	std::atomic<int64_t> control_idle_ms;
	std::atomic<int64_t> last_progress_ms;
};

} // namespace n6k
} // namespace duckdb
