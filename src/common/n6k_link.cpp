#include "n6k_link.hpp"

#include "arrow_ipc_encode.hpp"
#include "n6k_arrow_frame_decoder.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"

#include <chrono>
#include <cstring>
#include <deque>
#include <thread>

namespace duckdb {
namespace n6k {

namespace {

constexpr idx_t CONTROL_STREAM = 0;

int64_t NowMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

void PackError(msgpack::packer<msgpack::sbuffer> &pk, const ErrorData &error) {
	PackString(pk, "exception_type");
	pk.pack(Exception::ExceptionTypeToString(error.Type()));
	PackString(pk, "exception_message");
	pk.pack(error.RawMessage());
}

ErrorData ErrorOf(const msgpack::object &header) {
	if (!HasKey(header, "exception_type")) {
		return ErrorData();
	}
	return ErrorData(Exception::StringToExceptionType(GetStr(header, "exception_type")),
	                 GetStr(header, "exception_message"));
}

std::string PackStreamEnd(int64_t ns, uint32_t link, idx_t stream, optional_ptr<const ErrorData> error) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(error ? 6 : 4);
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::STREAM_END));
	PackString(pk, "ns");
	pk.pack(ns);
	PackString(pk, "link");
	pk.pack(link);
	PackString(pk, "stream");
	pk.pack(stream);
	if (error) {
		PackError(pk, *error);
	}
	return std::string(sb.data(), sb.size());
}

std::string PackStreamCredit(int64_t ns, uint32_t link, idx_t stream, uint32_t n) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(5);
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::STREAM_CREDIT));
	PackString(pk, "ns");
	pk.pack(ns);
	PackString(pk, "link");
	pk.pack(link);
	PackString(pk, "stream");
	pk.pack(stream);
	PackString(pk, "n");
	pk.pack(n);
	return std::string(sb.data(), sb.size());
}

std::string PackStreamData(int64_t ns, uint32_t link, idx_t stream, bool fin, const char *data, size_t len) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(5);
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::STREAM_DATA));
	PackString(pk, "ns");
	pk.pack(ns);
	PackString(pk, "link");
	pk.pack(link);
	PackString(pk, "stream");
	pk.pack(stream);
	PackString(pk, "fin");
	pk.pack(fin);
	std::string frame(sb.data(), sb.size());
	frame.append(data, len);
	return frame;
}

//! `[u32 header_len][header][arrow]`.
struct Inbound {
	std::string payload;
	size_t header_len = 0;
};

} // namespace

bool IsLinkFrame(FrameType type) {
	switch (type) {
	case FrameType::LINK_OPEN:
	case FrameType::LINK_CANCEL:
	case FrameType::LINK_CLOSE:
	case FrameType::LINK_END:
	case FrameType::STREAM_DATA:
	case FrameType::STREAM_END:
	case FrameType::STREAM_CREDIT:
		return true;
	default:
		return false;
	}
}

std::string PackLinkFrame(FrameType type, int64_t ns, uint32_t link) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(3);
	PackString(pk, "t");
	pk.pack(static_cast<int>(type));
	PackString(pk, "ns");
	pk.pack(ns);
	PackString(pk, "link");
	pk.pack(link);
	return std::string(sb.data(), sb.size());
}

std::string PackLinkEnd(int64_t ns, uint32_t link, optional_ptr<const ErrorData> error) {
	msgpack::sbuffer sb;
	msgpack::packer<msgpack::sbuffer> pk(&sb);
	pk.pack_map(error ? 5 : 3);
	PackString(pk, "t");
	pk.pack(static_cast<int>(FrameType::LINK_END));
	PackString(pk, "ns");
	pk.pack(ns);
	PackString(pk, "link");
	pk.pack(link);
	if (error) {
		PackError(pk, *error);
	}
	return std::string(sb.data(), sb.size());
}

struct N6kLinkSlot {
	explicit N6kLinkSlot(idx_t id_p) : id(id_p) {
	}

	const idx_t id;

	// Guarded by N6kLinkState::lock.
	bool opened = false;
	optional_ptr<CrossingReceiver> receiver;
	std::deque<Inbound> inbound;
	std::string partial;
	size_t queued_bytes = 0;
	bool peer_ended = false;
	ErrorData peer_error;
	bool closed = false;
	bool closed_delivered = false;
	bool delivering = false;
	std::thread::id delivering_thread;
	int send_credits = DEFAULT_BATCH_CREDITS;
	int recv_window = DEFAULT_BATCH_CREDITS;
	bool send_armed = false;
	CrossingWaker send_waker;

	// Used by the one thread at a time that sends, and the one that delivers.
	unique_ptr<Connection> send_codec;
	unique_ptr<ArrowIpcStreamEncoder> encoder;
	unique_ptr<Connection> recv_codec;
	unique_ptr<N6kArrowFrameDecoder> decoder;
	unique_ptr<ArrowScanLocalState> scan_state;
	ArrowTableSchema arrow_table;
	vector<LogicalType> wire_types;
	vector<bool> needs_cast;
	bool any_cast = false;
	unique_ptr<DataChunk> scratch;
};

class N6kLinkStream : public CrossingStream {
public:
	N6kLinkStream(std::shared_ptr<N6kLinkState> state_p, std::shared_ptr<N6kLinkSlot> slot_p)
	    : state(std::move(state_p)), slot(std::move(slot_p)) {
	}

	CrossingIo Send(CrossingFrame &frame, CrossingWaker waker) override {
		return state->StreamSend(*slot, frame, waker);
	}
	void Close(optional_ptr<const ErrorData> error) override {
		state->StreamClose(*slot, error);
	}

private:
	std::shared_ptr<N6kLinkState> state;
	std::shared_ptr<N6kLinkSlot> slot;
};

N6kLinkState::N6kLinkState(DatabaseInstance &db_p, int64_t ns_p, uint32_t link_p, idx_t streams_p, SendBytes send_p)
    : db(db_p), ns(ns_p), link(link_p), streams(streams_p), send(std::move(send_p)), control_idle_ms(NowMs()),
      last_progress_ms(NowMs()) {
}

N6kLinkState::~N6kLinkState() = default;

void N6kLinkState::Touch() {
	last_progress_ms.store(NowMs());
}

void N6kLinkState::SendFrame(std::string frame) {
	send(std::move(frame));
}

void N6kLinkState::SetOnClose(std::function<void()> on_close_p) {
	std::lock_guard<std::mutex> guard(lock);
	on_close = std::move(on_close_p);
}

bool N6kLinkState::Ended() {
	std::lock_guard<std::mutex> guard(lock);
	return ended;
}

bool N6kLinkState::SendParked() {
	std::lock_guard<std::mutex> guard(lock);
	for (auto &entry : slots) {
		if (entry.second->send_armed) {
			return true;
		}
	}
	return false;
}

void N6kLinkState::SendCancel() {
	if (!cancel_sent.exchange(true)) {
		send(PackLinkFrame(FrameType::LINK_CANCEL, ns, link));
	}
}

std::shared_ptr<N6kLinkSlot> N6kLinkState::SlotOf(idx_t id) {
	auto it = slots.find(id);
	if (it != slots.end()) {
		return it->second;
	}
	if (slots.size() >= N6K_LINK_MAX_STREAMS) {
		throw IOException("n6k: link %llu opened more than %llu streams", link, N6K_LINK_MAX_STREAMS);
	}
	auto slot = std::make_shared<N6kLinkSlot>(id);
	slots[id] = slot;
	return slot;
}

unique_ptr<CrossingStream> N6kLinkState::Open(idx_t id, optional_ptr<CrossingReceiver> receiver) {
	std::shared_ptr<N6kLinkSlot> slot;
	{
		std::lock_guard<std::mutex> guard(lock);
		slot = SlotOf(id);
		slot->opened = true;
		slot->receiver = receiver;
	}
	Pump(slot);
	return make_uniq<N6kLinkStream>(shared_from_this(), slot);
}

void N6kLinkState::Pump(const std::shared_ptr<N6kLinkSlot> &slot_p) {
	auto &slot = *slot_p;
	std::unique_lock<std::mutex> guard(lock);
	if (slot.delivering || !slot.opened) {
		return;
	}
	slot.delivering = true;
	slot.delivering_thread = std::this_thread::get_id();
	while (!slot.closed) {
		auto receiver = slot.receiver;
		if (!slot.inbound.empty()) {
			auto next = std::move(slot.inbound.front());
			slot.inbound.pop_front();
			slot.queued_bytes -= next.payload.size();
			inbound_bytes -= next.payload.size();
			if (slot.id != CONTROL_STREAM) {
				slot.recv_window++;
			}
			const bool link_ended = ended;
			guard.unlock();
			ErrorData failure;
			try {
				if (receiver) {
					CrossingFrame frame;
					frame.header.assign(next.payload, sizeof(uint32_t), next.header_len);
					const auto arrow_at = sizeof(uint32_t) + next.header_len;
					if (next.payload.size() > arrow_at) {
						DecodeRows(slot, next.payload, arrow_at, *receiver, frame.rows);
					}
					receiver->Deliver(frame);
				}
				if (slot.id != CONTROL_STREAM && !link_ended) {
					send(PackStreamCredit(ns, link, slot.id, 1));
				}
			} catch (const std::exception &ex) {
				failure = ErrorData(ex);
			}
			guard.lock();
			if (failure.HasError() && !slot.peer_ended) {
				slot.peer_ended = true;
				slot.peer_error = failure;
				inbound_bytes -= slot.queued_bytes;
				slot.queued_bytes = 0;
				slot.inbound.clear();
			}
			continue;
		}
		if (receiver && !slot.closed_delivered && (slot.peer_ended || ended)) {
			slot.closed_delivered = true;
			const auto error = slot.peer_ended ? slot.peer_error : end_error;
			guard.unlock();
			receiver->Closed(error.HasError() ? &error : nullptr);
			guard.lock();
			continue;
		}
		break;
	}
	slot.delivering = false;
	guard.unlock();
	delivered.notify_all();
}

void N6kLinkState::Deliver(FrameType type, const msgpack::object &header, const char *body, size_t body_len) {
	Touch();
	if (type == FrameType::LINK_END) {
		Fail(ErrorOf(header));
		return;
	}
	const auto stream = GetInt(header, "stream", -1);
	if (stream < 0) {
		throw IOException("n6k: a link frame names no stream");
	}
	const auto id = static_cast<idx_t>(stream);
	switch (type) {
	case FrameType::STREAM_DATA:
		DeliverData(id, header, body, body_len);
		break;
	case FrameType::STREAM_END:
		DeliverEnd(id, header);
		break;
	case FrameType::STREAM_CREDIT:
		DeliverCredit(id, header);
		break;
	default:
		throw IOException("n6k: unexpected frame on a link");
	}
}

void N6kLinkState::DeliverData(idx_t id, const msgpack::object &header, const char *body, size_t body_len) {
	const auto *fin_key = FindKey(header, "fin");
	const bool fin = fin_key && fin_key->type == msgpack::type::BOOLEAN && fin_key->via.boolean;
	std::shared_ptr<N6kLinkSlot> slot;
	{
		std::lock_guard<std::mutex> guard(lock);
		slot = SlotOf(id);
		if (slot->closed) {
			return;
		}
		if (inbound_bytes + body_len > N6K_LINK_MAX_INBOUND_BYTES) {
			throw IOException("n6k: link %llu holds more than %llu inbound bytes", link, N6K_LINK_MAX_INBOUND_BYTES);
		}
		slot->partial.append(body, body_len);
		slot->queued_bytes += body_len;
		inbound_bytes += body_len;
		if (!fin) {
			return;
		}
		if (id != CONTROL_STREAM && --slot->recv_window < 0) {
			throw IOException("n6k: stream %llu of link %llu sent past its credit", id, link);
		}
		if (slot->partial.size() < sizeof(uint32_t)) {
			throw IOException("n6k: stream %llu of link %llu sent a frame with no header length", id, link);
		}
		uint32_t header_len;
		std::memcpy(&header_len, slot->partial.data(), sizeof(header_len));
		if (slot->partial.size() - sizeof(uint32_t) < header_len) {
			throw IOException("n6k: stream %llu of link %llu sent a frame shorter than its header", id, link);
		}
		Inbound frame;
		frame.header_len = header_len;
		frame.payload = std::move(slot->partial);
		slot->partial.clear();
		slot->inbound.push_back(std::move(frame));
		if (id == CONTROL_STREAM) {
			control_unanswered++;
			control_idle_ms.store(0);
		}
	}
	Pump(slot);
}

void N6kLinkState::DeliverEnd(idx_t id, const msgpack::object &header) {
	std::shared_ptr<N6kLinkSlot> slot;
	CrossingWaker to_wake;
	bool wake = false;
	{
		std::lock_guard<std::mutex> guard(lock);
		slot = SlotOf(id);
		if (slot->peer_ended) {
			return;
		}
		slot->peer_ended = true;
		slot->peer_error = ErrorOf(header);
		if (slot->send_armed) {
			slot->send_armed = false;
			wake = true;
			to_wake = slot->send_waker;
		}
	}
	if (wake) {
		to_wake.Wake();
	}
	Pump(slot);
}

void N6kLinkState::DeliverCredit(idx_t id, const msgpack::object &header) {
	const auto n = GetInt(header, "n", 0);
	if (n <= 0 || n > DEFAULT_BATCH_CREDITS) {
		throw IOException("n6k: stream %llu of link %llu granted %lld credits", id, link, n);
	}
	CrossingWaker to_wake;
	bool wake = false;
	{
		std::lock_guard<std::mutex> guard(lock);
		auto slot = SlotOf(id);
		slot->send_credits += static_cast<int>(n);
		if (slot->send_credits > DEFAULT_BATCH_CREDITS) {
			throw IOException("n6k: stream %llu of link %llu granted credit it never used", id, link);
		}
		if (slot->send_armed) {
			slot->send_armed = false;
			wake = true;
			to_wake = slot->send_waker;
		}
	}
	if (wake) {
		to_wake.Wake();
	}
}

void N6kLinkState::Fail(const ErrorData &error) {
	vector<CrossingWaker> to_wake;
	vector<std::shared_ptr<N6kLinkSlot>> open;
	{
		std::lock_guard<std::mutex> guard(lock);
		if (ended) {
			return;
		}
		ended = true;
		end_error = error;
		for (auto &entry : slots) {
			auto &slot = *entry.second;
			open.push_back(entry.second);
			if (slot.send_armed) {
				slot.send_armed = false;
				to_wake.push_back(slot.send_waker);
			}
		}
	}
	for (auto &waker : to_wake) {
		waker.Wake();
	}
	for (auto &slot : open) {
		Pump(slot);
	}
}

CrossingIo N6kLinkState::StreamSend(N6kLinkSlot &slot, CrossingFrame &frame, const CrossingWaker &waker) {
	ErrorData failure;
	{
		std::lock_guard<std::mutex> guard(lock);
		if (slot.peer_ended || ended) {
			failure = slot.peer_ended ? slot.peer_error : end_error;
			if (!failure.HasError()) {
				return CrossingIo::DONE;
			}
		} else if (slot.id != CONTROL_STREAM) {
			if (slot.send_credits == 0) {
				slot.send_armed = true;
				slot.send_waker = waker;
				return CrossingIo::WAIT;
			}
			slot.send_credits--;
		}
	}
	if (failure.HasError()) {
		failure.Throw();
	}
	std::string arrow;
	if (frame.rows.ColumnCount() > 0 && frame.rows.size() > 0) {
		arrow = EncodeRows(slot, frame.rows);
	}
	const auto header_len = static_cast<uint32_t>(frame.header.size());
	std::string payload;
	payload.reserve(sizeof(header_len) + frame.header.size() + arrow.size());
	payload.append(reinterpret_cast<const char *>(&header_len), sizeof(header_len));
	payload.append(frame.header);
	payload.append(arrow);
	size_t offset = 0;
	while (true) {
		const auto len = MinValue<size_t>(N6K_STREAM_FRAGMENT_BYTES, payload.size() - offset);
		const bool fin = offset + len == payload.size();
		send(PackStreamData(ns, link, slot.id, fin, payload.data() + offset, len));
		offset += len;
		if (fin) {
			break;
		}
	}
	if (slot.id == CONTROL_STREAM) {
		std::lock_guard<std::mutex> guard(lock);
		if (control_unanswered > 0 && --control_unanswered == 0) {
			control_idle_ms.store(NowMs());
		}
	}
	Touch();
	return CrossingIo::TAKEN;
}

void N6kLinkState::StreamClose(N6kLinkSlot &slot, optional_ptr<const ErrorData> error) {
	std::function<void()> hook;
	bool link_ended;
	{
		std::unique_lock<std::mutex> guard(lock);
		if (slot.closed) {
			return;
		}
		slot.closed = true;
		inbound_bytes -= slot.queued_bytes;
		slot.queued_bytes = 0;
		slot.inbound.clear();
		slot.partial.clear();
		if (slot.delivering_thread != std::this_thread::get_id()) {
			delivered.wait(guard, [&slot]() { return !slot.delivering; });
		}
		slot.receiver = nullptr;
		link_ended = ended;
		hook = on_close;
	}
	try {
		if (hook) {
			hook();
		}
		if (!link_ended) {
			send(PackStreamEnd(ns, link, slot.id, error));
		}
	} catch (...) { // NOLINT(bugprone-empty-catch): Close never throws, and a dead transport fails the link anyway
	}
}

std::string N6kLinkState::EncodeRows(N6kLinkSlot &slot, DataChunk &rows) {
	rows.Flatten();
	std::string out;
	if (!slot.encoder) {
		slot.send_codec = make_uniq<Connection>(db);
		auto types = rows.GetTypes();
		vector<string> names;
		for (idx_t c = 0; c < types.size(); c++) {
			names.push_back("c" + std::to_string(c));
		}
		slot.encoder = make_uniq<ArrowIpcStreamEncoder>(*slot.send_codec->context, types, names);
		out = slot.encoder->SchemaMessage();
	}
	out += slot.encoder->EncodeChunk(rows);
	return out;
}

void N6kLinkState::DecodeRows(N6kLinkSlot &slot, const std::string &payload, size_t offset, CrossingReceiver &receiver,
                              DataChunk &rows) {
	if (!slot.recv_codec) {
		slot.recv_codec = make_uniq<Connection>(db);
	}
	auto &context = *slot.recv_codec->context;
	if (!slot.decoder) {
		slot.decoder = make_uniq<N6kArrowFrameDecoder>();
	}
	if (!slot.decoder->PushFrame(reinterpret_cast<const uint8_t *>(payload.data()) + offset, payload.size() - offset)) {
		throw IOException("n6k: stream %llu of link %llu sent bad Arrow: %s", slot.id, link,
		                  slot.decoder->ErrorMessage());
	}
	const auto &types = receiver.Types();
	rows.Initialize(Allocator::DefaultAllocator(), types);
	if (!slot.scan_state) {
		if (!slot.decoder->HasSchema()) {
			throw IOException("n6k: stream %llu of link %llu sent rows before a schema", slot.id, link);
		}
		ArrowTableFunction::PopulateArrowTableSchema(context, slot.arrow_table, *slot.decoder->GetSchema());
		slot.wire_types = slot.arrow_table.GetTypes();
		if (slot.wire_types.size() != types.size()) {
			throw IOException("n6k: stream %llu of link %llu sent %llu columns, %llu expected", slot.id, link,
			                  slot.wire_types.size(), types.size());
		}
		slot.needs_cast.resize(types.size(), false);
		for (idx_t c = 0; c < types.size(); c++) {
			if (slot.wire_types[c] != types[c]) {
				slot.needs_cast[c] = true;
				slot.any_cast = true;
			}
		}
		slot.scan_state = make_uniq<ArrowScanLocalState>(make_uniq<ArrowArrayWrapper>(), context);
		for (idx_t c = 0; c < types.size(); c++) {
			slot.scan_state->column_ids.push_back(c);
		}
	}
	auto wrapper = make_shared_ptr<ArrowArrayWrapper>();
	if (!slot.decoder->TryPopBatch(&wrapper->arrow_array)) {
		throw IOException("n6k: stream %llu of link %llu sent a frame with no rows", slot.id, link);
	}
	const auto count = static_cast<idx_t>(wrapper->arrow_array.length);
	if (count > STANDARD_VECTOR_SIZE) {
		throw IOException("n6k: stream %llu of link %llu sent %llu rows in one frame", slot.id, link, count);
	}
	auto &state = *slot.scan_state;
	state.Reset();
	state.chunk = std::move(wrapper);
	state.chunk_offset = 0;
	auto &columns = slot.arrow_table.GetColumns();
	if (!slot.any_cast) {
		rows.SetChildCardinality(count);
		ArrowTableFunction::ArrowToDuckDB(state, columns, rows);
		rows.Verify();
		return;
	}
	if (!slot.scratch) {
		slot.scratch = make_uniq<DataChunk>();
		slot.scratch->Initialize(context, slot.wire_types);
	}
	slot.scratch->Reset();
	slot.scratch->SetChildCardinality(count);
	ArrowTableFunction::ArrowToDuckDB(state, columns, *slot.scratch);
	rows.SetChildCardinality(count);
	for (idx_t c = 0; c < rows.ColumnCount(); c++) {
		if (slot.needs_cast[c]) {
			VectorOperations::Cast(context, slot.scratch->data[c], rows.data[c], count);
		} else {
			rows.data[c].Reference(slot.scratch->data[c]);
		}
	}
	rows.Verify();
}

} // namespace n6k
} // namespace duckdb
