#include "serve_reactor.hpp"

#include "frame_codec.hpp"
#include "host_callbacks.hpp"
#include "n6k_exception_types.hpp"
#include "n6k_served.hpp"
#include "n6k_sql_builder.hpp"
#include "n6k_stream_framing.hpp"
#include "n6k_protocol_generated.hpp"
#include "request_handlers.hpp"
#include "serve_handshake.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/logger.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <memory>
#include <string>

namespace duckdb {
namespace n6k {

// How long shutdown waits for requests to finish before interrupting them.
static constexpr int SHUTDOWN_DRAIN_TIMEOUT_MS = 5000;

// Past this many unsent payload bytes the client has stopped reading: the queue is dropped for a CLOSE and
// nothing more is queued.
static constexpr size_t MAX_QUEUED_OUT_BYTES = static_cast<size_t>(256) << 20;

// JSON-RPC messages waiting for on_text; past it the client gets a busy error instead.
static constexpr size_t MAX_QUEUED_TEXT = 64;

// How long AwaitServe waits for the serve of its session to register.
static constexpr int RESOLVE_REGISTER_WAIT_MS = 10000;

// Idle connections get reaped by proxies, and a client-side timer is no substitute — browsers
// throttle those in background tabs. 0 or negative disables.
static constexpr const char *PING_INTERVAL_ENV = "N6K_PING_INTERVAL";
static constexpr double DEFAULT_PING_INTERVAL_S = 25.0;

static int ResolvePingIntervalMs() {
	const char *raw = std::getenv(PING_INTERVAL_ENV);
	double seconds = DEFAULT_PING_INTERVAL_S;
	if (raw != nullptr && raw[0] != '\0') {
		const std::string text(raw);
		size_t parsed = 0;
		try {
			seconds = std::stod(text, &parsed);
		} catch (const std::exception &) {
			throw InvalidInputException("n6k_server: %s=\"%s\" is not a number", PING_INTERVAL_ENV, text);
		}
		if (parsed != text.size() || !std::isfinite(seconds) || seconds > INT_MAX / 1000.0) {
			throw InvalidInputException("n6k_server: %s=\"%s\" is not a valid interval in seconds", PING_INTERVAL_ENV,
			                            text);
		}
	}
	if (seconds <= 0) {
		return 0;
	}
	return static_cast<int>(seconds * 1000);
}

ServeReactor::ServeReactor(ServeTransport &conn, DatabaseInstance &db, ClientContext &context,
                           std::vector<std::string> catalogs, ServeOptions options, hugeint_t session_id)
    : conn_(conn), db_(db), context_(context), served_(std::move(catalogs)), options_(std::move(options)),
      session_id_(session_id), open_pending_interval_ms_(options_.open_pending_interval_ms),
      resolve_unknown_(!options_.resolve.empty()) {
	served_set_.insert(served_.begin(), served_.end());
	listed_.insert(served_.begin(), served_.end());
	ping_interval_ms_ = options_.ping_interval_ms >= 0 ? options_.ping_interval_ms : ResolvePingIntervalMs();
}

static std::string ResolvingMessage(const std::string &catalog) {
	return "resolving catalog \"" + catalog + "\"";
}

// CANCEL {ns, id: 0}: what a client sends on DETACH, sent upstream when a forwarded session moves off it.
static std::string PackCloseSession(int64_t ns) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	const FrameTarget target {ns, 0};
	pk.pack_map(2 + NsKeyCount(target));
	PackTIdInto(pk, FrameType::CANCEL, target);
	return std::string(sbuf.data(), sbuf.size());
}

static std::string JoinQuoted(const std::vector<std::string> &names) {
	std::string out;
	for (auto &name : names) {
		if (!out.empty()) {
			out += ", ";
		}
		out += "\"" + name + "\"";
	}
	return out;
}

ServeReactor::~ServeReactor() {
	StopWorkers();
	StopCallbackThreads();
	StopWriter();
}

void ServeReactor::StopWriter() {
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		writer_stop_ = true;
	}
	out_cv_.notify_all();
	if (writer_.joinable()) {
		writer_.join();
	}
}

void ServeReactor::StartPinging() {
	if (ping_interval_ms_ <= 0) {
		return;
	}
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		pinging_ = true;
	}
	out_cv_.notify_one();
}

void ServeReactor::SendFrame(std::string frame) {
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		EnqueueOut(OutFrame {StreamFrameType::N6K, std::move(frame), 0});
	}
	out_cv_.notify_one();
}

void ServeReactor::SendJsonRpc(std::string message) {
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		EnqueueOut(OutFrame {StreamFrameType::JSONRPC, std::move(message), 0});
	}
	out_cv_.notify_one();
}

void ServeReactor::SendForRequest(RequestSlot &slot, std::string frame) {
	std::lock_guard<std::mutex> lk(slot.mu);
	if (!slot.ended) {
		SendFrame(std::move(frame));
	}
}

void ServeReactor::EndRequest(RequestSlot &slot, std::string terminal) {
	std::lock_guard<std::mutex> lk(slot.mu);
	if (slot.ended) {
		return;
	}
	slot.ended = true;
	slot.cancel.store(true);
	SendFrame(std::move(terminal));
}

void ServeReactor::SendText(std::string text) {
	if (!accepts_text_) {
		throw InvalidInputException("n6k_server: the client was not granted jsonrpc or mcp");
	}
	SendJsonRpc(std::move(text));
}

// RFC 6455 caps a close reason at 123 bytes; cut on a UTF-8 boundary.
static std::string CloseReason(std::string reason) {
	static constexpr size_t MAX_CLOSE_REASON_BYTES = 123;
	if (reason.size() <= MAX_CLOSE_REASON_BYTES) {
		return reason;
	}
	size_t end = MAX_CLOSE_REASON_BYTES;
	while (end > 0 && (static_cast<unsigned char>(reason[end]) & 0xC0) == 0x80) {
		end--;
	}
	reason.resize(end);
	return reason;
}

bool ServeReactor::EnqueueOut(OutFrame frame) {
	if (out_overflowed_) {
		return false;
	}
	if (frame.type != StreamFrameType::CLOSE && out_q_bytes_ + frame.payload.size() > MAX_QUEUED_OUT_BYTES) {
		// Credits bound what a request may have queued, but not PONGs to a flood of PINGs, nor a client
		// that grants credit it never reads.
		out_overflowed_ = true;
		out_q_.clear();
		out_q_.push_back(OutFrame {StreamFrameType::CLOSE, CloseReason("client is not reading"), 1008});
		out_q_bytes_ = out_q_.back().payload.size();
		stopping_.store(true);
		return false;
	}
	out_q_bytes_ += frame.payload.size();
	out_q_.push_back(std::move(frame));
	return true;
}

void ServeReactor::Close(int code, std::string reason) {
	{
		std::lock_guard<std::mutex> lk(out_mu_);
		EnqueueOut(OutFrame {StreamFrameType::CLOSE, CloseReason(std::move(reason)), code});
	}
	out_cv_.notify_one();
}

void Upstream::Send(std::string frame) {
	{
		std::lock_guard<std::mutex> lk(out_mu);
		out_q.push_back(std::move(frame));
	}
	out_cv.notify_one();
}

std::shared_ptr<RequestSlot> ServeReactor::FindSlot(int64_t ns, uint32_t req_id) {
	std::lock_guard<std::mutex> lk(map_mu_);
	auto it = inflight_.find(InflightKey {ns, req_id});
	return it == inflight_.end() ? nullptr : it->second;
}

const std::string *ServeReactor::LookupSessionCatalog(int64_t ns) const {
	auto it = sessions_.find(ns);
	return it == sessions_.end() ? nullptr : &it->second;
}

// A duplicate or unknown request is answered, not dropped: there is no client-visible log channel,
// and a silent drop hangs the client's attach waiter.
void ServeReactor::HandleOpen(const msgpack::object &header, const std::string &frame) {
	const auto ns = GetInt(header, "ns", 0);
	const std::string catalog = GetStr(header, "catalog");
	auto capabilities = IntersectCaps(GetStrArray(header, "capabilities"), OPEN_CAPS);

	if (ns == 0) {
		SendFrame(PackOpenErr(ns, "InvalidInputException", "FT_OPEN requires a non-zero \"ns\""));
		return;
	}
	if (LookupSessionCatalog(ns) != nullptr || IsParked(ns) || forwarded_.count(ns) != 0) {
		SendFrame(PackOpenErr(ns, "InvalidInputException",
		                      "duplicate FT_OPEN for ns=" + std::to_string(ns) + "; that session is already open"));
		return;
	}
	if (served_set_.find(catalog) != served_set_.end()) {
		BindSession(ns, catalog, capabilities);
		return;
	}
	auto pending = pending_.find(catalog);
	if (pending != pending_.end()) {
		pending->second.parked.push_back(ParkedOpen {ns, std::move(capabilities), frame});
		SendFrame(PackOpenPending(ns, pending->second.message));
		return;
	}
	auto upstream = upstream_catalogs_.find(catalog);
	if (upstream != upstream_catalogs_.end()) {
		ForwardOpen(*upstreams_.at(upstream->second), catalog, ns, frame);
		return;
	}
	auto failed = failed_.find(catalog);
	if (failed != failed_.end()) {
		SendFrame(PackOpenErr(ns, "IOException", "catalog \"" + catalog + "\" failed to open: " + failed->second));
		return;
	}
	if (resolve_unknown_) {
		auto unresolved = unresolved_.find(catalog);
		if (unresolved == unresolved_.end() && unresolved_.size() >= static_cast<size_t>(MAX_CONCURRENT_REQS)) {
			SendFrame(PackOpenErr(ns, "InvalidInputException",
			                      "too many catalogs resolving on this connection (limit " +
			                          std::to_string(MAX_CONCURRENT_REQS) + ")"));
			return;
		}
		if (unresolved == unresolved_.end()) {
			{
				std::lock_guard<std::mutex> lk(resolve_mu_);
				resolving_.insert(catalog);
			}
			unresolved = unresolved_.emplace(catalog, std::vector<ParkedOpen> {}).first;
			StartResolve(catalog);
		}
		unresolved->second.push_back(ParkedOpen {ns, std::move(capabilities), frame});
		SendFrame(PackOpenPending(ns, ResolvingMessage(catalog)));
		return;
	}
	std::vector<std::string> offered = ListedCatalogs();
	SendFrame(PackOpenErr(ns, "InvalidInputException",
	                      "catalog \"" + catalog + "\" is not served by this connection (serving " +
	                          JoinQuoted(offered) + ")"));
}

void ServeReactor::BindSession(int64_t ns, const std::string &catalog, const std::vector<std::string> &capabilities) {
	sessions_[ns] = catalog;
	TrackSession(ns, catalog);
	SendFrame(PackOpenAck(ns, capabilities));
}

// The upstream sees only its own catalogs, so a plan naming a catalog served here or elsewhere would
// fail there: catalog_plans is not offered on.
void ServeReactor::ForwardOpen(Upstream &up, const std::string &catalog, int64_t ns, const std::string &frame) {
	forwarded_[ns] = Forwarded {&up, catalog};
	{
		std::lock_guard<std::mutex> lk(up.track_mu);
		up.opening.insert(ns);
		up.forwarded.insert(ns);
	}
	msgpack::object_handle oh;
	const char *body;
	size_t body_len;
	ParseFrame(frame, oh, body, body_len);
	auto capabilities = GetStrArray(oh.get(), "capabilities");
	capabilities.erase(std::remove(capabilities.begin(), capabilities.end(), CAP_CATALOG_PLANS), capabilities.end());
	up.Send(PackOpen(ns, catalog, capabilities));
}

void ServeReactor::ForwardUnresolved(Upstream &up, const std::string &catalog) {
	auto unresolved = unresolved_.find(catalog);
	if (unresolved == unresolved_.end()) {
		return;
	}
	for (auto &open : unresolved->second) {
		ForwardOpen(up, catalog, open.ns, open.frame);
	}
	unresolved->second.clear();
}

bool ServeReactor::IsParked(int64_t ns) const {
	for (auto &entry : pending_) {
		for (auto &open : entry.second.parked) {
			if (open.ns == ns) {
				return true;
			}
		}
	}
	for (auto &entry : unresolved_) {
		for (auto &open : entry.second) {
			if (open.ns == ns) {
				return true;
			}
		}
	}
	return false;
}

void ServeReactor::ResolvePending(const std::string &catalog, bool ok, const std::string &message) {
	{
		std::lock_guard<std::mutex> lk(resolve_mu_);
		if (resolvable_.erase(catalog) == 0) {
			throw InvalidInputException("catalog \"%s\" is not pending on this serve (never declared, or already "
			                            "resolved)",
			                            catalog);
		}
		if (!ok) {
			listed_.erase(catalog);
		}
		catalog_changes_.push_back(
		    CatalogChange {ok ? CatalogChange::Kind::READY : CatalogChange::Kind::FAILED, catalog, message});
		if (!ok) {
			Reassign(catalog);
		}
	}
}

void ServeReactor::Resolved(const std::string &catalog, const std::string &error) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	if (resolving_.erase(catalog) == 0) {
		throw InvalidInputException("catalog \"%s\" is not being resolved on this serve", catalog);
	}
	catalog_changes_.push_back(CatalogChange {CatalogChange::Kind::RESOLVED, catalog, error});
}

static std::string UpstreamServedMessage(const std::string &catalog) {
	return "catalog \"" + catalog + "\" is served by an upstream on this serve; use n6k_serve_remove_upstream";
}

void ServeReactor::Reassign(const std::string &catalog) {
	auto outranks = [](const Claim &a, const Claim &b) {
		return a.rank != b.rank ? a.rank < b.rank : a.seq < b.seq;
	};
	int owner = -1;
	if (listed_.count(catalog) == 0) {
		Claim best {NO_RANK, std::numeric_limits<uint64_t>::max()};
		auto lazy = lazy_claims_.find(catalog);
		if (lazy != lazy_claims_.end()) {
			best = lazy->second;
		}
		for (auto &entry : upstream_claims_) {
			if (entry.second.reported_set.count(catalog) != 0 && outranks(entry.second.claim, best)) {
				best = entry.second.claim;
				owner = entry.first;
			}
		}
	}
	auto current = upstream_owner_.find(catalog);
	const int previous = current == upstream_owner_.end() ? -1 : current->second;
	if (previous == owner) {
		return;
	}
	if (owner < 0) {
		upstream_owner_.erase(current);
	} else {
		upstream_owner_[catalog] = owner;
	}
	CatalogChange change {CatalogChange::Kind::UPSTREAM_OWNER, catalog, ""};
	change.upstream_id = owner;
	change.previous_upstream_id = previous;
	catalog_changes_.push_back(std::move(change));
}

std::vector<std::string> ServeReactor::NotWon(int up_fd, const std::vector<std::string> &reported) const {
	std::vector<std::string> lost;
	for (auto &name : reported) {
		auto owner = upstream_owner_.find(name);
		if (owner == upstream_owner_.end() || owner->second != up_fd) {
			lost.push_back(name);
		}
	}
	return lost;
}

void ServeReactor::DropUpstreamClaim(int up_fd) {
	auto claim = upstream_claims_.find(up_fd);
	if (claim == upstream_claims_.end()) {
		return;
	}
	auto reported = std::move(claim->second.reported);
	upstream_claims_.erase(claim);
	for (auto &name : reported) {
		Reassign(name);
	}
}

void ServeReactor::ReportUpstreamCatalogs(int up_fd, std::vector<std::string> names) {
	auto claim = upstream_claims_.find(up_fd);
	if (claim == upstream_claims_.end()) {
		return;
	}
	auto affected = claim->second.reported;
	affected.insert(affected.end(), names.begin(), names.end());
	claim->second.reported_set = std::unordered_set<std::string>(names.begin(), names.end());
	claim->second.reported = std::move(names);
	for (auto &name : affected) {
		Reassign(name);
	}
}

std::vector<std::string> ServeReactor::AddUpstream(int up_fd, const std::vector<std::string> &catalogs, int64_t rank) {
	auto up = std::make_shared<Upstream>();
	up->id = up_fd;
	up->conn.AdoptBlockingDupOfFd(up_fd);
	std::lock_guard<std::mutex> lk(resolve_mu_);
	auto status = upstream_status_.find(up_fd);
	if (status != upstream_status_.end() && status->second.open) {
		throw InvalidInputException("fd %d is already an upstream on this serve", up_fd);
	}
	upstream_status_[up_fd] = UpstreamStatus {true, ""};
	CatalogChange change {CatalogChange::Kind::UPSTREAM_ADD, "", ""};
	change.upstream = std::move(up);
	// Queued before its UPSTREAM_OWNERs, so the reader knows it when they apply.
	catalog_changes_.push_back(std::move(change));
	upstream_claims_[up_fd] = UpstreamClaim {Claim {rank, next_claim_seq_++}, catalogs,
	                                         std::unordered_set<std::string>(catalogs.begin(), catalogs.end())};
	for (auto &name : catalogs) {
		Reassign(name);
	}
	return NotWon(up_fd, catalogs);
}

std::vector<std::string> ServeReactor::SetUpstreamRank(int up_fd, int64_t rank) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	auto claim = upstream_claims_.find(up_fd);
	if (claim == upstream_claims_.end()) {
		throw InvalidInputException("fd %d is not an open upstream on this serve", up_fd);
	}
	claim->second.claim.rank = rank;
	auto reported = claim->second.reported;
	for (auto &name : reported) {
		Reassign(name);
	}
	return NotWon(up_fd, reported);
}

void ServeReactor::RemoveUpstream(int up_fd) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	auto status = upstream_status_.find(up_fd);
	if (status == upstream_status_.end() || !status->second.open) {
		throw InvalidInputException("fd %d is not an open upstream on this serve", up_fd);
	}
	status->second = UpstreamStatus {false, "removed by host"};
	CatalogChange change {CatalogChange::Kind::UPSTREAM_GONE, "", "removed by host"};
	change.upstream_id = up_fd;
	catalog_changes_.push_back(std::move(change));
	DropUpstreamClaim(up_fd);
}

UpstreamStatus ServeReactor::GetUpstreamStatus(int up_fd) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	auto status = upstream_status_.find(up_fd);
	if (status == upstream_status_.end()) {
		throw InvalidInputException("fd %d was never an upstream on this serve", up_fd);
	}
	return status->second;
}

void ServeReactor::AddCatalog(const std::string &catalog) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	if (!listed_.insert(catalog).second) {
		throw InvalidInputException("catalog \"%s\" is already served or pending on this serve", catalog);
	}
	// Its UPSTREAM_OWNER first, so an upstream's sessions on it are gone before it is served here.
	Reassign(catalog);
	catalog_changes_.push_back(CatalogChange {CatalogChange::Kind::ADD, catalog, ""});
}

void ServeReactor::AddLazyCatalog(const std::string &catalog, int64_t rank) {
	if (!resolve_unknown_) {
		throw InvalidInputException("catalog \"%s\": a lazy catalog needs a resolve callback on this serve", catalog);
	}
	std::lock_guard<std::mutex> lk(resolve_mu_);
	if (listed_.count(catalog) != 0 || lazy_claims_.count(catalog) != 0) {
		throw InvalidInputException("catalog \"%s\" is already served, pending or lazy on this serve", catalog);
	}
	lazy_claims_[catalog] = Claim {rank, next_claim_seq_++};
	catalog_changes_.push_back(CatalogChange {CatalogChange::Kind::LAZY_ADD, catalog, ""});
	Reassign(catalog);
}

void ServeReactor::RemoveCatalog(const std::string &catalog) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	const bool lazy = lazy_claims_.erase(catalog) != 0;
	const bool listed = listed_.erase(catalog) != 0;
	if (!lazy && !listed) {
		if (upstream_owner_.count(catalog) != 0) {
			throw InvalidInputException(UpstreamServedMessage(catalog));
		}
		throw InvalidInputException("catalog \"%s\" is not served or pending or lazy on this serve", catalog);
	}
	if (listed) {
		resolvable_.erase(catalog);
		catalog_changes_.push_back(CatalogChange {CatalogChange::Kind::REMOVE, catalog, ""});
	}
	if (lazy) {
		catalog_changes_.push_back(CatalogChange {CatalogChange::Kind::LAZY_REMOVE, catalog, ""});
	}
	Reassign(catalog);
}

void ServeReactor::Pause(const std::string &catalog) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	if (listed_.count(catalog) == 0 && upstream_owner_.count(catalog) != 0) {
		throw InvalidInputException(UpstreamServedMessage(catalog));
	}
	if (listed_.find(catalog) == listed_.end()) {
		throw InvalidInputException("catalog \"%s\" is not served or pending on this serve", catalog);
	}
	if (!paused_.insert(catalog).second) {
		throw InvalidInputException("catalog \"%s\" is already paused on this serve", catalog);
	}
	catalog_changes_.push_back(CatalogChange {CatalogChange::Kind::PAUSE, catalog, ""});
}

void ServeReactor::Resume(const std::string &catalog) {
	std::lock_guard<std::mutex> lk(resolve_mu_);
	if (paused_.erase(catalog) == 0) {
		throw InvalidInputException("catalog \"%s\" is not paused on this serve", catalog);
	}
	catalog_changes_.push_back(CatalogChange {CatalogChange::Kind::RESUME, catalog, ""});
}

InflightState ServeReactor::Inflight(const std::string &catalog) {
	InflightState state {false, 0};
	{
		std::lock_guard<std::mutex> lk(resolve_mu_);
		if (paused_.count(catalog) == 0) {
			throw InvalidInputException("catalog \"%s\" is not paused on this serve", catalog);
		}
		state.holding = pause_applied_.count(catalog) != 0;
	}
	std::lock_guard<std::mutex> lk(map_mu_);
	for (auto &entry : inflight_) {
		if (entry.second->catalog == catalog || entry.second->plan_catalogs.count(catalog) != 0) {
			state.running++;
		}
	}
	return state;
}

int64_t ServeReactor::CancelInflight(const std::string &catalog) {
	int64_t running = 0;
	{
		std::lock_guard<std::mutex> lk(map_mu_);
		for (auto &entry : inflight_) {
			auto &slot = *entry.second;
			if (slot.catalog != catalog && slot.plan_catalogs.count(catalog) == 0) {
				continue;
			}
			running++;
			shared_ptr<ClientContext> ctx;
			{
				std::lock_guard<std::mutex> slk(slot.mu);
				slot.cancel.store(true);
				ctx = slot.context;
			}
			QueueParked(entry.second, RequestTask::Kind::CANCEL);
			if (ctx) {
				ctx->Interrupt();
			}
		}
	}
	std::unordered_set<int64_t> sessions;
	{
		std::lock_guard<std::mutex> lk(push_mu_);
		for (auto &entry : push_sessions_) {
			if (entry.second == catalog) {
				sessions.insert(entry.first);
			}
		}
	}
	// A statement in a transaction runs on the pinned connection, not the worker's own. Interrupting an
	// idle one is harmless: DuckDB clears the flag when the next statement starts.
	std::lock_guard<std::mutex> lk(txn_mu_);
	for (auto &entry : txns_) {
		if (sessions.count(entry.first.ns) != 0 && entry.second->conn) {
			entry.second->conn->Interrupt();
		}
	}
	return running;
}

std::vector<ServeReactor::HeldReq> ServeReactor::TakeHeldOf(int64_t ns) {
	std::vector<HeldReq> taken;
	for (auto &entry : held_) {
		auto &reqs = entry.second;
		for (auto it = reqs.begin(); it != reqs.end();) {
			if (it->sess.ns == ns) {
				taken.push_back(std::move(*it));
				it = reqs.erase(it);
			} else {
				++it;
			}
		}
	}
	held_count_ -= taken.size();
	return taken;
}

bool ServeReactor::CancelHeld(int64_t ns, uint32_t req_id) {
	for (auto &entry : held_) {
		auto &reqs = entry.second;
		for (auto it = reqs.begin(); it != reqs.end(); ++it) {
			if (it->sess.ns == ns && it->req_id == req_id) {
				reqs.erase(it);
				held_count_--;
				cancels_.fetch_add(1);
				SendFrame(PackRespEndCancelled(FrameTarget {ns, req_id}));
				return true;
			}
		}
	}
	return false;
}

std::vector<std::string> ServeReactor::ListedCatalogs() const {
	std::vector<std::string> listed = served_;
	listed.insert(listed.end(), pending_order_.begin(), pending_order_.end());
	listed.insert(listed.end(), upstream_order_.begin(), upstream_order_.end());
	for (auto &name : lazy_) {
		if (served_set_.count(name) == 0 && pending_.count(name) == 0 && upstream_catalogs_.count(name) == 0) {
			listed.push_back(name);
		}
	}
	return listed;
}

void ServeReactor::ApplyCatalogChanges() {
	std::vector<CatalogChange> changes;
	{
		std::lock_guard<std::mutex> lk(resolve_mu_);
		changes.swap(catalog_changes_);
	}
	bool listed_changed = false;
	for (auto &change : changes) {
		if (change.kind == CatalogChange::Kind::ADD) {
			served_.push_back(change.catalog);
			served_set_.insert(change.catalog);
			failed_.erase(change.catalog);
			auto unresolved = unresolved_.find(change.catalog);
			if (unresolved != unresolved_.end()) {
				for (auto &open : unresolved->second) {
					BindSession(open.ns, change.catalog, open.capabilities);
				}
				unresolved->second.clear();
			}
			listed_changed = true;
			continue;
		}
		if (change.kind == CatalogChange::Kind::RESOLVED) {
			SettleResolved(change.catalog, change.message);
			continue;
		}
		if (change.kind == CatalogChange::Kind::UPSTREAM_ADD) {
			ApplyUpstreamAdd(change.upstream);
			continue;
		}
		if (change.kind == CatalogChange::Kind::UPSTREAM_OWNER) {
			ApplyUpstreamOwner(change.catalog, change.previous_upstream_id, change.upstream_id);
			listed_changed = true;
			continue;
		}
		if (change.kind == CatalogChange::Kind::UPSTREAM_GONE) {
			ApplyUpstreamGone(change.upstream_id, change.message);
			listed_changed = true;
			continue;
		}
		if (change.kind == CatalogChange::Kind::FORWARD_DROP) {
			auto fwd = forwarded_.find(change.ns);
			if (fwd != forwarded_.end() && fwd->second.up->id == change.upstream_id) {
				auto &up = *fwd->second.up;
				{
					std::lock_guard<std::mutex> lk(up.track_mu);
					up.forwarded.erase(change.ns);
				}
				forwarded_.erase(fwd);
			}
			continue;
		}
		if (change.kind == CatalogChange::Kind::REMOVE) {
			ApplyRemove(change.catalog);
			listed_changed = true;
			continue;
		}
		if (change.kind == CatalogChange::Kind::LAZY_ADD) {
			lazy_.push_back(change.catalog);
			listed_changed = true;
			continue;
		}
		if (change.kind == CatalogChange::Kind::LAZY_REMOVE) {
			lazy_.erase(std::find(lazy_.begin(), lazy_.end(), change.catalog));
			listed_changed = true;
			continue;
		}
		if (change.kind == CatalogChange::Kind::PAUSE) {
			held_.emplace(change.catalog, std::vector<HeldReq> {});
			std::lock_guard<std::mutex> lk(resolve_mu_);
			pause_applied_.insert(change.catalog);
			continue;
		}
		if (change.kind == CatalogChange::Kind::RESUME) {
			auto held = held_.find(change.catalog);
			D_ASSERT(held != held_.end());
			auto reqs = std::move(held->second);
			held_.erase(held);
			held_count_ -= reqs.size();
			{
				std::lock_guard<std::mutex> lk(resolve_mu_);
				pause_applied_.erase(change.catalog);
			}
			for (auto &req : reqs) {
				AdmitAndQueueRequest(req.sess, req.req_id, req.op, std::move(req.body));
			}
			continue;
		}
		const bool ok = change.kind == CatalogChange::Kind::READY;
		auto it = pending_.find(change.catalog);
		D_ASSERT(it != pending_.end());
		auto parked = std::move(it->second.parked);
		pending_.erase(it);
		pending_order_.erase(std::find(pending_order_.begin(), pending_order_.end(), change.catalog));
		if (ok) {
			served_.push_back(change.catalog);
			served_set_.insert(change.catalog);
			for (auto &open : parked) {
				BindSession(open.ns, change.catalog, open.capabilities);
			}
		} else {
			failed_[change.catalog] = change.message;
			for (auto &open : parked) {
				SendFrame(PackOpenErr(open.ns, "IOException",
				                      "catalog \"" + change.catalog + "\" failed to open: " + change.message));
			}
			listed_changed = true;
		}
	}
	if (listed_changed) {
		SendFrame(PackPushCatalogsChanged(ListedCatalogs()));
	}
}

void ServeReactor::SettleResolved(const std::string &catalog, const std::string &error) {
	auto it = unresolved_.find(catalog);
	D_ASSERT(it != unresolved_.end());
	auto parked = std::move(it->second);
	unresolved_.erase(it);
	// An ADD or upstream that serves `catalog` has already drained `parked`.
	for (auto &open : parked) {
		if (error.empty()) {
			SendFrame(PackOpenErr(open.ns, "InvalidInputException", "catalog \"" + catalog + "\" not found"));
		} else {
			SendFrame(PackOpenErr(open.ns, "IOException", "catalog \"" + catalog + "\" failed to resolve: " + error));
		}
	}
}

void ServeReactor::ApplyUpstreamAdd(const std::shared_ptr<Upstream> &up) {
	upstreams_[up->id] = up;
	up->writer = std::thread(&ServeReactor::UpstreamWriterLoop, this, std::ref(*up));
	up->reader = std::thread(&ServeReactor::UpstreamReaderLoop, this, std::ref(*up));
}

void ServeReactor::ApplyUpstreamOwner(const std::string &catalog, int previous, int owner) {
	if (previous >= 0 && upstream_catalogs_.erase(catalog) != 0) {
		upstream_order_.erase(std::find(upstream_order_.begin(), upstream_order_.end(), catalog));
		auto loser = upstreams_.find(previous);
		if (loser != upstreams_.end()) {
			EvictForwarded(*loser->second, catalog,
			               "catalog \"" + catalog + "\" is now served by another source on this connection");
		}
	}
	if (owner < 0) {
		return;
	}
	auto winner = upstreams_.find(owner);
	if (winner == upstreams_.end()) {
		return;
	}
	upstream_catalogs_[catalog] = owner;
	upstream_order_.push_back(catalog);
	ForwardUnresolved(*winner->second, catalog);
}

void ServeReactor::EvictForwarded(Upstream &up, const std::string &catalog, const std::string &reason) {
	for (auto fwd = forwarded_.begin(); fwd != forwarded_.end();) {
		if (fwd->second.up != &up || fwd->second.catalog != catalog) {
			++fwd;
			continue;
		}
		const auto ns = fwd->first;
		std::vector<uint32_t> inflight;
		bool opening;
		{
			std::lock_guard<std::mutex> lk(up.track_mu);
			opening = up.opening.erase(ns) != 0;
			up.forwarded.erase(ns);
			for (auto key = up.inflight.begin(); key != up.inflight.end();) {
				if (key->ns == ns) {
					inflight.push_back(key->req_id);
					key = up.inflight.erase(key);
				} else {
					++key;
				}
			}
		}
		if (opening) {
			SendFrame(PackOpenErr(ns, "InvalidInputException", reason));
		}
		for (auto req_id : inflight) {
			SendError(*this, FrameTarget {ns, req_id}, "InvalidInputException", reason);
		}
		up.Send(PackCloseSession(ns));
		fwd = forwarded_.erase(fwd);
	}
}

void ServeReactor::ApplyUpstreamGone(int id, const std::string &reason_in) {
	auto it = upstreams_.find(id);
	if (it == upstreams_.end()) {
		return;
	}
	auto up = it->second;
	upstreams_.erase(it);
	StopUpstream(*up);
	std::string reason = reason_in;
	std::unordered_set<int64_t> opening;
	std::unordered_set<InflightKey, InflightKeyHash> inflight;
	{
		std::lock_guard<std::mutex> lk(up->track_mu);
		if (reason.empty()) {
			reason = up->reason;
		}
		opening.swap(up->opening);
		inflight.swap(up->inflight);
	}
	{
		std::lock_guard<std::mutex> lk(resolve_mu_);
		auto &status = upstream_status_[id];
		if (status.open) {
			status = UpstreamStatus {false, reason};
		}
	}
	// Its names are re-decided by the UPSTREAM_OWNERs queued after this; until they apply, nothing serves them.
	for (auto name = upstream_catalogs_.begin(); name != upstream_catalogs_.end();) {
		if (name->second != id) {
			++name;
			continue;
		}
		upstream_order_.erase(std::find(upstream_order_.begin(), upstream_order_.end(), name->first));
		name = upstream_catalogs_.erase(name);
	}
	for (auto fwd = forwarded_.begin(); fwd != forwarded_.end();) {
		if (fwd->second.up != up.get()) {
			++fwd;
			continue;
		}
		const auto ns = fwd->first;
		const std::string message = "upstream for catalog \"" + fwd->second.catalog + "\" closed: " + reason;
		if (opening.count(ns) != 0) {
			SendFrame(PackOpenErr(ns, "IOException", message));
		}
		for (auto &key : inflight) {
			if (key.ns == ns) {
				SendError(*this, FrameTarget {ns, key.req_id}, "IOException", message);
			}
		}
		fwd = forwarded_.erase(fwd);
	}
}

void ServeReactor::StopUpstream(Upstream &up) {
	up.stop.store(true);
	{
		std::lock_guard<std::mutex> lk(up.out_mu);
		up.out_stop = true;
	}
	up.out_cv.notify_all();
	if (up.writer.joinable()) {
		up.writer.join();
	}
	if (up.reader.joinable()) {
		up.reader.join();
	}
	up.conn.ShutdownWrite();
}

void ServeReactor::UpstreamWriterLoop(Upstream &up) {
	while (true) {
		std::string frame;
		{
			std::unique_lock<std::mutex> lk(up.out_mu);
			up.out_cv.wait(lk, [&] { return !up.out_q.empty() || up.out_stop; });
			if (up.out_stop) {
				return;
			}
			frame = std::move(up.out_q.front());
			up.out_q.pop_front();
		}
		try {
			up.conn.WriteFrame(StreamFrameType::N6K, frame);
		} catch (const std::exception &e) {
			{
				std::lock_guard<std::mutex> lk(up.track_mu);
				if (up.reason.empty()) {
					up.reason = std::string("write failed: ") + e.what();
				}
			}
			up.broken.store(true);
			return;
		}
	}
}

// Liveness is any frame at all, not just PONG: a PONG queued behind a long stream would otherwise read
// as a dead upstream.
void ServeReactor::UpstreamReaderLoop(Upstream &up) {
	using Clock = std::chrono::steady_clock;
	auto last_heard = Clock::now();
	auto last_ping = last_heard;
	uint32_t ping_id = 0;
	std::string reason;
	std::string frame;
	StreamFrameType frame_type;
	try {
		while (!up.stop.load() && !stopping_.load() && !up.broken.load()) {
			const auto now = Clock::now();
			if (ping_interval_ms_ > 0) {
				const auto quiet = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heard).count();
				if (quiet > ping_interval_ms_ + OPEN_PENDING_STALL_MS) {
					reason = "ping timeout: nothing received for " + std::to_string(quiet) + "ms";
					break;
				}
				if (quiet >= ping_interval_ms_ && now - last_ping >= std::chrono::milliseconds(ping_interval_ms_)) {
					last_ping = now;
					up.Send(PackPing(++ping_id));
				}
			}
			if (!up.conn.WaitReadable(250)) {
				continue;
			}
			if (!up.conn.ReadFrame(frame, frame_type)) {
				reason = "upstream closed the connection";
				break;
			}
			last_heard = Clock::now();
			if (frame_type != StreamFrameType::N6K) {
				continue;
			}
			msgpack::object_handle oh;
			const char *raw_body;
			size_t raw_body_len;
			ParseFrame(frame, oh, raw_body, raw_body_len);
			const msgpack::object &h = oh.get();
			const auto type = static_cast<FrameType>(GetInt(h, "t", -1));
			const auto ns = GetInt(h, "ns", 0);
			if (ns != 0) {
				const auto req_id = static_cast<uint32_t>(GetInt(h, "id", 0));
				bool refused = false;
				{
					std::lock_guard<std::mutex> lk(up.track_mu);
					if (up.forwarded.count(ns) == 0) {
						continue;
					}
					if (type == FrameType::OPEN_ACK || type == FrameType::OPEN_ERR) {
						up.opening.erase(ns);
						refused = type == FrameType::OPEN_ERR;
						if (refused) {
							up.forwarded.erase(ns);
						}
					} else if (type == FrameType::RESP_END || type == FrameType::ERR) {
						up.inflight.erase(InflightKey {ns, req_id});
					}
				}
				// Queued before the OPEN_ERR goes out, so a retried OPEN on `ns` can't be read before the drop.
				if (refused) {
					std::lock_guard<std::mutex> lk(resolve_mu_);
					CatalogChange change {CatalogChange::Kind::FORWARD_DROP, "", ""};
					change.upstream_id = up.id;
					change.ns = ns;
					catalog_changes_.push_back(std::move(change));
				}
				SendFrame(frame);
				continue;
			}
			if (type == FrameType::PING) {
				up.Send(PackPong(static_cast<uint32_t>(GetInt(h, "id", 0))));
			} else if (type == FrameType::PUSH && GetInt(h, "op", -1) == OP_CATALOGS_CHANGED) {
				std::lock_guard<std::mutex> lk(resolve_mu_);
				ReportUpstreamCatalogs(up.id, GetStrArray(h, "catalogs"));
			}
		}
	} catch (const std::exception &e) {
		reason = std::string("upstream connection failed: ") + e.what();
	}
	if (up.stop.load() || stopping_.load()) {
		return;
	}
	{
		std::lock_guard<std::mutex> lk(up.track_mu);
		if (up.reason.empty()) {
			up.reason = reason;
		}
		reason = up.reason;
	}
	// Recorded here, before the socket is shut, so a host that sees it close reads the reason at once.
	std::lock_guard<std::mutex> lk(resolve_mu_);
	auto &status = upstream_status_[up.id];
	if (status.open) {
		status = UpstreamStatus {false, reason};
	}
	CatalogChange change {CatalogChange::Kind::UPSTREAM_GONE, "", reason};
	change.upstream_id = up.id;
	catalog_changes_.push_back(std::move(change));
	DropUpstreamClaim(up.id);
}

void ServeReactor::ApplyRemove(const std::string &catalog) {
	const std::string reason = "catalog \"" + catalog + "\" was removed from this connection";
	auto pending = pending_.find(catalog);
	if (pending != pending_.end()) {
		for (auto &open : pending->second.parked) {
			SendFrame(PackOpenErr(open.ns, "InvalidInputException", reason));
		}
		pending_.erase(pending);
		pending_order_.erase(std::find(pending_order_.begin(), pending_order_.end(), catalog));
		return;
	}
	D_ASSERT(served_set_.count(catalog) == 1);
	served_set_.erase(catalog);
	served_.erase(std::find(served_.begin(), served_.end(), catalog));
	std::vector<int64_t> bound;
	for (auto &entry : sessions_) {
		if (entry.second == catalog) {
			bound.push_back(entry.first);
		}
	}
	for (auto ns : bound) {
		EvictSession(ns, reason);
	}
}

void ServeReactor::EvictSession(int64_t ns, const std::string &reason) {
	std::vector<std::pair<std::shared_ptr<RequestSlot>, shared_ptr<ClientContext>>> inflight;
	{
		std::lock_guard<std::mutex> lk(map_mu_);
		for (auto &entry : inflight_) {
			if (entry.first.ns != ns) {
				continue;
			}
			std::lock_guard<std::mutex> slk(entry.second->mu);
			inflight.emplace_back(entry.second, entry.second->context);
		}
	}
	// Ended now, not when each worker notices: a worker may be stuck in a long query or a host call.
	for (auto &req : inflight) {
		EndRequest(*req.first, PackErr(FrameTarget {ns, req.first->req_id}, "InvalidInputException", reason, false));
	}
	for (auto &req : TakeHeldOf(ns)) {
		SendError(*this, FrameTarget {ns, req.req_id}, "InvalidInputException", reason);
	}
	CloseSessionAndCancelItsRequests(ns);
	for (auto &req : inflight) {
		if (req.second) {
			req.second->Interrupt();
		}
	}
}

void ServeReactor::SendDuePendingBeats() {
	const auto now = std::chrono::steady_clock::now();
	if (now < next_pending_beat_) {
		return;
	}
	next_pending_beat_ = now + std::chrono::milliseconds(open_pending_interval_ms_);
	for (auto &entry : pending_) {
		for (auto &open : entry.second.parked) {
			SendFrame(PackOpenPending(open.ns, entry.second.message));
		}
	}
	for (auto &entry : unresolved_) {
		for (auto &open : entry.second) {
			SendFrame(PackOpenPending(open.ns, ResolvingMessage(entry.first)));
		}
	}
}

void ServeReactor::TrackSession(int64_t ns, const std::string &catalog) {
	std::lock_guard<std::mutex> lk(push_mu_);
	push_sessions_[ns] = catalog;
}

void ServeReactor::AdmitPlanCatalog(RequestSlot &slot, const std::string &catalog) {
	if (StringUtil::CIEquals(catalog, slot.catalog)) {
		return;
	}
	std::string opened;
	{
		std::lock_guard<std::mutex> lk(push_mu_);
		for (auto &entry : push_sessions_) {
			if (StringUtil::CIEquals(entry.second, catalog)) {
				opened = entry.second;
				break;
			}
		}
	}
	if (opened.empty()) {
		throw CatalogException("n6k_server: the plan names catalog \"%s\", which no session on this connection "
		                       "has open",
		                       catalog);
	}
	// Counted before the pause is checked: a pause that lands in between then waits for this request.
	{
		std::lock_guard<std::mutex> lk(map_mu_);
		slot.plan_catalogs.insert(opened);
	}
	std::lock_guard<std::mutex> lk(resolve_mu_);
	if (paused_.count(opened) != 0) {
		throw TransactionException("n6k_server: catalog \"%s\" is paused; retry", opened);
	}
}

void ServeReactor::UntrackSession(int64_t ns) {
	std::lock_guard<std::mutex> lk(push_mu_);
	push_sessions_.erase(ns);
}

int ServeReactor::NotifyInvalidate(const std::string &catalog, const std::vector<std::string> &schemas) {
	if (schemas.empty()) {
		return 0;
	}
	std::vector<int64_t> targets;
	{
		std::lock_guard<std::mutex> lk(push_mu_);
		for (auto &entry : push_sessions_) {
			if (entry.second == catalog) {
				targets.push_back(entry.first);
			}
		}
	}
	// Enqueued, not written: the writer thread serialises, so a push never races a response mid-frame
	// and never blocks the caller on a slow socket.
	for (auto ns : targets) {
		SendFrame(PackPushInvalidate(ns, schemas));
	}
	return static_cast<int>(targets.size());
}

int64_t ServeReactor::BeginTxn(RequestSlot &slot) {
	auto pinned = std::make_shared<PinnedTxn>();
	pinned->conn = std::unique_ptr<Connection>(new Connection(db_));
	MarkServed(*pinned->conn->context);
	pinned->conn->BeginTransaction();
	{
		// Checked under txn_mu_: a session close sets `cancel` before DropTxnsOf takes it.
		std::lock_guard<std::mutex> lk(txn_mu_);
		if (!slot.cancel.load()) {
			auto txn = next_txn_++;
			txns_[TxnKey {slot.ns, txn}] = std::move(pinned);
			return txn;
		}
	}
	pinned->conn->Rollback();
	throw InterruptException();
}

std::shared_ptr<PinnedTxn> ServeReactor::FindTxn(int64_t ns, int64_t txn) {
	std::lock_guard<std::mutex> lk(txn_mu_);
	auto it = txns_.find(TxnKey {ns, txn});
	return it == txns_.end() ? nullptr : it->second;
}

std::shared_ptr<PinnedTxn> ServeReactor::TakeTxn(int64_t ns, int64_t txn) {
	std::lock_guard<std::mutex> lk(txn_mu_);
	auto it = txns_.find(TxnKey {ns, txn});
	if (it == txns_.end()) {
		return nullptr;
	}
	auto pinned = std::move(it->second);
	txns_.erase(it);
	return pinned;
}

void ServeReactor::DropTxnsOf(int64_t ns) {
	std::vector<std::shared_ptr<PinnedTxn>> dropped;
	{
		std::lock_guard<std::mutex> lk(txn_mu_);
		for (auto it = txns_.begin(); it != txns_.end();) {
			if (it->first.ns == ns) {
				dropped.push_back(std::move(it->second));
				it = txns_.erase(it);
			} else {
				++it;
			}
		}
	}
	// Out of `txns_`, nothing but the destructor touches `conn`, so reading it unlocked is safe.
	for (auto &pinned : dropped) {
		pinned->dropped.store(true);
		if (pinned->conn) {
			pinned->conn->Interrupt();
		}
	}
}

void ServeReactor::DropAllTxns() {
	std::vector<std::shared_ptr<PinnedTxn>> dropped;
	{
		std::lock_guard<std::mutex> lk(txn_mu_);
		for (auto &entry : txns_) {
			dropped.push_back(std::move(entry.second));
		}
		txns_.clear();
	}
	for (auto &pinned : dropped) {
		std::lock_guard<std::mutex> plk(pinned->mu);
		pinned->conn.reset();
	}
}

void ServeReactor::CloseSessionAndCancelItsRequests(int64_t ns) {
	auto is_ns = [ns](const ParkedOpen &open) {
		return open.ns == ns;
	};
	for (auto &entry : pending_) {
		auto &parked = entry.second.parked;
		parked.erase(std::remove_if(parked.begin(), parked.end(), is_ns), parked.end());
	}
	for (auto &entry : unresolved_) {
		entry.second.erase(std::remove_if(entry.second.begin(), entry.second.end(), is_ns), entry.second.end());
	}
	if (sessions_.erase(ns) == 0) {
		return;
	}
	TakeHeldOf(ns);
	UntrackSession(ns);
	{
		std::lock_guard<std::mutex> lk(map_mu_);
		for (auto &entry : inflight_) {
			if (entry.first.ns != ns) {
				continue;
			}
			{
				std::lock_guard<std::mutex> slk(entry.second->mu);
				entry.second->cancel.store(true);
			}
			QueueParked(entry.second, RequestTask::Kind::CANCEL);
		}
	}
	DropTxnsOf(ns);
}

void ServeReactor::ExpireIdle() {
	if (options_.idle_stream_timeout_ms > 0) {
		const auto cutoff =
		    std::chrono::steady_clock::now() - std::chrono::milliseconds(options_.idle_stream_timeout_ms);
		std::vector<std::shared_ptr<RequestSlot>> idle;
		{
			std::lock_guard<std::mutex> lk(map_mu_);
			for (auto &entry : inflight_) {
				std::lock_guard<std::mutex> slk(entry.second->mu);
				if (entry.second->parked && entry.second->parked_at < cutoff) {
					idle.push_back(entry.second);
				}
			}
		}
		for (auto &slot : idle) {
			EndRequest(*slot, PackErr(FrameTarget {slot->ns, slot->req_id}, "IOException",
			                          "n6k_server: stream idle for more than " +
			                              std::to_string(options_.idle_stream_timeout_ms) + " ms without credit",
			                          false));
			QueueParked(slot, RequestTask::Kind::CANCEL);
		}
	}
	if (options_.idle_txn_timeout_ms > 0) {
		const auto cutoff = SteadyNowMs() - options_.idle_txn_timeout_ms;
		// Taken out of `txns_` with `mu` held, so a request that found it first sees it closed.
		std::vector<std::shared_ptr<PinnedTxn>> expired;
		{
			std::lock_guard<std::mutex> lk(txn_mu_);
			for (auto it = txns_.begin(); it != txns_.end();) {
				if (it->second->last_used_ms.load() < cutoff && it->second->mu.try_lock()) {
					expired.push_back(std::move(it->second));
					it = txns_.erase(it);
				} else {
					++it;
				}
			}
		}
		for (auto &pinned : expired) {
			std::unique_lock<std::mutex> guard(pinned->mu, std::adopt_lock);
			if (!pinned->conn) {
				continue;
			}
			try {
				pinned->conn->Rollback();
			} catch (const std::exception &e) {
				DUCKDB_LOG_WARNING(context_, "n6k_server: rolling back an idle transaction: %s",
				                   ErrorData(e).Message());
			}
			pinned->conn.reset();
		}
	}
}

void ServeReactor::InterruptWorkers() {
	std::lock_guard<std::mutex> lk(map_mu_);
	for (auto &entry : inflight_) {
		shared_ptr<ClientContext> ctx;
		{
			std::lock_guard<std::mutex> slk(entry.second->mu);
			ctx = entry.second->context;
		}
		if (ctx) {
			ctx->Interrupt();
		}
	}
}

ServeReactor::CreditResult ServeReactor::TryAcquireCredit(RequestSlot &slot) {
	std::lock_guard<std::mutex> lk(slot.mu);
	if (slot.cancel.load() || stopping_.load()) {
		return CreditResult::STOPPED;
	}
	if (slot.credits <= 0) {
		credit_pauses_.fetch_add(1);
		return CreditResult::EMPTY;
	}
	slot.credits--;
	return CreditResult::TAKEN;
}

void ServeReactor::AddCredits(const std::shared_ptr<RequestSlot> &slot, int n) {
	{
		std::lock_guard<std::mutex> lk(slot->mu);
		slot->credits += n;
	}
	QueueParked(slot, RequestTask::Kind::RESUME);
}

void ServeReactor::WriterLoop() {
	const auto ping_interval = std::chrono::milliseconds(ping_interval_ms_);
	uint32_t ping_id = 0;
	bool ping_armed = false;
	std::chrono::steady_clock::time_point next_ping;
	while (true) {
		OutFrame frame;
		{
			std::unique_lock<std::mutex> lk(out_mu_);
			while (out_q_.empty() && !writer_stop_) {
				if (!pinging_) {
					out_cv_.wait(lk);
					continue;
				}
				const auto now = std::chrono::steady_clock::now();
				if (!ping_armed) {
					next_ping = now + ping_interval;
					ping_armed = true;
				}
				if (now < next_ping) {
					out_cv_.wait_until(lk, next_ping);
					continue;
				}
				next_ping = now + ping_interval;
				if (!stopping_.load()) {
					ping_id++;
					EnqueueOut(OutFrame {StreamFrameType::N6K, PackPing(ping_id), 0});
				}
			}
			if (out_q_.empty()) {
				return;
			}
			frame = std::move(out_q_.front());
			out_q_.pop_front();
			out_q_bytes_ -= frame.payload.size();
		}
		try {
			if (frame.type == StreamFrameType::CLOSE) {
				conn_.SendClose(frame.close_code, frame.payload);
				stopping_.store(true);
				return;
			}
			conn_.WriteFrame(frame.type, frame.payload);
			frames_out_.fetch_add(1, std::memory_order_relaxed);
			bytes_out_.fetch_add(static_cast<int64_t>(frame.payload.size()), std::memory_order_relaxed);
		} catch (const std::exception &) {
			// Peer is gone; the reader will hit EOF and shut down.
			stopping_.store(true);
			return;
		}
	}
}

namespace {

// Live reactors keyed by the DatabaseInstance they serve. Global because "who is currently serving
// this database" is a process-scoped question — several serves can run at once, and one serve can
// hold one reactor per client.
std::mutex &RegistryMutex() {
	static std::mutex mu;
	return mu;
}

std::vector<std::pair<DatabaseInstance *, ServeReactor *>> &RegistryEntries() {
	static std::vector<std::pair<DatabaseInstance *, ServeReactor *>> entries;
	return entries;
}

// Signalled on every registration and session release, for AwaitServe racing a serve that has not reached Run().
std::condition_variable &RegistryCv() {
	static std::condition_variable cv;
	return cv;
}

struct ServeSession {
	DatabaseInstance *db;
	int fd;
	bool claimed;
};

// Guarded by RegistryMutex.
std::unordered_map<hugeint_t, ServeSession> &SessionEntries() {
	static std::unordered_map<hugeint_t, ServeSession> entries;
	return entries;
}

} // namespace

ServeReactorRegistration::ServeReactorRegistration(DatabaseInstance &db, ServeReactor &reactor) : reactor_(&reactor) {
	{
		std::lock_guard<std::mutex> lk(RegistryMutex());
		RegistryEntries().emplace_back(&db, &reactor);
	}
	RegistryCv().notify_all();
}

void ServeReactorRegistration::Release() {
	if (!reactor_) {
		return;
	}
	std::lock_guard<std::mutex> lk(RegistryMutex());
	auto &entries = RegistryEntries();
	for (auto it = entries.begin(); it != entries.end(); ++it) {
		if (it->second == reactor_) {
			entries.erase(it);
			break;
		}
	}
	reactor_ = nullptr;
}

ServeReactorRegistration::~ServeReactorRegistration() {
	Release();
}

hugeint_t CreateServeSession(DatabaseInstance &db, int fd) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	auto &entries = SessionEntries();
	hugeint_t id;
	do {
		id = UUID::GenerateRandomUUID();
	} while (entries.count(id));
	entries.emplace(id, ServeSession {&db, fd, false});
	return id;
}

ServeSessionClaim::ServeSessionClaim(DatabaseInstance &db, hugeint_t id) : id_(id) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	auto it = SessionEntries().find(id);
	if (it == SessionEntries().end() || it->second.db != &db) {
		throw InvalidInputException("no n6k serve session %s", UUID::ToString(id));
	}
	if (it->second.claimed) {
		throw InvalidInputException("n6k serve session %s is already being served", UUID::ToString(id));
	}
	it->second.claimed = true;
	fd_ = it->second.fd;
}

ServeSessionClaim::~ServeSessionClaim() {
	{
		std::lock_guard<std::mutex> lk(RegistryMutex());
		SessionEntries().erase(id_);
	}
	RegistryCv().notify_all();
}

int BroadcastInvalidate(DatabaseInstance &db, const std::string &catalog, const std::vector<std::string> &schemas) {
	// Lock held across NotifyInvalidate so a reactor cannot unregister and be destroyed between being
	// found and being used. Safe to hold: NotifyInvalidate only enqueues, so it cannot block on the
	// socket or re-enter here.
	std::lock_guard<std::mutex> lk(RegistryMutex());
	int notified = 0;
	for (auto &entry : RegistryEntries()) {
		if (entry.first == &db) {
			notified += entry.second->NotifyInvalidate(catalog, schemas);
		}
	}
	return notified;
}

// Callers hold `lk` across their use of the result for the same reason as BroadcastInvalidate; no
// ServeReactor method they call blocks on a socket or re-enters the registry. An unknown id throws at
// once; a known one waits for its serve to register, or gives up when that serve ends first or after
// RESOLVE_REGISTER_WAIT_MS.
static ServeReactor &AwaitServe(std::unique_lock<std::mutex> &lk, DatabaseInstance &db, hugeint_t session_id) {
	auto &sessions = SessionEntries();
	ServeReactor *reactor = nullptr;
	auto live = [&] {
		auto it = sessions.find(session_id);
		return it != sessions.end() && it->second.db == &db;
	};
	if (live()) {
		RegistryCv().wait_for(lk, std::chrono::milliseconds(RESOLVE_REGISTER_WAIT_MS), [&] {
			if (!live()) {
				return true;
			}
			for (auto &entry : RegistryEntries()) {
				if (entry.first == &db && entry.second->SessionId() == session_id) {
					reactor = entry.second;
					return true;
				}
			}
			return false;
		});
	}
	if (reactor == nullptr) {
		throw InvalidInputException("no n6k serve session %s", UUID::ToString(session_id));
	}
	return *reactor;
}

void ResolvePendingOpen(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog, bool ok,
                        const std::string &message) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).ResolvePending(catalog, ok, message);
}

void AddServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).AddCatalog(catalog);
}

void AddServedLazyCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog, int64_t rank) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).AddLazyCatalog(catalog, rank);
}

void RemoveServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).RemoveCatalog(catalog);
}

void SendServedText(DatabaseInstance &db, hugeint_t session_id, const std::string &text) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).SendText(text);
}

void CloseServed(DatabaseInstance &db, hugeint_t session_id, int code, const std::string &reason) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).Close(code, reason);
}

std::vector<std::string> AddServedUpstream(DatabaseInstance &db, hugeint_t session_id, int up_fd,
                                           const std::vector<std::string> &catalogs, int64_t rank) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	return AwaitServe(lk, db, session_id).AddUpstream(up_fd, catalogs, rank);
}

std::vector<std::string> SetServedUpstreamRank(DatabaseInstance &db, hugeint_t session_id, int up_fd, int64_t rank) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	return AwaitServe(lk, db, session_id).SetUpstreamRank(up_fd, rank);
}

void RemoveServedUpstream(DatabaseInstance &db, hugeint_t session_id, int up_fd) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).RemoveUpstream(up_fd);
}

UpstreamStatus ServedUpstreamStatus(DatabaseInstance &db, hugeint_t session_id, int up_fd) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	return AwaitServe(lk, db, session_id).GetUpstreamStatus(up_fd);
}

void PauseServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).Pause(catalog);
}

void ResumeServedCatalog(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	AwaitServe(lk, db, session_id).Resume(catalog);
}

InflightState ServedCatalogInflight(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	return AwaitServe(lk, db, session_id).Inflight(catalog);
}

int64_t CancelServedCatalogInflight(DatabaseInstance &db, hugeint_t session_id, const std::string &catalog) {
	std::unique_lock<std::mutex> lk(RegistryMutex());
	return AwaitServe(lk, db, session_id).CancelInflight(catalog);
}

SessionStats ServeReactor::Stats() const {
	return SessionStats {
	    frames_in_.load(),
	    bytes_in_.load(),
	    frames_out_.load(),
	    bytes_out_.load(),
	    reqs_handled_.load(),
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_).count(),
	    authenticate_ms_,
	    peak_workers_.load()};
}

SessionStats ServedSessionStats(DatabaseInstance &db, hugeint_t session_id) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	for (auto &entry : RegistryEntries()) {
		if (entry.first == &db && entry.second->SessionId() == session_id) {
			return entry.second->Stats();
		}
	}
	throw InvalidInputException("no n6k serve session %s", UUID::ToString(session_id));
}

ServeStats CollectServeStats(DatabaseInstance &db) {
	std::lock_guard<std::mutex> lk(RegistryMutex());
	ServeStats stats;
	for (auto &entry : RegistryEntries()) {
		if (entry.first != &db) {
			continue;
		}
		stats.connections++;
		stats.requests_handled += entry.second->RequestsHandled();
		stats.credit_pauses += entry.second->CreditPauses();
		stats.cancels += entry.second->Cancels();
	}
	return stats;
}

void ServeReactor::SetLazyCatalogs(const std::vector<std::pair<std::string, int64_t>> &names) {
	for (auto &entry : names) {
		lazy_.push_back(entry.first);
		lazy_claims_[entry.first] = Claim {entry.second, next_claim_seq_++};
	}
}

static Value VarcharList(const std::vector<std::string> &items) {
	vector<Value> values;
	for (auto &item : items) {
		values.emplace_back(item);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(values));
}

void ServeReactor::Reject(const std::string &reason, int code) {
	SendFrame(PackHelloErr("InvalidInputException", reason));
	Close(code, reason);
}

void ServeReactor::ApplyAuthResult(const AuthResult &result) {
	if (!result.lazy.empty() && !resolve_unknown_) {
		throw InvalidInputException("n6k_server: authenticate returned lazy catalogs, but the serve has no resolve "
		                            "callback");
	}
	if (result.has_catalogs) {
		Connection conn(db_);
		auto attached = conn.Prepare("SELECT count(*) FROM duckdb_databases() WHERE database_name = ?");
		if (attached->HasError()) {
			attached->GetErrorObject().Throw();
		}
		for (auto &name : result.catalogs) {
			vector<Value> params {Value(name)};
			auto found = attached->Execute(params);
			if (found->HasError()) {
				found->ThrowError();
			}
			if (found->Fetch()->GetValue(0, 0).GetValue<int64_t>() == 0) {
				throw InvalidInputException("n6k_server: authenticate returned catalog \"%s\", which is not attached",
				                            name);
			}
		}
		served_ = result.catalogs;
		served_set_ = std::unordered_set<std::string>(served_.begin(), served_.end());
	}
	std::lock_guard<std::mutex> lk(resolve_mu_);
	listed_ = std::unordered_set<std::string>(served_.begin(), served_.end());
	for (auto &spec : result.pending) {
		if (!listed_.insert(spec.catalog).second) {
			throw InvalidInputException("n6k_server: authenticate: catalog \"%s\" is named twice", spec.catalog);
		}
		pending_order_.push_back(spec.catalog);
		resolvable_.insert(spec.catalog);
		pending_[spec.catalog] = PendingOpen {spec.message, {}};
	}
	for (auto &entry : result.lazy) {
		if (listed_.count(entry.first) != 0) {
			throw InvalidInputException("n6k_server: authenticate: catalog \"%s\" is named twice", entry.first);
		}
	}
	SetLazyCatalogs(result.lazy);
}

bool ServeReactor::Authenticate(const HelloRequest &hello) {
	static constexpr const char *INTERNAL_ERROR = "internal error";
	static constexpr int INTERNAL_ERROR_CLOSE_CODE = 1011;
	AuthResult result;
	const auto auth_started = std::chrono::steady_clock::now();
	if (!options_.authenticate.empty()) {
		std::mutex mu;
		std::condition_variable cv;
		bool done = false;
		std::string json;
		std::string error;
		std::thread caller([&] {
			try {
				HostCallback authenticate(db_, options_.authenticate, 4);
				auto value =
				    authenticate.Call({Value::UUID(session_id_), Value(hello.token), VarcharList(hello.capabilities),
				                       options_.has_request ? Value(options_.request) : Value(LogicalType::VARCHAR)});
				if (value.IsNull()) {
					throw InvalidInputException("n6k_server: authenticate returned NULL");
				}
				json = value.GetValue<string>();
			} catch (const std::exception &e) {
				error = ErrorData(e).Message();
			}
			{
				std::lock_guard<std::mutex> lk(mu);
				done = true;
			}
			cv.notify_all();
		});
		auto next_beat = std::chrono::steady_clock::now() + std::chrono::milliseconds(options_.hello_pending_first_ms);
		{
			std::unique_lock<std::mutex> lk(mu);
			while (!cv.wait_until(lk, next_beat, [&] { return done; })) {
				SendFrame(PackHelloPending(options_.hello_pending_message));
				next_beat = std::chrono::steady_clock::now() + std::chrono::milliseconds(open_pending_interval_ms_);
			}
		}
		caller.join();
		authenticate_ms_ =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - auth_started)
		        .count();
		if (!error.empty()) {
			DUCKDB_LOG_WARNING(context_, "n6k_server: authenticate failed: %s", error);
			Reject(INTERNAL_ERROR, INTERNAL_ERROR_CLOSE_CODE);
			return false;
		}
		try {
			result = ParseAuthResult(json);
		} catch (const std::exception &e) {
			DUCKDB_LOG_WARNING(context_, "%s", ErrorData(e).Message());
			Reject(INTERNAL_ERROR, INTERNAL_ERROR_CLOSE_CODE);
			return false;
		}
	}
	if (result.reject) {
		Reject(result.reason, result.code);
		return false;
	}
	try {
		ApplyAuthResult(result);
	} catch (const std::exception &e) {
		DUCKDB_LOG_WARNING(context_, "%s", ErrorData(e).Message());
		Reject(INTERNAL_ERROR, INTERNAL_ERROR_CLOSE_CODE);
		return false;
	}
	app_caps_ = result.app_capabilities;
	for (auto &cap : IntersectCaps(hello.capabilities, APP_HELLO_CAPS)) {
		if (app_caps_.empty() || std::find(app_caps_.begin(), app_caps_.end(), cap) != app_caps_.end()) {
			accepts_text_ = true;
		}
	}
	SendFrame(PackHelloAck(GrantedHelloCaps(hello.capabilities, app_caps_)));
	return true;
}

void ServeReactor::TextWorkerLoop() {
	unique_ptr<HostCallback> on_text;
	try {
		on_text = make_uniq<HostCallback>(db_, options_.on_text, 2);
	} catch (const std::exception &e) {
		DUCKDB_LOG_WARNING(context_, "%s", ErrorData(e).Message());
	}
	while (true) {
		std::string text;
		{
			std::unique_lock<std::mutex> lk(text_mu_);
			text_cv_.wait(lk, [&] { return !text_q_.empty() || text_stop_; });
			if (text_stop_) {
				return;
			}
			text = std::move(text_q_.front());
			text_q_.pop_front();
		}
		if (!on_text) {
			continue;
		}
		try {
			auto reply = on_text->Call({Value::UUID(session_id_), Value(std::move(text))});
			if (!reply.IsNull()) {
				auto message = reply.GetValue<string>();
				if (!message.empty()) {
					SendJsonRpc(std::move(message));
				}
			}
		} catch (const std::exception &e) {
			DUCKDB_LOG_WARNING(context_, "%s", ErrorData(e).Message());
		}
	}
}

void ServeReactor::StartResolve(const std::string &catalog) {
	{
		std::lock_guard<std::mutex> lk(resolve_q_mu_);
		resolve_q_.push_back(catalog);
	}
	resolve_q_cv_.notify_one();
	if (!resolver_.joinable()) {
		resolver_ = std::thread(&ServeReactor::ResolverLoop, this);
	}
}

void ServeReactor::ResolverLoop() {
	while (true) {
		std::string catalog;
		{
			std::unique_lock<std::mutex> lk(resolve_q_mu_);
			resolve_q_cv_.wait(lk, [&] { return !resolve_q_.empty() || resolve_stop_; });
			if (resolve_stop_) {
				return;
			}
			catalog = std::move(resolve_q_.front());
			resolve_q_.pop_front();
		}
		std::string error;
		try {
			HostCallback resolve(db_, options_.resolve, 2);
			auto result = resolve.Call({Value::UUID(session_id_), Value(catalog)});
			if (!result.IsNull()) {
				error = result.GetValue<string>();
			}
		} catch (const std::exception &e) {
			error = ErrorData(e).RawMessage();
		}
		try {
			Resolved(catalog, error);
		} catch (const std::exception &) {
		}
	}
}

void ServeReactor::StopCallbackThreads() {
	{
		std::lock_guard<std::mutex> lk(text_mu_);
		text_stop_ = true;
	}
	text_cv_.notify_all();
	if (text_worker_.joinable()) {
		text_worker_.join();
	}
	{
		std::lock_guard<std::mutex> lk(resolve_q_mu_);
		resolve_stop_ = true;
	}
	resolve_q_cv_.notify_all();
	if (resolver_.joinable()) {
		resolver_.join();
	}
}

int64_t ServeReactor::Run() {
	started_ = std::chrono::steady_clock::now();
	HelloRequest hello;
	try {
		hello = ReadHello(conn_);
	} catch (const std::exception &) {
		return 0;
	}
	writer_ = std::thread(&ServeReactor::WriterLoop, this);
	if (!Authenticate(hello)) {
		StopWriter();
		conn_.ShutdownWrite();
		return 0;
	}

	// After HELLO_ACK is queued, so nothing a host sends through the registry precedes it.
	ServeReactorRegistration registration(db_, *this);
	if (!options_.on_text.empty()) {
		text_worker_ = std::thread(&ServeReactor::TextWorkerLoop, this);
	}
	StartPinging();

	// Poll with a timeout so Ctrl-C (interrupt) ends the loop even on an idle peer.
	std::string frame;
	StreamFrameType frame_type;
	try {
		while (!stopping_.load()) {
			// Applied after the wait, so the frame read next sees changes queued while parked in it.
			const bool readable = conn_.WaitReadable(250);
			ApplyCatalogChanges();
			SendDuePendingBeats();
			ExpireIdle();
			if (!readable) {
				if (context_.IsInterrupted()) {
					break;
				}
				continue;
			}
			if (!conn_.ReadFrame(frame, frame_type)) {
				break;
			}
			frames_in_.fetch_add(1, std::memory_order_relaxed);
			bytes_in_.fetch_add(static_cast<int64_t>(frame.size()), std::memory_order_relaxed);
			if (frame_type == StreamFrameType::JSONRPC) {
				if (options_.on_text.empty()) {
					auto reply = JsonRpcMethodNotFoundReply(frame);
					if (reply.empty()) {
						DUCKDB_LOG_WARNING(context_, "n6k_server: discarding a JSON-RPC message (no JSON-RPC methods)");
					} else {
						SendJsonRpc(std::move(reply));
					}
				} else if (accepts_text_) {
					std::string busy;
					{
						std::lock_guard<std::mutex> lk(text_mu_);
						if (text_q_.size() < MAX_QUEUED_TEXT) {
							text_q_.push_back(std::move(frame));
						} else {
							busy = JsonRpcErrorReply(frame, -32000, "Server busy: too many unanswered messages");
						}
					}
					text_cv_.notify_one();
					if (!busy.empty()) {
						SendJsonRpc(std::move(busy));
					}
				}
				continue;
			}
			msgpack::object_handle oh;
			const char *raw_body;
			size_t raw_body_len;
			ParseFrame(frame, oh, raw_body, raw_body_len);
			const msgpack::object &h = oh.get();
			const auto type = static_cast<FrameType>(GetInt(h, "t", -1));
			const auto req_id = static_cast<uint32_t>(GetInt(h, "id", 0));
			const auto ns = GetInt(h, "ns", 0);
			if (ns != 0 && (type == FrameType::REQ || type == FrameType::CREDIT || type == FrameType::CANCEL)) {
				auto fwd = forwarded_.find(ns);
				if (fwd != forwarded_.end()) {
					auto &up = *fwd->second.up;
					if (type == FrameType::REQ) {
						std::lock_guard<std::mutex> lk(up.track_mu);
						up.inflight.insert(InflightKey {ns, req_id});
					} else if (type == FrameType::CANCEL && req_id == 0) {
						{
							std::lock_guard<std::mutex> lk(up.track_mu);
							up.opening.erase(ns);
							up.forwarded.erase(ns);
							for (auto key = up.inflight.begin(); key != up.inflight.end();) {
								key = key->ns == ns ? up.inflight.erase(key) : std::next(key);
							}
						}
						forwarded_.erase(fwd);
					}
					up.Send(frame);
					continue;
				}
			}
			switch (type) {
			case FrameType::REQ: {
				const auto op = static_cast<uint8_t>(GetInt(h, "op", 0xff));
				if (op == OP_CATALOG_LIST) {
					SendFrame(PackCatalogsChunk(req_id, ListedCatalogs()));
					SendFrame(PackRespEnd({0, req_id}));
					break;
				}
				const std::string *catalog = LookupSessionCatalog(ns);
				if (catalog == nullptr) {
					// Never guess: answering a SCAN from an arbitrary catalog returns wrong data with
					// no error.
					SendError(*this, FrameTarget {ns, req_id}, "InvalidInputException",
					          "unknown session ns=" + std::to_string(ns) + "; send FT_OPEN first");
					break;
				}
				// Reconstruct the JSON args body; ops that carry Arrow append it raw after a newline.
				std::string body;
				try {
					body = HeaderPayloadJsonExcludingRouting(h);
				} catch (const std::invalid_argument &e) {
					SendError(*this, FrameTarget {ns, req_id}, "InvalidInputException",
					          std::string("request header: ") + e.what());
					break;
				}
				if (op == OP_INSERT || op == OP_RPC_TABLE || op == OP_WRITE_PLAN) {
					body.push_back('\n');
					body.append(raw_body, raw_body_len);
				}
				auto held = held_.find(*catalog);
				if (held != held_.end()) {
					if (held_count_ >= static_cast<size_t>(MAX_CONCURRENT_REQS)) {
						SendError(*this, FrameTarget {ns, req_id}, "InvalidInputException",
						          "too many requests held for paused catalog \"" + *catalog + "\" (limit " +
						              std::to_string(MAX_CONCURRENT_REQS) + ")");
						break;
					}
					held->second.push_back(HeldReq {SessionRef {ns, *catalog}, req_id, op, std::move(body)});
					held_count_++;
					break;
				}
				AdmitAndQueueRequest(SessionRef {ns, *catalog}, req_id, op, std::move(body));
				break;
			}
			case FrameType::CREDIT: {
				const auto n = static_cast<int>(GetInt(h, "n", 0));
				auto slot = FindSlot(ns, req_id);
				if (slot && n > 0) {
					AddCredits(slot, n);
				}
				break;
			}
			case FrameType::CANCEL: {
				// Reserved req id 0 = "close this session".
				if (req_id == 0 && ns != 0) {
					CloseSessionAndCancelItsRequests(ns);
					break;
				}
				if (CancelHeld(ns, req_id)) {
					break;
				}
				auto slot = FindSlot(ns, req_id);
				if (slot) {
					cancels_.fetch_add(1);
					{
						std::lock_guard<std::mutex> lk(slot->mu);
						slot->cancel.store(true);
					}
					QueueParked(slot, RequestTask::Kind::CANCEL);
				}
				break;
			}
			case FrameType::PING:
				SendFrame(PackPong(req_id));
				break;
			case FrameType::PONG:
				// No deadline is enforced; matched explicitly to distinguish it from an unknown type.
				break;
			case FrameType::OPEN:
				HandleOpen(h, frame);
				break;
			case FrameType::HELLO:
				SendFrame(PackHelloErr("InvalidInputException", "FT_HELLO already received on this socket"));
				stopping_.store(true);
				break;
			default:
				break;
			}
		}
	} catch (const std::exception &) {
		// A hard read error ends the session like an EOF would.
	}

	stopping_.store(true);
	// Leave the registry first, so a push arriving during teardown is not queued onto a writer that is
	// about to stop.
	registration.Release();
	{
		// A parked stream is on no worker, so it would never see `stopping_` by itself.
		std::lock_guard<std::mutex> lk(map_mu_);
		for (auto &entry : inflight_) {
			QueueParked(entry.second, RequestTask::Kind::CANCEL);
		}
	}
	{
		// The cancel flag only lands between chunks, so a worker inside a long query would sit there
		// until the query finished on its own. Interrupting its ClientContext unwinds it from inside
		// DuckDB instead.
		std::unique_lock<std::mutex> lk(workers_mu_);
		if (!workers_cv_.wait_for(lk, std::chrono::milliseconds(SHUTDOWN_DRAIN_TIMEOUT_MS),
		                          [&] { return active_workers_ == 0; })) {
			lk.unlock();
			InterruptWorkers();
			lk.lock();
			// Unbounded after the interrupt, deliberately: a running request holds references into
			// this reactor and its transactions, so moving on while one is live is a use-after-free.
			// A hang is recoverable; that is not.
			workers_cv_.wait(lk, [&] { return active_workers_ == 0; });
		}
	}
	StopWorkers();
	StopCallbackThreads();
	DropAllTxns();
	// Before the writer stops: an upstream's reader forwards onto it.
	for (auto &entry : upstreams_) {
		StopUpstream(*entry.second);
	}
	StopWriter();
	conn_.ShutdownWrite();
	return reqs_handled_.load();
}

void ServeReactor::AdmitAndQueueRequest(const SessionRef &sess, uint32_t req_id, uint8_t op, std::string body) {
	auto slot = std::make_shared<RequestSlot>();
	slot->ns = sess.ns;
	slot->req_id = req_id;
	slot->catalog = sess.catalog;
	const InflightKey key {sess.ns, req_id};
	{
		std::lock_guard<std::mutex> lk(map_mu_);
		if (inflight_.find(key) != inflight_.end()) {
			SendError(*this, FrameTarget {sess.ns, req_id}, "InvalidInputException",
			          "req_id " + std::to_string(req_id) + " in use");
			return;
		}
		// Enforce the window advertised in every OPEN_ACK. Each accepted REQ holds a queue entry,
		// then a Connection while it runs or parks, so an unbounded window is a client-driven memory bomb; refusing is
		// what the advertised limit means.
		if (inflight_.size() >= static_cast<size_t>(MAX_CONCURRENT_REQS)) {
			SendError(*this, FrameTarget {sess.ns, req_id}, "InvalidInputException",
			          "too many concurrent requests on this connection (limit " + std::to_string(MAX_CONCURRENT_REQS) +
			              ", as advertised in OPEN_ACK)");
			return;
		}
		inflight_.emplace(key, slot);
	}
	reqs_handled_.fetch_add(1);
	{
		std::lock_guard<std::mutex> lk(workers_mu_);
		active_workers_++;
	}
	QueueTask(RequestTask {RequestTask::Kind::START, sess, req_id, std::move(slot), op, std::move(body), nullptr});
}

void ServeReactor::QueueTask(RequestTask task) {
	{
		std::lock_guard<std::mutex> lk(workers_mu_);
		tasks_.push_back(std::move(task));
		if (tasks_.size() > static_cast<size_t>(idle_workers_) &&
		    worker_threads_.size() < static_cast<size_t>(options_.workers)) {
			worker_threads_.emplace_back(&ServeReactor::WorkerLoop, this);
			peak_workers_.store(static_cast<int64_t>(worker_threads_.size()));
		}
	}
	tasks_cv_.notify_one();
}

void ServeReactor::QueueParked(const std::shared_ptr<RequestSlot> &slot, RequestTask::Kind kind) {
	std::unique_ptr<ParkedStream> stream;
	{
		std::lock_guard<std::mutex> lk(slot->mu);
		stream = std::move(slot->parked);
	}
	if (stream) {
		QueueTask(
		    RequestTask {kind, SessionRef {slot->ns, slot->catalog}, slot->req_id, slot, 0, {}, std::move(stream)});
	}
}

bool ServeReactor::HasWaitingRequests() {
	std::lock_guard<std::mutex> lk(workers_mu_);
	return !tasks_.empty();
}

void ServeReactor::WorkerLoop() {
	std::unique_lock<std::mutex> lk(workers_mu_);
	while (true) {
		idle_workers_++;
		tasks_cv_.wait(lk, [&] { return workers_stop_ || !tasks_.empty(); });
		idle_workers_--;
		if (tasks_.empty()) {
			return;
		}
		auto task = std::move(tasks_.front());
		tasks_.pop_front();
		lk.unlock();
		RunRequest(task);
		lk.lock();
	}
}

void ServeReactor::RunRequest(RequestTask &task) {
	auto &slot = *task.slot;
	if (task.kind == RequestTask::Kind::RESUME && stopping_.load()) {
		task.kind = RequestTask::Kind::CANCEL;
	}
	try {
		switch (task.kind) {
		case RequestTask::Kind::START: {
			if (stopping_.load()) {
				break;
			}
			// Cancelled while queued: never run.
			if (slot.cancel.load()) {
				EndRequest(slot, PackRespEndCancelled(FrameTarget {slot.ns, slot.req_id}));
				break;
			}
			std::unique_ptr<Connection> conn(new Connection(db_));
			MarkServed(*conn->context);
			{
				std::lock_guard<std::mutex> lk(slot.mu);
				slot.context = conn->context;
			}
			task.stream = HandleRequest(*this, conn, task.sess, task.op, task.body.data(), task.body.size(), slot);
			break;
		}
		case RequestTask::Kind::RESUME:
			break;
		case RequestTask::Kind::CANCEL:
			task.stream->Cancel(*this, slot);
			task.stream.reset();
			break;
		}
		if (task.stream) {
			auto result = task.stream->Pump(*this, slot);
			if (result != PumpResult::DONE) {
				ParkOrQueue(std::move(task), result);
				return;
			}
		}
	} catch (const std::exception &e) {
		ErrorData err(e);
		EndRequest(slot, PackErr(FrameTarget {task.sess.ns, task.req_id}, n6k::WireExceptionName(err.Type()),
		                         err.RawMessage(), n6k::IsRetriableExceptionType(err.Type())));
	}
	// Before the request retires: an RPC stream's close runs the host generator's `finally`.
	task.stream.reset();

	{
		std::lock_guard<std::mutex> lk(map_mu_);
		inflight_.erase(InflightKey {task.sess.ns, task.req_id});
	}
	{
		std::lock_guard<std::mutex> lk(workers_mu_);
		active_workers_--;
	}
	workers_cv_.notify_all();
}

void ServeReactor::ParkOrQueue(RequestTask task, PumpResult result) {
	if (result == PumpResult::PARKED) {
		auto &slot = *task.slot;
		std::lock_guard<std::mutex> lk(slot.mu);
		const bool stopped = slot.cancel.load() || stopping_.load();
		if (!stopped && slot.credits <= 0) {
			slot.parked = std::move(task.stream);
			slot.parked_at = std::chrono::steady_clock::now();
			return;
		}
		task.kind = stopped ? RequestTask::Kind::CANCEL : RequestTask::Kind::RESUME;
	} else {
		task.kind = RequestTask::Kind::RESUME;
	}
	QueueTask(std::move(task));
}

void ServeReactor::StopWorkers() {
	std::vector<std::thread> workers;
	{
		std::lock_guard<std::mutex> lk(workers_mu_);
		workers_stop_ = true;
		workers.swap(worker_threads_);
	}
	tasks_cv_.notify_all();
	for (auto &worker : workers) {
		if (worker.joinable()) {
			worker.join();
		}
	}
}

} // namespace n6k
} // namespace duckdb
