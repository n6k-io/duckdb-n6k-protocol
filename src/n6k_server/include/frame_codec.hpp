#pragma once

#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
namespace n6k {

// Wire frame: one msgpack header map + optional raw Arrow body (same wire the client speaks).

// Decode one frame into header map (owned by oh) + raw body.
//
// Throws on input we cannot parse. A malformed frame carries no req_id, so there is nothing to
// answer with an FT_ERR; skipping it silently would leave the client's request hanging to its
// deadline with no diagnostic on either side.
void ParseFrame(const std::string &frame, msgpack::object_handle &oh, const char *&body, size_t &body_len);

// Where an outbound session-scoped frame is routed. `ns` is the protocol's session key: it lets one
// socket carry several catalog sessions. ns 0 = connection-scoped; the key is omitted from the wire.
struct FrameTarget {
	int64_t ns = 0;
	uint32_t req_id = 0;
};

// Extra map entries a target contributes, so callers can size their pack_map correctly.
inline uint32_t NsKeyCount(const FrameTarget &target) {
	return target.ns != 0 ? 1u : 0u;
}

// Packs the {t, id, [ns]} prefix shared by every session-scoped frame. Callers must have sized their
// pack_map to include these (2 + NsKeyCount(target)) entries.
inline void PackTIdInto(msgpack::packer<msgpack::sbuffer> &pk, FrameType t, const FrameTarget &target) {
	PackString(pk, "t");
	pk.pack(static_cast<int>(t));
	PackString(pk, "id");
	pk.pack(target.req_id);
	if (target.ns != 0) {
		PackString(pk, "ns");
		pk.pack(target.ns);
	}
}

// The offered capabilities this implementation also supports, in offered order.
template <size_t N>
std::vector<std::string> IntersectCaps(const std::vector<std::string> &offered,
                                       const std::array<const char *, N> &supported) {
	std::vector<std::string> out;
	for (auto &cap : offered) {
		for (auto *mine : supported) {
			if (cap == mine) {
				out.push_back(cap);
				break;
			}
		}
	}
	return out;
}

std::vector<std::string> GrantedHelloCaps(const std::vector<std::string> &offered,
                                          const std::vector<std::string> &app_caps);
std::string PackHelloAck(const std::vector<std::string> &capabilities);
std::string PackHelloErr(const std::string &type, const std::string &message);
std::string PackHelloPending(const std::string &message);
// An OPEN as this server forwards it upstream.
std::string PackOpen(int64_t ns, const std::string &catalog, const std::vector<std::string> &capabilities);
std::string PackOpenAck(int64_t ns, const std::vector<std::string> &capabilities);
std::string PackOpenErr(int64_t ns, const std::string &type, const std::string &message);
std::string PackOpenPending(int64_t ns, const std::string &message);

// PONG is connection-scoped, not session-scoped: the protocol exempts PING/PONG from `ns`.
std::string PackPong(uint32_t req_id);

// Server-initiated keepalive, same exemption from `ns`.
std::string PackPing(uint32_t ping_id);

// Unsolicited server->client notification: {t, op, ...body, [ns]} and NO `id`, since it answers no
// request. `ns` routes it to the session whose catalog it concerns, so a client attached to several
// catalogs over one socket invalidates only the right one.
std::string PackPushInvalidate(int64_t ns, const std::vector<std::string> &schemas);

// Connection-scoped (no `ns`): the full catalog list after it changed on a live connection.
std::string PackPushCatalogsChanged(const std::vector<std::string> &catalogs);

std::string PackCatalogsChunk(uint32_t req_id, const std::vector<std::string> &catalogs);
std::string PackErr(const FrameTarget &target, const std::string &type, const std::string &message, bool retriable);
std::string PackRespSchema(const FrameTarget &target, const std::string &arrow);
std::string PackRespChunkArrow(const FrameTarget &target, const std::string &arrow);
std::string PackRespEnd(const FrameTarget &target);
std::string PackRespEndRowcount(const FrameTarget &target, int64_t rowcount);
std::string PackRespEndRowcountNull(const FrameTarget &target);
std::string PackRespEndTxn(const FrameTarget &target, int64_t txn);
std::string PackRespEndCancelled(const FrameTarget &target);
std::string PackRespEndOk(const FrameTarget &target);

} // namespace n6k
} // namespace duckdb
