#include "n6k_discovery.hpp"
#include "n6k_http_client.hpp"
#include "ws_json.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

#include <mutex>
#include <unordered_map>

namespace duckdb {

static std::mutex &MetadataCacheLock() {
	static std::mutex instance;
	return instance;
}

static std::unordered_map<string, N6kAuthServerMetadata> &MetadataCache() {
	static std::unordered_map<string, N6kAuthServerMetadata> instance;
	return instance;
}

const N6kAuthServerMetadata &GetOrFetchAuthServerMetadata(ClientContext &context, const string &issuer) {
	std::lock_guard<std::mutex> guard(MetadataCacheLock());
	auto &cache = MetadataCache();
	auto cached = cache.find(issuer);
	if (cached != cache.end()) {
		// Map references stay valid (never erased), so returning after the lock releases is safe.
		return cached->second;
	}

	string base = issuer;
	if (StringUtil::EndsWith(base, "/")) {
		base = base.substr(0, base.size() - 1);
	}
	string url = base + "/.well-known/oauth-authorization-server";

	DUCKDB_LOG_INFO(context, "n6k oauth: discovering " + url);
	auto body = N6kHttpClient::Get().Get(context, url);
	n6k::JsonDoc doc(body);
	if (!doc.Parsed()) {
		throw IOException("n6k: invalid OAuth authorization-server metadata from %s", url);
	}

	N6kAuthServerMetadata meta;
	meta.token_endpoint = n6k::JsonGetStr(doc.Root(), "token_endpoint");
	meta.device_authorization_endpoint = n6k::JsonGetStr(doc.Root(), "device_authorization_endpoint");

	if (meta.token_endpoint.empty()) {
		throw IOException("n6k: OAuth metadata from %s is missing 'token_endpoint'", url);
	}
	DUCKDB_LOG_INFO(context, "n6k oauth: discovered token_endpoint=" + meta.token_endpoint +
	                             " device_authorization_endpoint=" + meta.device_authorization_endpoint);

	auto inserted = cache.emplace(issuer, std::move(meta));
	return inserted.first->second;
}

} // namespace duckdb
