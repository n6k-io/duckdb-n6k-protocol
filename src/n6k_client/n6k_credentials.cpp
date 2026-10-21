#include "n6k_credentials.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#ifndef WASM_LOADABLE_EXTENSIONS
#include "n6k_discovery.hpp"
#include "n6k_http_client.hpp"
#include "ws_json.hpp"

#include "duckdb/common/types/blob.hpp"

#include <cctype>
#include <ctime>
#include <mutex>
#endif

namespace duckdb {

string StripUrlScheme(const string &url) {
	static const char *const kSchemes[] = {"https://", "http://", "n6ks://", "n6k://"};
	for (auto *scheme : kSchemes) {
		if (StringUtil::StartsWith(url, scheme)) {
			return url.substr(string(scheme).size());
		}
	}
	return url;
}

static string TokenFromSecret(ClientContext &context, const string &url) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, StripUrlScheme(url), "n6k");
	if (!match.HasMatch()) {
		return "";
	}
	const auto &kv = dynamic_cast<const KeyValueSecret &>(match.GetSecret());
	auto value = kv.TryGetValue("token");
	if (value.IsNull()) {
		return "";
	}
	return value.ToString();
}

#ifndef WASM_LOADABLE_EXTENSIONS

static constexpr int64_t kJwtExpiryMarginSeconds = 60;

// The payload is decoded, never verified — no signature check.
static bool JwtHasExpiryWithin(const string &token, int64_t margin_seconds) {
	auto first = token.find('.');
	if (first == string::npos) {
		return false;
	}
	auto second = token.find('.', first + 1);
	if (second == string::npos) {
		return false;
	}
	string seg = token.substr(first + 1, second - first - 1);
	for (auto &ch : seg) {
		if (ch == '-') {
			ch = '+';
		} else if (ch == '_') {
			ch = '/';
		}
	}
	while (seg.size() % 4 != 0) {
		seg.push_back('=');
	}
	string payload;
	try {
		payload = Blob::FromBase64(string_t(seg.c_str(), seg.size()));
	} catch (...) {
		return false;
	}
	n6k::JsonDoc doc(payload);
	if (!doc.Parsed()) {
		return false;
	}
	auto exp = n6k::JsonGetUint(doc.Root(), "exp", 0);
	if (exp == 0) {
		return false;
	}
	auto now = static_cast<int64_t>(std::time(nullptr));
	return static_cast<int64_t>(exp) - now < margin_seconds;
}

string FormUrlEncode(const string &s) {
	static const char *const kHex = "0123456789ABCDEF";
	string out;
	out.reserve(s.size() * 3);
	for (unsigned char c : s) {
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
			out.push_back(static_cast<char>(c));
		} else {
			out.push_back('%');
			out.push_back(kHex[c >> 4]);
			out.push_back(kHex[c & 0x0F]);
		}
	}
	return out;
}

// Store a minted JWT as a per-host TEMPORARY n6k access secret so WS/HTTP pick it up; re-mints replace in place.
static void StoreMintedAccessSecret(ClientContext &context, const string &key, const string &token) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	vector<string> scope {key};
	auto secret = make_uniq<KeyValueSecret>(scope, "n6k", "config", "__n6k_minted_" + key);
	secret->secret_map["token"] = Value(token);
	secret->redact_keys = {"token"};
	secret_manager.RegisterSecret(transaction, std::move(secret), OnCreateConflict::REPLACE_ON_CONFLICT,
	                              SecretPersistType::TEMPORARY);
}

static std::mutex &MintLock() {
	static std::mutex instance;
	return instance;
}

// Mint a JWT from an n6k_refresh secret via OAuth discovery + RFC 8693 exchange; "" if none. Single-flight.
static string MintAndStoreAccessToken(ClientContext &context, const string &url) {
	auto key = StripUrlScheme(url);
	std::lock_guard<std::mutex> guard(MintLock());

	// Another caller may have just minted while we waited for the lock.
	auto existing = TokenFromSecret(context, url);
	if (!existing.empty() && !JwtHasExpiryWithin(existing, kJwtExpiryMarginSeconds)) {
		return existing;
	}

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, key, "n6k_refresh");
	if (!match.HasMatch()) {
		DUCKDB_LOG_DEBUG(context, "n6k auth: no n6k_refresh secret matches " + key + "; cannot mint");
		return "";
	}
	const auto &kv = dynamic_cast<const KeyValueSecret &>(match.GetSecret());
	auto field = [&](const char *name) -> string {
		auto value = kv.TryGetValue(name);
		return value.IsNull() ? string() : value.ToString();
	};
	auto subject_token = field("subject_token");
	auto issuer = field("issuer");
	auto client_id = field("client_id");
	auto resource = field("resource");
	if (subject_token.empty() || issuer.empty()) {
		DUCKDB_LOG_WARNING(context, "n6k auth: n6k_refresh secret for " + key + " is missing subject_token/issuer");
		return "";
	}

	auto &meta = GetOrFetchAuthServerMetadata(context, issuer);
	DUCKDB_LOG_INFO(context, "n6k auth: minting data-service token for " + key + " via " + meta.token_endpoint +
	                             " (resource=" + resource + ")");

	// RFC 8693 token exchange (form-encoded request at the token endpoint).
	string body = "grant_type=" + FormUrlEncode("urn:ietf:params:oauth:grant-type:token-exchange") +
	              "&subject_token=" + FormUrlEncode(subject_token) +
	              "&subject_token_type=" + FormUrlEncode("urn:ietf:params:oauth:token-type:access_token");
	if (!resource.empty()) {
		body += "&resource=" + FormUrlEncode(resource);
	}
	if (!client_id.empty()) {
		body += "&client_id=" + FormUrlEncode(client_id);
	}

	auto response = N6kHttpClient::Get().PostForm(context, meta.token_endpoint, body);
	n6k::JsonDoc doc(response);
	if (!doc.Parsed()) {
		throw IOException("n6k: invalid token-exchange response from %s", meta.token_endpoint);
	}
	auto minted = n6k::JsonGetStr(doc.Root(), "access_token");
	if (minted.empty()) {
		throw IOException("n6k: token-exchange response from %s is missing 'access_token'", meta.token_endpoint);
	}
	DUCKDB_LOG_INFO(context,
	                "n6k auth: token exchange succeeded for " + key + " (" + std::to_string(minted.size()) + " bytes)");

	StoreMintedAccessSecret(context, key, minted);
	return minted;
}

static string TokenFromSecretMintingIfNearExpiry(ClientContext &context, const string &url) {
	auto access = TokenFromSecret(context, url);
	if (!access.empty() && !JwtHasExpiryWithin(access, kJwtExpiryMarginSeconds)) {
		return access;
	}
	auto minted = MintAndStoreAccessToken(context, url);
	if (!minted.empty()) {
		return minted;
	}
	return access;
}

#endif // !WASM_LOADABLE_EXTENSIONS

string ResolveAttachToken(ClientContext &context, const string &base_url, const string &inline_token) {
	if (!inline_token.empty()) {
		return inline_token;
	}
#ifndef WASM_LOADABLE_EXTENSIONS
	auto token = TokenFromSecretMintingIfNearExpiry(context, base_url);
#else
	auto token = TokenFromSecret(context, base_url);
#endif
	if (token.empty()) {
		DUCKDB_LOG_INFO(context, "n6k auth: no credential resolved for " + StripUrlScheme(base_url) +
		                             " (anonymous; the server will reject if it requires auth)");
	}
	return token;
}

} // namespace duckdb
