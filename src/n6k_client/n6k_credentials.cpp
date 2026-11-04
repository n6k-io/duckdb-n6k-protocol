#include "n6k_credentials.hpp"

#include "n6k_endpoint.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
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

static string TokenFromMatch(SecretMatch &match) {
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

static string TokenFromSecret(ClientContext &context, const string &url) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, n6k::SecretLookupKey(url), "n6k");
	return TokenFromMatch(match);
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

// Mint state runs in its own transaction: a caller queued on MintLock holds a snapshot older than the previous mint,
// and a rotated refresh token must survive the caller's ATTACH failing.
static SecretMatch LookupCommittedSecret(ClientContext &context, const string &key, const string &type) {
	Connection con(DatabaseInstance::GetDatabase(context));
	con.BeginTransaction();
	auto match = SecretManager::Get(*con.context)
	                 .LookupSecret(CatalogTransaction::GetSystemCatalogTransaction(*con.context), key, type);
	con.Commit();
	return match;
}

static void CommitSecretReplacement(ClientContext &context, unique_ptr<const BaseSecret> secret,
                                    SecretPersistType persist_type, const string &storage = "") {
	Connection con(DatabaseInstance::GetDatabase(context));
	con.BeginTransaction();
	SecretManager::Get(*con.context)
	    .RegisterSecret(CatalogTransaction::GetSystemCatalogTransaction(*con.context), std::move(secret),
	                    OnCreateConflict::REPLACE_ON_CONFLICT, persist_type, storage);
	con.Commit();
}

// Store a minted JWT as a per-host TEMPORARY n6k access secret so WS/HTTP pick it up; re-mints replace in place.
static void StoreMintedAccessSecret(ClientContext &context, const string &key, const string &token) {
	vector<string> scope {key};
	auto secret = make_uniq<KeyValueSecret>(scope, "n6k", "config", "__n6k_minted_" + key);
	secret->secret_map["token"] = Value(token);
	secret->redact_keys = {"token"};
	CommitSecretReplacement(context, std::move(secret), SecretPersistType::TEMPORARY);
}

static std::mutex &MintLock() {
	static std::mutex instance;
	return instance;
}

static string SecretFieldOrEmpty(const KeyValueSecret &kv, const char *name) {
	auto value = kv.TryGetValue(name);
	return value.IsNull() ? string() : value.ToString();
}

// A rotated refresh token is written before the access token is cached, in the tier the secret already lives in.
static string MintViaRefreshGrant(ClientContext &context, const string &key, const SecretMatch &match,
                                  const KeyValueSecret &kv) {
	auto refresh_token = SecretFieldOrEmpty(kv, "refresh_token");
	auto token_endpoint = SecretFieldOrEmpty(kv, "token_endpoint");
	auto client_id = SecretFieldOrEmpty(kv, "client_id");
	auto resource = SecretFieldOrEmpty(kv, "resource");
	if (token_endpoint.empty() || client_id.empty()) {
		throw InvalidInputException("n6k: n6k_refresh secret '%s' has refresh_token but is missing "
		                            "token_endpoint/client_id",
		                            kv.GetName());
	}

	DUCKDB_LOG_INFO(context, "n6k auth: refreshing data-service token for " + key + " via " + token_endpoint);

	string body = "grant_type=refresh_token&refresh_token=" + FormUrlEncode(refresh_token) +
	              "&client_id=" + FormUrlEncode(client_id);
	if (!resource.empty()) {
		body += "&resource=" + FormUrlEncode(resource);
	}

	auto response = N6kHttpClient::Get().PostFormStatus(context, token_endpoint, body);
	n6k::JsonDoc doc(response.body);
	auto error = doc.Parsed() ? n6k::JsonGetStr(doc.Root(), "error") : string();
	if (error == "invalid_grant") {
		throw IOException("n6k: refresh token in secret '%s' was rejected by %s (invalid_grant); log in again and "
		                  "update the secret",
		                  kv.GetName(), token_endpoint);
	}
	if (response.status < 200 || response.status >= 300) {
		throw IOException("n6k: token refresh at %s failed (HTTP %d): %s", token_endpoint, response.status,
		                  error.empty() ? string("no OAuth error code") : error);
	}
	if (!doc.Parsed()) {
		throw IOException("n6k: invalid token-refresh response from %s", token_endpoint);
	}
	auto minted = n6k::JsonGetStr(doc.Root(), "access_token");
	if (minted.empty()) {
		throw IOException("n6k: token-refresh response from %s is missing 'access_token'", token_endpoint);
	}

	auto rotated = n6k::JsonGetStr(doc.Root(), "refresh_token");
	if (!rotated.empty() && rotated != refresh_token) {
		auto replacement = make_uniq<KeyValueSecret>(kv);
		replacement->secret_map["refresh_token"] = Value(rotated);
		CommitSecretReplacement(context, std::move(replacement), match.secret_entry->persist_type,
		                        match.secret_entry->storage_mode);
		DUCKDB_LOG_INFO(context, "n6k auth: stored rotated refresh token in secret '" + kv.GetName().GetIdentifierName() + "'");
	}

	StoreMintedAccessSecret(context, key, minted);
	return minted;
}

// Mint a JWT from an n6k_refresh secret via the refresh grant, or discovery + RFC 8693 exchange; "" if none.
// Single-flight within this process only.
static string MintAndStoreAccessToken(ClientContext &context, const string &url) {
	auto key = n6k::SecretLookupKey(url);
	std::lock_guard<std::mutex> guard(MintLock());

	// Another caller may have just minted while we waited for the lock.
	auto committed_access = LookupCommittedSecret(context, key, "n6k");
	auto existing = TokenFromMatch(committed_access);
	if (!existing.empty() && !JwtHasExpiryWithin(existing, kJwtExpiryMarginSeconds)) {
		return existing;
	}

	auto match = LookupCommittedSecret(context, key, "n6k_refresh");
	if (!match.HasMatch()) {
		// Created in the caller's still-open transaction.
		match = SecretManager::Get(context).LookupSecret(CatalogTransaction::GetSystemCatalogTransaction(context), key,
		                                                 "n6k_refresh");
	}
	if (!match.HasMatch()) {
		DUCKDB_LOG_DEBUG(context, "n6k auth: no n6k_refresh secret matches " + key + "; cannot mint");
		return "";
	}
	const auto &kv = dynamic_cast<const KeyValueSecret &>(match.GetSecret());
	if (!SecretFieldOrEmpty(kv, "refresh_token").empty()) {
		return MintViaRefreshGrant(context, key, match, kv);
	}
	auto subject_token = SecretFieldOrEmpty(kv, "subject_token");
	auto issuer = SecretFieldOrEmpty(kv, "issuer");
	auto client_id = SecretFieldOrEmpty(kv, "client_id");
	auto resource = SecretFieldOrEmpty(kv, "resource");
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

std::function<string()> RedialTokenResolver(DatabaseInstance &db, const string &base_url) {
	weak_ptr<DatabaseInstance> weak_db = db.shared_from_this();
	return [weak_db, base_url]() -> string {
		auto database = weak_db.lock();
		if (!database) {
			throw IOException("n6k: cannot refresh the token for %s: the database has closed", base_url);
		}
		Connection con(*database);
		con.BeginTransaction();
		auto token = ResolveAttachToken(*con.context, base_url, "");
		con.Commit();
		return token;
	};
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
		DUCKDB_LOG_INFO(context, "n6k auth: no credential resolved for " + n6k::SecretLookupKey(base_url) +
		                             " (anonymous; the server will reject if it requires auth)");
	}
	return token;
}

} // namespace duckdb
