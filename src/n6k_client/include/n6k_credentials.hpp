#pragma once

#include "duckdb.hpp"

#include <functional>

namespace duckdb {

class ClientContext;
class DatabaseInstance;

// Strip a known scheme to a host[:port][/path] key, matching how secret SCOPEs are normalized.
string StripUrlScheme(const string &url);

// Precedence: inline `token` > TYPE n6k secret > "". Native: an access token that is missing or near
// expiry is minted from an n6k_refresh secret and stored as a secret.
string ResolveAttachToken(ClientContext &context, const string &base_url, const string &inline_token);

// Resolves a secret-sourced token for ConnectionHub::Options::resolve_token. Native-only.
std::function<string()> RedialTokenResolver(DatabaseInstance &db, const string &base_url);

// Percent-encode for an x-www-form-urlencoded body. Native-only.
string FormUrlEncode(const string &value);

} // namespace duckdb
