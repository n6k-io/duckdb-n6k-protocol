#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ClientContext;

// Strip a known scheme to a host[:port][/path] key, matching how secret SCOPEs are normalized.
string StripUrlScheme(const string &url);

// Precedence: inline `token` > TYPE n6k secret > "". Native: an access token that is missing or near
// expiry is minted by token exchange and stored as a secret. Resolved once per ATTACH / n6k_connect;
// reconnects resend the same token without refreshing it.
string ResolveAttachToken(ClientContext &context, const string &base_url, const string &inline_token);

// Percent-encode for an x-www-form-urlencoded body. Native-only.
string FormUrlEncode(const string &value);

} // namespace duckdb
