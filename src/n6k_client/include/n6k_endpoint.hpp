#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace n6k {

struct N6kEndpoint {
	enum class Scheme { NONE, PLAIN, SECURE };

	Scheme scheme = Scheme::NONE;
	// Lowercase; IPv6 keeps its brackets.
	string host;
	// Empty when the input gave none.
	string port;
	// "" or "/a/b", with no trailing slash.
	string prefix;
};

// Accepts n6k(s)://, http(s)://, ws(s):// or no scheme. Throws InvalidInputException on userinfo, a malformed host or
// port, and a query or fragment.
N6kEndpoint ParseN6kEndpoint(const string &url);

// The path secrets are looked up by: n6k(s)://host:port[/prefix]/, with the scheme's default port filled in. No scheme
// reads as n6k://. A browser-relative /prefix stays relative, as /prefix/.
string SecretLookupKey(const string &url);

// What a secret stores for its SCOPE. DuckDB matches secrets with a plain StartsWith, so each stored prefix ends on a
// boundary of the lookup key: a host can't match a longer host, nor a port a longer port. No scheme stores one prefix
// per scheme, no port matches any port, and "" stays the catch-all.
vector<string> NormalizeSecretScope(const vector<string> &scope);

} // namespace n6k
} // namespace duckdb
