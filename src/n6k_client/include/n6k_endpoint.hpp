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

// Accepts no scheme or one listed in SCHEMES (n6k_endpoint.cpp). Throws InvalidInputException on any other scheme,
// userinfo, a missing or malformed host, a malformed port, a query or fragment, a backslash, whitespace or a control
// character.
N6kEndpoint ParseN6kEndpoint(const string &url);

// The path secrets are looked up by: n6k(s)://host:port[/prefix]/, with the scheme's default port filled in. No scheme
// reads as n6k://. A browser-relative /prefix stays relative, as /prefix/.
string SecretLookupKey(const string &url);

// What a secret stores for its SCOPE. DuckDB matches secrets with a plain StartsWith, so each stored prefix ends on a
// boundary of the lookup key: a host can't match a longer host, nor a port a longer port. No scheme stores one prefix
// per scheme, no port matches any port, "" stays the catch-all, and a browser-relative /prefix stays relative, as
// /prefix/. Throws InvalidInputException as ParseN6kEndpoint does, and on a path with no port.
vector<string> NormalizeSecretScope(const vector<string> &scope);

} // namespace n6k
} // namespace duckdb
