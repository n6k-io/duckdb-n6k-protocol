#include "catch.hpp"
#include "n6k_endpoint.hpp"

#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace duckdb::n6k;

namespace {

bool ScopeMatches(const string &scope, const string &url) {
	const auto key = SecretLookupKey(url);
	for (auto &prefix : NormalizeSecretScope({scope})) {
		if (StringUtil::StartsWith(key, prefix)) {
			return true;
		}
	}
	return false;
}

} // namespace

TEST_CASE("an endpoint splits into scheme, lowercase host, port and prefix", "[endpoint]") {
	auto endpoint = ParseN6kEndpoint("n6k://Host.Example:8099/a/b/");

	REQUIRE(endpoint.scheme == N6kEndpoint::Scheme::PLAIN);
	REQUIRE(endpoint.host == "host.example");
	REQUIRE(endpoint.port == "8099");
	REQUIRE(endpoint.prefix == "/a/b");
}

TEST_CASE("each scheme maps to plain or secure", "[endpoint]") {
	auto [url, scheme] = GENERATE(table<const char *, N6kEndpoint::Scheme>({
	    {"n6k://h", N6kEndpoint::Scheme::PLAIN},
	    {"http://h", N6kEndpoint::Scheme::PLAIN},
	    {"ws://h", N6kEndpoint::Scheme::PLAIN},
	    {"n6ks://h", N6kEndpoint::Scheme::SECURE},
	    {"https://h", N6kEndpoint::Scheme::SECURE},
	    {"wss://h", N6kEndpoint::Scheme::SECURE},
	    {"h", N6kEndpoint::Scheme::NONE},
	}));
	INFO(url);

	REQUIRE(ParseN6kEndpoint(url).scheme == scheme);
}

TEST_CASE("an IPv6 host keeps its brackets", "[endpoint]") {
	auto endpoint = ParseN6kEndpoint("n6k://[::1]:8080/p");

	REQUIRE(endpoint.host == "[::1]");
	REQUIRE(endpoint.port == "8080");
	REQUIRE(endpoint.prefix == "/p");
}

TEST_CASE("a port is canonicalized", "[endpoint]") {
	REQUIRE(ParseN6kEndpoint("n6k://h:00080").port == "80");
}

TEST_CASE("a malformed endpoint is refused with its reason", "[endpoint]") {
	auto [url, reason] = GENERATE(table<const char *, const char *>({
	    {"ftp://h", "unknown scheme"},
	    {"n6k://h?catalog=db", "contains '?'"},
	    {"n6k://h#f", "contains '#'"},
	    {"n6k://h x", "contains ' '"},
	    {"n6k://", "no host"},
	    {"n6k:///prefix", "no host"},
	    {"n6k://user@h", "host contains '@'"},
	    {"n6k://h:", "malformed port"},
	    {"n6k://h:123456", "malformed port"},
	    {"n6k://h:1x", "malformed port"},
	    {"n6k://h:0", "port out of range"},
	    {"n6k://h:70000", "port out of range"},
	    {"n6k://[::1", "malformed IPv6 host"},
	    {"n6k://[::1]x", "malformed IPv6 host"},
	    {"n6k://[]", "malformed IPv6 host"},
	    {"n6k://[zz]", "host contains 'z'"},
	}));
	INFO(url);

	REQUIRE_THROWS_WITH(ParseN6kEndpoint(url), Catch::Contains(reason));
}

TEST_CASE("a lookup key fills in the scheme and its default port", "[endpoint]") {
	auto [url, key] = GENERATE(table<const char *, const char *>({
	    {"localhost:8099", "n6k://localhost:8099/"},
	    {"n6k://h", "n6k://h:80/"},
	    {"n6ks://h", "n6ks://h:443/"},
	    {"https://H:1/a/", "n6ks://h:1/a/"},
	    {"/prefix", "/prefix/"},
	    {"/prefix/", "/prefix/"},
	}));
	INFO(url);

	REQUIRE(SecretLookupKey(url) == key);
}

TEST_CASE("a scope stores a prefix per scheme it allows", "[endpoint]") {
	REQUIRE(NormalizeSecretScope({""}) == vector<string> {""});
	REQUIRE(NormalizeSecretScope({"/p/"}) == vector<string> {"/p/"});
	REQUIRE(NormalizeSecretScope({"h"}) == vector<string> {"n6k://h:", "n6ks://h:"});
	REQUIRE(NormalizeSecretScope({"h:8099"}) == vector<string> {"n6k://h:8099/", "n6ks://h:8099/"});
	REQUIRE(NormalizeSecretScope({"n6k://h:8099"}) == vector<string> {"n6k://h:8099/"});
	REQUIRE(NormalizeSecretScope({"n6ks://h:443"}) == vector<string> {"n6ks://h:443/"});
}

TEST_CASE("a malformed scope is refused", "[endpoint]") {
	auto [scope, reason] = GENERATE(table<const char *, const char *>({
	    {"host/path", "a path needs a port"},
	    {"bad@host", "host contains '@'"},
	    {"host:99999", "port out of range"},
	}));
	INFO(scope);

	REQUIRE_THROWS_WITH(NormalizeSecretScope({scope}), Catch::Contains(reason));
}

TEST_CASE("a scope stops at the host, port and path boundary", "[endpoint]") {
	auto [scope, url, matches] = GENERATE(table<const char *, const char *, bool>({
	    {"prod.example.com", "n6k://prod.example.com", true},
	    {"prod.example.com", "n6ks://prod.example.com", true},
	    {"prod.example.com", "n6k://prod.example.com.evil.net", false},
	    {"prod.example.com", "n6k://prod.example.community", false},
	    {"localhost:80", "n6k://localhost:8099", false},
	    {"n6ks://h:1", "n6k://h:1", false},
	    {"h:1/app", "n6k://h:1/app", true},
	    {"h:1/app", "n6k://h:1/app/sub", true},
	    {"h:1/app", "n6k://h:1/application", false},
	    {"h:1/app", "n6k://h:1/other", false},
	}));
	INFO(scope << " against " << url);

	REQUIRE(ScopeMatches(scope, url) == matches);
}
