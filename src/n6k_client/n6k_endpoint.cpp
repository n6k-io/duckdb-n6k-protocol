#include "n6k_endpoint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cctype>

namespace duckdb {
namespace n6k {

namespace {

static const char *const PLAIN_PREFIX = "n6k://";
static const char *const SECURE_PREFIX = "n6ks://";

[[noreturn]] void ThrowInvalid(const string &url, const string &why) {
	throw InvalidInputException("n6k: invalid URL '%s': %s", url, why);
}

bool IsAlnum(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) != 0;
}

void ValidateHost(const string &url, const string &host) {
	if (host.empty()) {
		ThrowInvalid(url, "no host");
	}
	const bool ipv6 = host[0] == '[';
	if (ipv6 && (host.size() < 3 || host.back() != ']')) {
		ThrowInvalid(url, "malformed IPv6 host");
	}
	const idx_t first = ipv6 ? 1 : 0;
	const idx_t last = ipv6 ? host.size() - 1 : host.size();
	for (idx_t i = first; i < last; i++) {
		const char c = host[i];
		const bool allowed = ipv6 ? (std::isxdigit(static_cast<unsigned char>(c)) != 0 || c == ':' || c == '.')
		                          : (IsAlnum(c) || c == '-' || c == '.' || c == '_');
		if (!allowed) {
			ThrowInvalid(url, "host contains '" + string(1, c) + "'");
		}
	}
}

string CanonicalPort(const string &url, const string &port) {
	if (port.empty() || port.size() > 5) {
		ThrowInvalid(url, "malformed port");
	}
	int value = 0;
	for (char c : port) {
		if (std::isdigit(static_cast<unsigned char>(c)) == 0) {
			ThrowInvalid(url, "malformed port");
		}
		value = value * 10 + (c - '0');
	}
	if (value < 1 || value > 65535) {
		ThrowInvalid(url, "port out of range");
	}
	return std::to_string(value);
}

string TrimTrailingSlashes(string path) {
	while (!path.empty() && path.back() == '/') {
		path.pop_back();
	}
	return path;
}

} // namespace

N6kEndpoint ParseN6kEndpoint(const string &url) {
	static const struct {
		const char *prefix;
		N6kEndpoint::Scheme scheme;
	} SCHEMES[] = {
	    {SECURE_PREFIX, N6kEndpoint::Scheme::SECURE}, {PLAIN_PREFIX, N6kEndpoint::Scheme::PLAIN},
	    {"https://", N6kEndpoint::Scheme::SECURE},    {"http://", N6kEndpoint::Scheme::PLAIN},
	    {"wss://", N6kEndpoint::Scheme::SECURE},      {"ws://", N6kEndpoint::Scheme::PLAIN},
	};

	N6kEndpoint endpoint;
	string rest = url;
	for (auto &candidate : SCHEMES) {
		if (StringUtil::StartsWith(url, candidate.prefix)) {
			endpoint.scheme = candidate.scheme;
			rest = url.substr(string(candidate.prefix).size());
			break;
		}
	}
	if (endpoint.scheme == N6kEndpoint::Scheme::NONE && rest.find("://") != string::npos) {
		ThrowInvalid(url, "unknown scheme");
	}
	for (char c : rest) {
		if (c == '?' || c == '#' || c == '\\' || std::isspace(static_cast<unsigned char>(c)) != 0 ||
		    std::iscntrl(static_cast<unsigned char>(c)) != 0) {
			ThrowInvalid(url, "contains '" + string(1, c) + "'");
		}
	}

	const auto slash = rest.find('/');
	const string authority = rest.substr(0, slash);
	if (slash != string::npos) {
		endpoint.prefix = TrimTrailingSlashes(rest.substr(slash));
	}

	string host = authority;
	string port;
	bool has_port = false;
	if (StringUtil::StartsWith(authority, "[")) {
		const auto close = authority.find(']');
		if (close == string::npos) {
			ThrowInvalid(url, "malformed IPv6 host");
		}
		host = authority.substr(0, close + 1);
		const auto after = authority.substr(close + 1);
		if (!after.empty()) {
			if (after[0] != ':') {
				ThrowInvalid(url, "malformed IPv6 host");
			}
			has_port = true;
			port = after.substr(1);
		}
	} else {
		const auto colon = authority.find(':');
		if (colon != string::npos) {
			host = authority.substr(0, colon);
			has_port = true;
			port = authority.substr(colon + 1);
		}
	}
	ValidateHost(url, host);
	endpoint.host = StringUtil::Lower(host);
	if (has_port) {
		endpoint.port = CanonicalPort(url, port);
	}
	return endpoint;
}

string SecretLookupKey(const string &url) {
	if (StringUtil::StartsWith(url, "/")) {
		return TrimTrailingSlashes(url) + "/";
	}
	const auto endpoint = ParseN6kEndpoint(url);
	const bool secure = endpoint.scheme == N6kEndpoint::Scheme::SECURE;
	const string port = !endpoint.port.empty() ? endpoint.port : (secure ? "443" : "80");
	return (secure ? SECURE_PREFIX : PLAIN_PREFIX) + endpoint.host + ":" + port + endpoint.prefix + "/";
}

vector<string> NormalizeSecretScope(const vector<string> &scope) {
	vector<string> normalized;
	for (auto &entry : scope) {
		if (entry.empty()) {
			normalized.emplace_back();
			continue;
		}
		if (StringUtil::StartsWith(entry, "/")) {
			normalized.push_back(TrimTrailingSlashes(entry) + "/");
			continue;
		}
		const auto endpoint = ParseN6kEndpoint(entry);
		if (endpoint.port.empty() && !endpoint.prefix.empty()) {
			throw InvalidInputException("n6k: SCOPE '%s': a path needs a port, as in host:port/path", entry);
		}
		const string rest = endpoint.host + ":" + (endpoint.port.empty() ? "" : endpoint.port + endpoint.prefix + "/");
		if (endpoint.scheme != N6kEndpoint::Scheme::SECURE) {
			normalized.push_back(PLAIN_PREFIX + rest);
		}
		if (endpoint.scheme != N6kEndpoint::Scheme::PLAIN) {
			normalized.push_back(SECURE_PREFIX + rest);
		}
	}
	return normalized;
}

} // namespace n6k
} // namespace duckdb
