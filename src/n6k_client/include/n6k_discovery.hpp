#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ClientContext;

// The subset of RFC 8414 Authorization Server Metadata that n6k uses.
struct N6kAuthServerMetadata {
	string token_endpoint;
	string device_authorization_endpoint;
};

// Fetch + cache (per issuer) RFC 8414 metadata from <issuer>/.well-known/oauth-authorization-server. Native-only.
const N6kAuthServerMetadata &GetOrFetchAuthServerMetadata(ClientContext &context, const string &issuer);

} // namespace duckdb
