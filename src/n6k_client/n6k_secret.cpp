#include "n6k_secret.hpp"
#include "n6k_credentials.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

// normalize scope prefixes to scheme-less form so a bare hostname matches; empty scope = catch-all
static vector<string> NormalizeScope(const vector<string> &input_scope) {
	auto scope = input_scope;
	if (scope.empty()) {
		scope = {""};
	}
	for (auto &prefix : scope) {
		prefix = StripUrlScheme(prefix);
	}
	return scope;
}

static unique_ptr<BaseSecret> CreateN6kSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(NormalizeScope(input.scope), input.type, input.provider, input.name);
	secret->TrySetValue("token", input);
	secret->redact_keys = {"token"};
	return std::move(secret);
}

#ifndef WASM_LOADABLE_EXTENSIONS
// refresh secret: durable subject token + bits to mint short-lived JWTs (RFC 8693); native-only
static unique_ptr<BaseSecret> CreateN6kRefreshSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(NormalizeScope(input.scope), input.type, input.provider, input.name);
	secret->TrySetValue("subject_token", input);
	secret->TrySetValue("issuer", input);
	secret->TrySetValue("client_id", input);
	secret->TrySetValue("resource", input);
	secret->redact_keys = {"subject_token"};
	return std::move(secret);
}
#endif

void RegisterN6kSecret(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = "n6k";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction config_fn;
	config_fn.secret_type = "n6k";
	config_fn.provider = "config";
	config_fn.function = CreateN6kSecretFromConfig;
	config_fn.named_parameters["token"] = LogicalType::VARCHAR;
	loader.RegisterFunction(config_fn);

#ifndef WASM_LOADABLE_EXTENSIONS
	SecretType refresh_type;
	refresh_type.name = "n6k_refresh";
	refresh_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	refresh_type.default_provider = "config";
	loader.RegisterSecretType(refresh_type);

	CreateSecretFunction refresh_fn;
	refresh_fn.secret_type = "n6k_refresh";
	refresh_fn.provider = "config";
	refresh_fn.function = CreateN6kRefreshSecretFromConfig;
	refresh_fn.named_parameters["subject_token"] = LogicalType::VARCHAR;
	refresh_fn.named_parameters["issuer"] = LogicalType::VARCHAR;
	refresh_fn.named_parameters["client_id"] = LogicalType::VARCHAR;
	refresh_fn.named_parameters["resource"] = LogicalType::VARCHAR;
	loader.RegisterFunction(refresh_fn);
#endif
}

} // namespace duckdb
