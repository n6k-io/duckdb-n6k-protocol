#include "n6k_login_function.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#ifndef WASM_LOADABLE_EXTENSIONS
#include "n6k_credentials.hpp"
#include "n6k_discovery.hpp"
#include "n6k_http_client.hpp"
#include "ws_json.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <chrono>
#include <thread>
#endif

namespace duckdb {

struct N6kLoginBindData : public TableFunctionData {
	string subject_token;
	string issuer;
	string client_id;
	string resource;
	string status;
	string detail;
};

struct N6kLoginState : public GlobalTableFunctionState {
	bool done = false;
};

#ifndef WASM_LOADABLE_EXTENSIONS

static const int64_t kDeviceDefaultMaxWaitSeconds = 300;
static const char *const kDeviceCodeGrant = "urn:ietf:params:oauth:grant-type:device_code";

// use SYSTEM transaction so the secret is visible to an immediate ATTACH regardless of result consumption
static void StoreRefreshSecret(ClientContext &context, const N6kLoginBindData &creds) {
	auto &secret_manager = SecretManager::Get(context);
	auto &db = DatabaseInstance::GetDatabase(context);

	// throwaway lookup forces InitializeSecrets so the persistent backend exists (RegisterSecret won't)
	secret_manager.LookupSecret(CatalogTransaction::GetSystemCatalogTransaction(context), "n6k-login", "n6k_refresh");

	vector<string> scope {""};
	auto secret = make_uniq<KeyValueSecret>(scope, "n6k_refresh", "config", "n6k_login");
	secret->secret_map["subject_token"] = Value(creds.subject_token);
	secret->secret_map["issuer"] = Value(creds.issuer);
	if (!creds.client_id.empty()) {
		secret->secret_map["client_id"] = Value(creds.client_id);
	}
	if (!creds.resource.empty()) {
		secret->secret_map["resource"] = Value(creds.resource);
	}
	secret->redact_keys = {"subject_token"};

	secret_manager.RegisterSecret(CatalogTransaction::GetSystemTransaction(db), std::move(secret),
	                              OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::PERSISTENT);
	DUCKDB_LOG_INFO(context, "n6k_login: stored persistent n6k_refresh secret 'n6k_login'");
}

static unique_ptr<FunctionData> RunDeviceLoginAndStoreRefreshSecret(ClientContext &context, const string &auth_url,
                                                                    const string &client_id, const string &resource) {
	auto &meta = GetOrFetchAuthServerMetadata(context, auth_url);
	if (meta.device_authorization_endpoint.empty()) {
		throw IOException("n6k_login: auth server %s does not advertise a device_authorization_endpoint", auth_url);
	}

	// RFC 8628 §3.1: device authorization request
	auto dev = N6kHttpClient::Get().PostFormStatus(context, meta.device_authorization_endpoint,
	                                               "client_id=" + FormUrlEncode(client_id));
	if (dev.status < 200 || dev.status >= 300) {
		throw IOException("n6k_login: device authorization to %s failed (HTTP %d): %s",
		                  meta.device_authorization_endpoint, dev.status, dev.body);
	}
	n6k::JsonDoc dev_doc(dev.body);
	if (!dev_doc.Parsed()) {
		throw IOException("n6k_login: invalid device authorization response");
	}
	auto device_code = n6k::JsonGetStr(dev_doc.Root(), "device_code");
	auto user_code = n6k::JsonGetStr(dev_doc.Root(), "user_code");
	auto verification = n6k::JsonGetStr(dev_doc.Root(), "verification_uri_complete");
	if (verification.empty()) {
		verification = n6k::JsonGetStr(dev_doc.Root(), "verification_uri");
	}
	auto interval = n6k::JsonGetUint(dev_doc.Root(), "interval", 5);
	auto expires_in = n6k::JsonGetUint(dev_doc.Root(), "expires_in", kDeviceDefaultMaxWaitSeconds);
	if (device_code.empty()) {
		throw IOException("n6k_login: device authorization response missing device_code");
	}
	if (verification.empty()) {
		throw IOException("n6k_login: device authorization response missing verification_uri");
	}
	DUCKDB_LOG_INFO(context, "n6k_login: device authorized via " + meta.device_authorization_endpoint +
	                             " (user_code=" + user_code + ", interval=" + std::to_string(interval) +
	                             "s, expires_in=" + std::to_string(expires_in) + "s)");

	Printer::Print("n6k_login: open " + verification + " and approve (code: " + user_code + ")");

	// RFC 8628 §3.4: poll the token endpoint until approval/expiry.
	string poll_body = "grant_type=" + FormUrlEncode(kDeviceCodeGrant) + "&device_code=" + FormUrlEncode(device_code) +
	                   "&client_id=" + FormUrlEncode(client_id);
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(static_cast<int64_t>(expires_in));
	int64_t wait_seconds = interval == 0 ? 1 : static_cast<int64_t>(interval);

	string subject_token;
	while (true) {
		std::this_thread::sleep_for(std::chrono::seconds(wait_seconds));
		auto resp = N6kHttpClient::Get().PostFormStatus(context, meta.token_endpoint, poll_body);
		n6k::JsonDoc doc(resp.body);
		if (resp.status >= 200 && resp.status < 300) {
			subject_token = doc.Parsed() ? n6k::JsonGetStr(doc.Root(), "access_token") : string();
			if (subject_token.empty()) {
				throw IOException("n6k_login: token response missing access_token");
			}
			break;
		}
		auto err = doc.Parsed() ? n6k::JsonGetStr(doc.Root(), "error") : string();
		if (err == "authorization_pending") {
			DUCKDB_LOG_DEBUG(context, "n6k_login: authorization_pending; still waiting for approval");
		} else if (err == "slow_down") {
			wait_seconds += 5;
			DUCKDB_LOG_WARNING(context, "n6k_login: server requested slow_down; interval now " +
			                                std::to_string(wait_seconds) + "s");
		} else {
			throw IOException("n6k_login: device token poll to %s failed (HTTP %d): %s", meta.token_endpoint,
			                  resp.status, resp.body);
		}
		if (std::chrono::steady_clock::now() >= deadline) {
			throw IOException("n6k_login: timed out waiting for device approval");
		}
	}
	DUCKDB_LOG_INFO(context, "n6k_login: approved; received subject token");

	auto result = make_uniq<N6kLoginBindData>();
	result->subject_token = subject_token;
	result->issuer = auth_url;
	result->client_id = client_id;
	result->resource = resource;
	result->status = "ok";
	result->detail = "stored persistent n6k_refresh secret 'n6k_login'";
	StoreRefreshSecret(context, *result);
	return std::move(result);
}

#endif // !WASM_LOADABLE_EXTENSIONS

static unique_ptr<FunctionData> N6kLoginBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("status");
	names.emplace_back("detail");
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);

#ifdef WASM_LOADABLE_EXTENSIONS
	throw NotImplementedException("n6k_login is not available in the browser — authenticate via the host application");
#else
	auto auth_url = input.inputs[0].GetValue<string>();
	string client_id = "n6k-duckdb";
	string resource;
	auto client_id_it = input.named_parameters.find("client_id");
	if (client_id_it != input.named_parameters.end()) {
		client_id = client_id_it->second.ToString();
	}
	auto resource_it = input.named_parameters.find("resource");
	if (resource_it != input.named_parameters.end()) {
		resource = resource_it->second.ToString();
	}
	return RunDeviceLoginAndStoreRefreshSecret(context, auth_url, client_id, resource);
#endif
}

static unique_ptr<GlobalTableFunctionState> N6kLoginInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<N6kLoginState>();
}

static void N6kLoginScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<N6kLoginBindData>();
	auto &state = data_p.global_state->Cast<N6kLoginState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	output.SetValue(0, 0, Value(bind_data.status));
	output.SetValue(1, 0, Value(bind_data.detail));
	output.SetCardinality(1);
	state.done = true;
}

void RegisterN6kLogin(ExtensionLoader &loader) {
	TableFunction fn("n6k_login", {LogicalType::VARCHAR}, N6kLoginScan, N6kLoginBind, N6kLoginInitGlobal);
	fn.named_parameters["client_id"] = LogicalType::VARCHAR;
	fn.named_parameters["resource"] = LogicalType::VARCHAR;
	loader.RegisterFunction(fn);
}

} // namespace duckdb
