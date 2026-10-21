#include "n6k_storage.hpp"
#include "n6k_catalog.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_credentials.hpp"
#include "n6k_schema_entry.hpp"
#include "n6k_transaction.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

static unique_ptr<Catalog> N6kStorageAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	bool secure = false;
	string host, port, prefix, token;

	if (StringUtil::StartsWith(info.path, "n6k://") || StringUtil::StartsWith(info.path, "n6ks://")) {
		secure = StringUtil::StartsWith(info.path, "n6ks://");
		auto remainder = info.path.substr(secure ? 7 : 6);

		auto slash_pos = remainder.find('/');
		string host_port = (slash_pos != string::npos) ? remainder.substr(0, slash_pos) : remainder;
		if (slash_pos != string::npos) {
			prefix = remainder.substr(slash_pos);
		}

		auto colon_pos = host_port.find(':');
		if (colon_pos != string::npos) {
			host = host_port.substr(0, colon_pos);
			port = host_port.substr(colon_pos + 1);
		} else {
			host = host_port;
		}
	}

	auto host_it = info.options.find("host");
	if (host_it != info.options.end()) {
		host = host_it->second.ToString();
	}
	auto port_it = info.options.find("port");
	if (port_it != info.options.end()) {
		port = port_it->second.ToString();
	}
	auto secure_it = info.options.find("secure");
	if (secure_it != info.options.end()) {
		secure = secure_it->second.GetValue<bool>();
	}
	auto prefix_it = info.options.find("prefix");
	if (prefix_it != info.options.end()) {
		prefix = prefix_it->second.ToString();
		if (!prefix.empty() && prefix[0] != '/') {
			prefix = "/" + prefix;
		}
	}
	auto token_it = info.options.find("token");
	if (token_it != info.options.end()) {
		token = token_it->second.ToString();
	}
	// Server-side catalog to open, decoupled from the ATTACH alias; defaults to the alias name.
	string server_catalog;
	auto catalog_it = info.options.find("catalog");
	if (catalog_it != info.options.end()) {
		server_catalog = catalog_it->second.ToString();
	}
	// Force the anonymous path: no secret lookup, no OAuth minting, no bearer token.
	bool anonymous = false;
	auto anonymous_it = info.options.find("anonymous");
	if (anonymous_it != info.options.end()) {
		anonymous = anonymous_it->second.GetValue<bool>();
	}
	if (anonymous && !token.empty()) {
		throw IOException("n6k: 'anonymous' and 'token' options are mutually exclusive");
	}
	// Ride a WebSocket the host app registered via registerWebsocket(ws); WASM/browser only.
	string ws_id;
	// DuckDB lowercases ATTACH option names, so the key is "wsid" not "wsId".
	auto wsid_it = info.options.find("wsid");
	if (wsid_it != info.options.end()) {
		ws_id = wsid_it->second.ToString();
	}
#ifndef WASM_LOADABLE_EXTENSIONS
	if (!ws_id.empty()) {
		throw IOException("n6k: wsId/registerWebsocket is only supported in the WASM/browser build");
	}
#endif

	// Ride an already-connected socket fd (native analogue of wsId); key is "wsfd" (lowercased).
	int ws_fd = -1;
	auto wsfd_it = info.options.find("wsfd");
	if (wsfd_it != info.options.end()) {
		ws_fd = wsfd_it->second.GetValue<int32_t>();
	}
#ifdef WASM_LOADABLE_EXTENSIONS
	if (ws_fd >= 0) {
		throw IOException("n6k: wsFd is only supported in the native build");
	}
#endif
#ifdef _WIN32
	// FdTransport rides a POSIX socket fd; no Windows equivalent.
	if (ws_fd >= 0) {
		throw IOException("n6k: wsFd is not supported on Windows");
	}
#endif
	if (ws_fd >= 0 && (!info.path.empty() || !host.empty() || !port.empty() || !prefix.empty() || secure)) {
		throw IOException("n6k: 'wsFd' is mutually exclusive with a URL / host / port");
	}
	if ((ws_fd >= 0 || !ws_id.empty()) && !token.empty()) {
		throw IOException("n6k: 'token' cannot be combined with 'wsFd' / 'wsId': the socket's token is sent once by "
		                  "n6k_connect() / registerWebsocket()");
	}

	auto render_options = [&info]() -> string {
		string s;
		for (auto &kv : info.options) {
			if (!s.empty()) {
				s += ", ";
			}
			const string val = kv.second.ToString();
			const bool redact = StringUtil::Lower(kv.first) == "token" && !val.empty();
			s += kv.first + "=" + (redact ? "'<redacted>'" : "'" + val + "'");
		}
		return s;
	};

	string base_url;

#ifdef WASM_LOADABLE_EXTENSIONS
	// In WASM/browser a plain relative path is enough — JS resolves it; no host/port needed.
	if (!ws_id.empty()) {
		// Registered socket carries the connection; base URL is derived JS-side.
		base_url = "";
	} else if (host.empty() && !info.path.empty()) {
		base_url = prefix.empty() ? info.path : info.path + prefix;
	} else {
		if (host.empty()) {
			if (wsid_it != info.options.end()) {
				throw IOException("n6k: attach '%s': wsId is empty — pass the id returned by "
				                  "registerWebsocket(), e.g. ATTACH '' AS %s (TYPE n6k, wsId '<id>'). "
				                  "(Got an empty value: did registerWebsocket() return undefined, or "
				                  "was `${id}` not interpolated?) [options: %s]",
				                  name, name, render_options());
			}
			throw IOException("n6k: attach '%s' has no server to connect to. Specify one of: a URL as the "
			                  "ATTACH path (n6k://host:port[/prefix], or a relative /prefix in the "
			                  "browser); a 'host' (+ optional 'port') option; or wsId '<id>' for a socket "
			                  "you registered with registerWebsocket(). [options: %s]",
			                  name, render_options());
		}
		base_url = (secure ? "https://" : "http://") + host + (port.empty() ? "" : ":" + port) + prefix;
	}
#else
	if (ws_fd >= 0) {
		// Riding a provided fd: nothing to dial.
		base_url = "";
	} else {
		if (host.empty()) {
			throw IOException("n6k: attach '%s' has no server to connect to. Specify a URL as the ATTACH "
			                  "path (n6k://host[:port][/prefix]), a 'host' (+ optional 'port') option, "
			                  "or wsFd <fd> for a socket you already opened. [options: %s]",
			                  name, render_options());
		}
		base_url = (secure ? "https://" : "http://") + host + (port.empty() ? "" : ":" + port) + prefix;
	}
#endif

	if (StringUtil::EndsWith(base_url, "/")) {
		base_url = base_url.substr(0, base_url.size() - 1);
	}
	info.path = ":memory:";

	if (!anonymous && ws_fd < 0 && ws_id.empty()) {
		token = ResolveAttachToken(context, base_url, token);
	}

	auto session = CatalogSession::Create(base_url, token, name, server_catalog, ws_id, ws_fd,
	                                      [&context] { return context.IsInterrupted(); });

	auto catalog = make_uniq<N6kCatalog>(db, base_url, session);

	// Create only the default schema (no RPC) so ATTACH returns immediately; full list loads lazily.
	auto system_transaction = CatalogTransaction::GetSystemTransaction(db.GetDatabase());
	catalog->GetOrCreateSchema(system_transaction, DEFAULT_SCHEMA);

	return std::move(catalog);
}

static unique_ptr<TransactionManager> N6kStorageTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                   AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<N6kTransactionManager>(db, catalog.Cast<N6kCatalog>());
}

shared_ptr<StorageExtension> CreateN6kStorageExtension() {
	auto result = make_shared_ptr<StorageExtension>();
	result->attach = N6kStorageAttach;
	result->create_transaction_manager = N6kStorageTransactionManager;
	return result;
}

} // namespace duckdb
