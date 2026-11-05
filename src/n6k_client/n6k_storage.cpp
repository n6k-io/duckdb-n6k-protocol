#include "n6k_storage.hpp"
#include "n6k_catalog.hpp"
#include "n6k_catalog_session.hpp"
#include "n6k_credentials.hpp"
#include "n6k_endpoint.hpp"
#include "n6k_schema_entry.hpp"
#include "n6k_transaction.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "connection_hub.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS
#include "n6k_str_utils.hpp"
#include "n6k_wasm_main_thread.hpp"
#include "ws_transport_vsock.hpp"
#endif

namespace duckdb {

#ifdef WASM_LOADABLE_EXTENSIONS
// The hub over a socket the host registered with registerWebsocket(), which already ran HELLO on it. Null when
// the host knows no such registration.
static std::shared_ptr<n6k::ConnectionHub>
AcquireRegisteredSocket(DatabaseInstance &instance, n6k::ConnectionRegistry &registry, const string &registration_id) {
	const auto id_literal = n6k::EscapeSingleQuotedJsLiteral(registration_id);
	if (n6k::RunScriptStringOnMain("n6k.isRegistered('" + id_literal + "')") != "1") {
		return nullptr;
	}
	auto id = n6k::ParseConnectionId(registration_id, "n6k");
	return registry
	    .Acquire("registered\n" + registration_id,
	             [&]() {
		             n6k::ConnectionHub::Options opts;
		             opts.dial = [registration_id]() {
			             return std::unique_ptr<n6k::Transport>(new n6k::VsockTransport(registration_id, ""));
		             };
		             opts.can_redial = true;
		             opts.queue_sends = false;
		             opts.endpoint = "registered socket " + registration_id;
		             opts.host_handshake = [id_literal]() {
			             std::vector<std::string> caps;
			             for (auto &cap : StringUtil::Split(
			                      n6k::RunScriptStringOnMain("n6k.helloCapabilities('" + id_literal + "')"), ',')) {
				             caps.push_back(cap);
			             }
			             return caps;
		             };
		             return n6k::ConnectionHub::Create(instance.shared_from_this(), std::move(opts), id);
	             })
	    .hub;
}
#endif

// A lease on the connection CONNECTION '<id>' names, or on the one for this URL and token (opened if none is).
static std::shared_ptr<n6k::ConnectionLease> LeaseConnection(DatabaseInstance &instance, const string &connection_id,
                                                             const string &base_url, const string &query,
                                                             const string &token, bool token_from_secrets,
                                                             const shared_ptr<Logger> &logger) {
	auto registry = n6k::ConnectionRegistry::Get(instance);
	if (!connection_id.empty()) {
		auto hub = registry->AcquireById(n6k::ParseConnectionId(connection_id, "n6k"));
#ifdef WASM_LOADABLE_EXTENSIONS
		if (!hub) {
			hub = AcquireRegisteredSocket(instance, *registry, connection_id);
		}
#endif
		if (!hub) {
			throw IOException("n6k: no open connection %s; n6k_connect() / registerWebsocket() returns the id",
			                  connection_id);
		}
		return std::make_shared<n6k::ConnectionLease>(registry, std::move(hub));
	}
#ifdef WASM_LOADABLE_EXTENSIONS
	const auto ws_url = n6k::RunScriptStringOnMain("n6k.resolveWsUrl('" +
	                                               n6k::EscapeSingleQuotedJsLiteral(base_url + "/ws" + query) + "')");
	const auto registered =
	    n6k::RunScriptStringOnMain("n6k.registeredFor('" + n6k::EscapeSingleQuotedJsLiteral(ws_url) + "', '" +
	                               n6k::EscapeSingleQuotedJsLiteral(token) + "')");
	if (!registered.empty()) {
		if (auto hub = AcquireRegisteredSocket(instance, *registry, registered)) {
			return std::make_shared<n6k::ConnectionLease>(registry, std::move(hub));
		}
	}
	auto make = [&]() {
		const auto id = UUID::GenerateRandomUUID();
		const auto id_string = UUID::ToString(id);
		n6k::ConnectionHub::Options opts;
		opts.dial = [id_string, ws_url]() {
			return std::unique_ptr<n6k::Transport>(new n6k::VsockTransport(id_string, ws_url));
		};
		opts.can_redial = true;
		opts.queue_sends = false;
		opts.token = token;
		opts.endpoint = ws_url;
		opts.logger = logger;
		return n6k::ConnectionHub::Create(instance.shared_from_this(), std::move(opts), id);
	};
#else
	const auto ws_url = n6k::WsUrlFromBaseUrl(base_url, query);
	auto make = [&]() {
		auto opts = n6k::WebSocketOptions(ws_url, token);
		if (token_from_secrets) {
			opts.resolve_token = RedialTokenResolver(instance, base_url);
		}
		opts.logger = logger;
		return n6k::ConnectionHub::Create(instance.shared_from_this(), std::move(opts));
	};
#endif
	auto acquired = registry->Acquire(n6k::WebSocketKey(ws_url, token), make);
	return std::make_shared<n6k::ConnectionLease>(registry, std::move(acquired.hub));
}

// Tells the browser host which catalogs ride which connection, so it can report status per catalog.
static void AnnounceCatalog(n6k::ConnectionHub &hub, const string &catalog, bool joined) {
#ifdef WASM_LOADABLE_EXTENSIONS
	n6k::RunScriptOnMain(string(joined ? "n6k.catalogJoined('" : "n6k.catalogLeft('") + hub.IdString() + "', '" +
	                     n6k::EscapeSingleQuotedJsLiteral(catalog) + "')");
#else
	(void)hub;
	(void)catalog;
	(void)joined;
#endif
}

static unique_ptr<Catalog> N6kStorageAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	bool secure = false;
	string host, port, prefix, token;
	// "?query" from the URL, sent after /ws on the dial and part of the connection's identity.
	string query;
	auto question = info.path.find('?');
	if (question != string::npos) {
		query = info.path.substr(question);
		info.path = info.path.substr(0, question);
	}

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
	if (info.options.find("wsid") != info.options.end()) {
		throw IOException("n6k: the 'wsId' option is gone: ATTACH '' AS %s (TYPE n6k, CONNECTION '<id>') with the id "
		                  "registerWebsocket() returned",
		                  name);
	}
	if (info.options.find("wsfd") != info.options.end()) {
		throw IOException("n6k: the 'wsFd' option is gone: open the socket with SELECT connection_id FROM "
		                  "n6k_connect(wsFd => <fd>, ...), then ATTACH '' AS %s (TYPE n6k, CONNECTION '<id>')",
		                  name);
	}
	// Ride a connection opened by n6k_connect() or registerWebsocket(); key is "connection" (lowercased).
	string connection_id;
	auto connection_it = info.options.find("connection");
	if (connection_it != info.options.end()) {
		connection_id = connection_it->second.ToString();
		if (connection_id.empty()) {
			throw IOException("n6k: 'connection' is empty: pass the id n6k_connect() / registerWebsocket() returned");
		}
	}
	if (!connection_id.empty() &&
	    (!info.path.empty() || !host.empty() || !port.empty() || !prefix.empty() || secure || anonymous)) {
		throw IOException("n6k: 'connection' is mutually exclusive with a URL / host / port / anonymous");
	}
	if (!connection_id.empty() && !token.empty()) {
		throw IOException("n6k: 'token' cannot be combined with 'connection': the connection's token was sent when "
		                  "it opened");
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
	if (connection_id.empty()) {
#ifdef WASM_LOADABLE_EXTENSIONS
		// In the browser a relative path is enough: JS resolves it.
		if (host.empty() && !info.path.empty()) {
			base_url = prefix.empty() ? info.path : info.path + prefix;
		}
#endif
		if (base_url.empty()) {
			if (host.empty()) {
				throw IOException("n6k: attach '%s' has no server to connect to. Specify a URL as the ATTACH "
				                  "path (n6k://host[:port][/prefix], or a relative /prefix in the browser), a "
				                  "'host' (+ optional 'port') option, or CONNECTION '<id>' for a connection "
				                  "n6k_connect() / registerWebsocket() opened. [options: %s]",
				                  name, render_options());
			}
			base_url = (secure ? "https://" : "http://") + host + (port.empty() ? "" : ":" + port) + prefix;
			n6k::ParseN6kEndpoint(base_url);
		}
	}

	if (StringUtil::EndsWith(base_url, "/")) {
		base_url = base_url.substr(0, base_url.size() - 1);
	}
	info.path = ":memory:";

	bool token_from_secrets = false;
	if (!anonymous && connection_id.empty()) {
		const bool has_inline_token = !token.empty();
		token = ResolveAttachToken(context, base_url, token);
		token_from_secrets = !has_inline_token && !token.empty();
	}

	auto should_abort = [&context] {
		return context.IsInterrupted();
	};
	auto logger = db.GetDatabase().GetLogManager().GlobalLoggerReference();
	auto lease = LeaseConnection(db.GetDatabase(), connection_id, base_url, query, token, token_from_secrets, logger);
	std::shared_ptr<CatalogSession> session;
	try {
		lease->Hub()->EnsureConnected(should_abort);
		auto on_detach = [lease, name = name]() {
			AnnounceCatalog(*lease->Hub(), name, false);
			lease->Release();
		};
		session =
		    CatalogSession::CreateOnConnection(lease->Hub(), on_detach, name, server_catalog, should_abort, logger);
		AnnounceCatalog(*lease->Hub(), name, true);
	} catch (...) {
		lease->Release();
		throw;
	}

	unique_ptr<N6kCatalog> catalog;
	try {
		catalog = make_uniq<N6kCatalog>(db, base_url, session);
	} catch (...) {
		session->Detach();
		throw;
	}

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
