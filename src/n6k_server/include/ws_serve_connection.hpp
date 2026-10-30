#pragma once

#include "serve_options.hpp"
#include "serve_reactor.hpp"
#include "ws_serve_transport.hpp"

#include "ixwebsocket/IXConnectionState.h"
#include "ixwebsocket/IXHttp.h"
#include "ixwebsocket/IXWebSocketServer.h"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;
class DatabaseInstance;

namespace n6k {

// Each upgraded connection reaches `on_open` as its socket fd, a transport fed by ix's callback thread, and its
// request as RequestJson. `on_open` runs on that callback thread, so it must hand the reactor to another.
class N6kWsServer : public ix::WebSocketServer {
public:
	using OnOpen = std::function<void(int fd, std::shared_ptr<WsServeTransport> transport, std::string request)>;

	// Past `max_connections` open at once, ix closes a new connection as soon as it is accepted. Not applied to
	// Adopt.
	N6kWsServer(int port, const std::string &host, OnOpen on_open,
	            size_t max_connections = ix::SocketServer::kDefaultMaxConnections);

	// Upgrade a connected socket whose HTTP request the host already read: ix takes the handshake from `request`
	// instead of reading one from `fd`. Takes ownership of `fd`; returns once the WebSocket closes or the upgrade
	// fails.
	void Adopt(int fd, ix::HttpRequestPtr request);

private:
	void handleConnection(std::unique_ptr<ix::Socket> socket,
	                      std::shared_ptr<ix::ConnectionState> connection_state) override;
	void OnMessage(const std::shared_ptr<ix::ConnectionState> &connection_state, ix::WebSocket &ws,
	               const ix::WebSocketMessagePtr &msg);

	OnOpen on_open_;
	std::mutex mu_;
	// Keyed by WebSocket address rather than ConnectionState id: ix hands the same WebSocket& to every callback for
	// a connection, so it is a stable key for its lifetime.
	std::map<ix::WebSocket *, std::shared_ptr<WsServeTransport>> live_;
};

// One connection's reactor, run on the calling thread, until it ends.
SessionStats ServeWsConnection(WsServeTransport &transport, DatabaseInstance &db, ClientContext &context,
                               const std::vector<std::string> &catalogs, ServeOptions options, hugeint_t session_id,
                               std::string request);

} // namespace n6k
} // namespace duckdb
