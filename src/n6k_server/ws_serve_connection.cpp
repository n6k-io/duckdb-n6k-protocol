#include "ws_serve_connection.hpp"

#include "n6k_yyjson_util.hpp"
#include "serve_reactor.hpp"

#include "yyjson.hpp"

#include "ixwebsocket/IXSocket.h"
#include "ixwebsocket/IXSocketConnect.h"
#include "ixwebsocket/IXWebSocket.h"

#include <utility>

namespace duckdb {
namespace n6k {

namespace {

class N6kConnectionState : public ix::ConnectionState {
public:
	int fd = -1;
};

std::string RequestJson(const ix::WebSocketOpenInfo &info) {
	auto *doc = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
	auto *root = duckdb_yyjson::yyjson_mut_obj(doc);
	duckdb_yyjson::yyjson_mut_doc_set_root(doc, root);
	duckdb_yyjson::yyjson_mut_obj_add_strncpy(doc, root, "uri", info.uri.c_str(), info.uri.size());
	auto *headers = duckdb_yyjson::yyjson_mut_obj_add_obj(doc, root, "headers");
	for (auto &header : info.headers) {
		duckdb_yyjson::yyjson_mut_obj_add_strncpy(doc, headers, header.first.c_str(), header.second.c_str(),
		                                          header.second.size());
	}
	return SerializeJsonDocAndFree(doc);
}

} // namespace

N6kWsServer::N6kWsServer(int port, const std::string &host, OnOpen on_open, size_t max_connections)
    : ix::WebSocketServer(port, host, ix::SocketServer::kDefaultTcpBacklog, max_connections),
      on_open_(std::move(on_open)) {
	// n6k frames are msgpack and Arrow IPC, already compact; deflate would only add CPU per frame.
	disablePerMessageDeflate();
	setConnectionStateFactory([] { return std::make_shared<N6kConnectionState>(); });
	setOnClientMessageCallback([this](const std::shared_ptr<ix::ConnectionState> &connection_state, ix::WebSocket &ws,
	                                  const ix::WebSocketMessagePtr &msg) { OnMessage(connection_state, ws, msg); });
}

void N6kWsServer::handleConnection(std::unique_ptr<ix::Socket> socket,
                                   std::shared_ptr<ix::ConnectionState> connection_state) {
	static_cast<N6kConnectionState &>(*connection_state).fd = socket->getFd();
	handleUpgrade(std::move(socket), connection_state);
	connection_state->setTerminated();
}

void N6kWsServer::Adopt(int fd, ix::HttpRequestPtr request) {
	// As ix::SocketServer::run configures an accepted fd.
	ix::SocketConnect::configure(fd);
	auto connection_state = std::make_shared<N6kConnectionState>();
	connection_state->fd = fd;
	handleUpgrade(std::unique_ptr<ix::Socket>(new ix::Socket(fd)), connection_state, std::move(request));
	connection_state->setTerminated();
}

void N6kWsServer::OnMessage(const std::shared_ptr<ix::ConnectionState> &connection_state, ix::WebSocket &ws,
                            const ix::WebSocketMessagePtr &msg) {
	switch (msg->type) {
	case ix::WebSocketMessageType::Open: {
		auto transport = std::make_shared<WsServeTransport>(ws);
		{
			std::lock_guard<std::mutex> lk(mu_);
			live_[&ws] = transport;
		}
		on_open_(static_cast<N6kConnectionState &>(*connection_state).fd, transport, RequestJson(msg->openInfo));
		break;
	}
	case ix::WebSocketMessageType::Message: {
		std::shared_ptr<WsServeTransport> transport;
		{
			std::lock_guard<std::mutex> lk(mu_);
			auto it = live_.find(&ws);
			if (it != live_.end()) {
				transport = it->second;
			}
		}
		if (transport) {
			transport->EnqueueFrameUnlessClosed(msg->binary ? StreamFrameType::N6K : StreamFrameType::JSONRPC,
			                                    msg->str);
		}
		break;
	}
	case ix::WebSocketMessageType::Close:
	case ix::WebSocketMessageType::Error: {
		std::shared_ptr<WsServeTransport> transport;
		{
			std::lock_guard<std::mutex> lk(mu_);
			auto it = live_.find(&ws);
			if (it != live_.end()) {
				transport = it->second;
				live_.erase(it);
			}
		}
		if (transport) {
			transport->CloseInbound();
		}
		break;
	}
	default:
		break;
	}
}

SessionStats ServeWsConnection(WsServeTransport &transport, DatabaseInstance &db, ClientContext &context,
                               const std::vector<std::string> &catalogs, ServeOptions options, hugeint_t session_id,
                               std::string request) {
	options.request = std::move(request);
	options.has_request = true;
	ServeReactor reactor(transport, db, context, catalogs, std::move(options), session_id);
	reactor.Run();
	return reactor.Stats();
}

} // namespace n6k
} // namespace duckdb
