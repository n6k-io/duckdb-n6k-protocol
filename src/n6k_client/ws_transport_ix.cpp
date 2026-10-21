#ifndef WASM_LOADABLE_EXTENSIONS

#include "ws_transport_ix.hpp"

#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketMessage.h>
#include <ixwebsocket/IXWebSocketMessageType.h>
#include <ixwebsocket/IXWebSocketSendData.h>

namespace duckdb {
namespace n6k {

IxWebSocketTransport::IxWebSocketTransport(std::string url) : url_(std::move(url)) {
}

IxWebSocketTransport::~IxWebSocketTransport() {
	Stop();
}

void IxWebSocketTransport::Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) {
	ws_.reset(new ix::WebSocket());
	ws_->setUrl(url_);
	ws_->disablePerMessageDeflate();
	// Deterministic drop semantics: surface an error on close rather than silently reconnect to a fresh
	// session.
	ws_->disableAutomaticReconnection();

	ws_->setOnMessageCallback([on_message, on_close, on_open](const ix::WebSocketMessagePtr &msg) {
		if (msg->type == ix::WebSocketMessageType::Message) {
			on_message(msg->str, msg->binary);
		} else if (msg->type == ix::WebSocketMessageType::Open) {
			// The 101 landed and the socket is ReadyState::Open. Until this point sendBinary refuses
			// every frame, so the reactor's writer must stay parked (see NativeReactor::StartWriterThread).
			if (on_open) {
				on_open();
			}
		} else if (msg->type == ix::WebSocketMessageType::Close) {
			on_close(msg->closeInfo.reason);
		} else if (msg->type == ix::WebSocketMessageType::Error) {
			on_close(msg->errorInfo.reason);
		}
	});

	// Dials asynchronously: the socket is NOT usable when this returns, only once Open fires above.
	ws_->start();
}

bool IxWebSocketTransport::Send(const std::string &frame) {
	if (!ws_) {
		return false;
	}
	// sendBinary returns success=false and discards the frame when the socket is not yet (or no longer)
	// ReadyState::Open. Propagate it; the writer thread turns a refusal into a connection error.
	return ws_->sendBinary(frame).success;
}

void IxWebSocketTransport::Stop() {
	if (ws_) {
		ws_->stop();
		ws_.reset();
	}
}

} // namespace n6k
} // namespace duckdb

#endif // !WASM_LOADABLE_EXTENSIONS
