#pragma once

#ifndef WASM_LOADABLE_EXTENSIONS

#include "ws_transport.hpp"

#include <memory>
#include <string>

namespace ix {
class WebSocket;
}

namespace duckdb {
namespace n6k {

// ixwebsocket-backed transport: dials ws(s)://url. The default transport.
class IxWebSocketTransport : public Transport {
public:
	explicit IxWebSocketTransport(std::string url);
	~IxWebSocketTransport() override;

	void Start(MessageFn on_message, CloseFn on_close, OpenFn on_open) override;
	bool Send(const std::string &frame) override;
	void Stop() override;

private:
	std::string url_;
	std::unique_ptr<ix::WebSocket> ws_;
};

} // namespace n6k
} // namespace duckdb

#endif // !WASM_LOADABLE_EXTENSIONS
