// `n6k_selftest_open_ns_distinct()` — pins the handshake a native dial ATTACH puts on the wire: one
// FT_HELLO carrying no `ns`, then an FT_OPEN with a nonzero `ns` distinct per attach (a shared wsFd puts
// several attaches on one socket, where a repeated ns is refused as a duplicate).
//
// Drives the REAL CatalogSession::Create against an in-process ix::WebSocketServer that answers
// HELLO_ACK and OPEN_ACK so Connect() returns immediately instead of burning its timeout.
//
// Two build regimes:
//   - native (!WASM_LOADABLE_EXTENSIONS): the real dial path.
//   - wasm (any variant): no ix transport and no in-process socket server; report mode='wasm_no_ix'.

#include "open_ns_selftest.hpp"
#include "duckdb/function/table_function.hpp"

#include <chrono>
#include <cstdint>
#include <string>

#ifndef WASM_LOADABLE_EXTENSIONS
#include "n6k_catalog_session.hpp"
#include "n6k_msgpack.hpp"
#include "n6k_protocol_generated.hpp"

#include <atomic>
#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXWebSocketMessage.h>
#include <ixwebsocket/IXWebSocketMessageType.h>
#include <ixwebsocket/IXWebSocketServer.h>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#endif

namespace duckdb {

namespace {

struct OpenNsResult {
	std::string mode;
	bool ok = false;
	bool listening = false;
	int32_t hellos_seen = 0;
	bool hello_has_no_ns = false;
	int32_t opens_seen = 0;
	bool ns_present = false;
	int64_t ns_first = 0;
	int64_t ns_second = 0;
	bool distinct = false;
	int64_t elapsed_ms = 0;
};

#ifndef WASM_LOADABLE_EXTENSIONS

constexpr int kAttachCount = 2;

std::string PackHelloAck() {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(3);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::HELLO_ACK));
	n6k::PackString(pk, "protocol_version");
	pk.pack(n6k::N6K_PROTOCOL_VERSION);
	n6k::PackString(pk, "capabilities");
	pk.pack_array(0);
	return std::string(sbuf.data(), sbuf.size());
}

std::string PackOpenAck(int64_t ns) {
	msgpack::sbuffer sbuf;
	msgpack::packer<msgpack::sbuffer> pk(&sbuf);
	pk.pack_map(5);
	n6k::PackString(pk, "t");
	pk.pack(static_cast<int>(n6k::FrameType::OPEN_ACK));
	n6k::PackString(pk, "ns");
	pk.pack(ns);
	n6k::PackString(pk, "max_concurrent_reqs");
	pk.pack(8);
	n6k::PackString(pk, "default_batch_credits");
	pk.pack(8);
	n6k::PackString(pk, "capabilities");
	pk.pack_array(0);
	return std::string(sbuf.data(), sbuf.size());
}

int CandidateBasePort() {
	const auto tid = std::hash<std::thread::id> {}(std::this_thread::get_id());
	return 41000 + static_cast<int>(tid % 2000);
}

OpenNsResult RunOpenNsSelftest() {
	OpenNsResult r;
	r.mode = "native";
	const auto t0 = std::chrono::steady_clock::now();
	auto stamp = [&]() {
		r.elapsed_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
	};

	std::mutex mu;
	int32_t hellos = 0;
	bool hellos_without_ns = true;
	std::vector<int64_t> ns_values;
	bool all_had_ns = true;

	std::unique_ptr<ix::WebSocketServer> server;
	const int base = CandidateBasePort();
	int port = 0;
	for (int attempt = 0; attempt < 16 && !r.listening; ++attempt) {
		const int candidate_port = base + attempt;
		std::unique_ptr<ix::WebSocketServer> candidate(new ix::WebSocketServer(candidate_port, "127.0.0.1"));
		candidate->disablePerMessageDeflate();
		candidate->setOnClientMessageCallback([&mu, &hellos, &hellos_without_ns, &ns_values,
		                                       &all_had_ns](const std::shared_ptr<ix::ConnectionState> &,
		                                                    ix::WebSocket &ws, const ix::WebSocketMessagePtr &msg) {
			if (msg->type != ix::WebSocketMessageType::Message || !msg->binary) {
				return;
			}
			msgpack::object_handle oh;
			size_t off = 0;
			try {
				oh = msgpack::unpack(msg->str.data(), msg->str.size(), off);
			} catch (...) {
				return;
			}
			const msgpack::object &h = oh.get();
			if (h.type != msgpack::type::MAP) {
				return;
			}
			const auto type = static_cast<n6k::FrameType>(n6k::GetInt(h, "t", -1));
			if (type == n6k::FrameType::HELLO) {
				{
					std::lock_guard<std::mutex> lk(mu);
					hellos++;
					if (n6k::HasKey(h, "ns")) {
						hellos_without_ns = false;
					}
				}
				ws.sendBinary(PackHelloAck());
				return;
			}
			if (type != n6k::FrameType::OPEN) {
				return;
			}
			const int64_t ns = n6k::GetInt(h, "ns", 0);
			{
				std::lock_guard<std::mutex> lk(mu);
				if (!n6k::HasKey(h, "ns")) {
					all_had_ns = false;
				}
				ns_values.push_back(ns);
			}
			ws.sendBinary(PackOpenAck(ns));
		});
		if (!candidate->listen().first) {
			continue;
		}
		candidate->start();
		server = std::move(candidate);
		r.listening = true;
		port = candidate_port;
	}
	if (!r.listening) {
		stamp();
		return r;
	}

	const std::string base_url = "http://127.0.0.1:" + std::to_string(port);
	// Hold the sessions so neither is torn down (and its ns conceptually released) before the second
	// attach runs — two live attaches is exactly the case a shared socket has to keep apart.
	std::vector<std::shared_ptr<CatalogSession>> sessions;
	for (int i = 0; i < kAttachCount; ++i) {
		try {
			sessions.push_back(CatalogSession::Create(base_url, /*token=*/"", "nstest" + std::to_string(i), "", "", -1,
			                                          StreamCancelCheck()));
		} catch (...) {
			// Connect failure is reported through hellos_seen/ok below, not swallowed silently.
		}
	}

	{
		std::lock_guard<std::mutex> lk(mu);
		r.hellos_seen = hellos;
		r.hello_has_no_ns = hellos_without_ns && hellos > 0;
		r.opens_seen = static_cast<int32_t>(ns_values.size());
		r.ns_present = all_had_ns && !ns_values.empty();
		if (!ns_values.empty()) {
			r.ns_first = ns_values[0];
		}
		if (ns_values.size() > 1) {
			r.ns_second = ns_values[1];
		}
		r.distinct = ns_values.size() == kAttachCount && ns_values[0] != ns_values[1];
	}

	sessions.clear();
	server->stop();

	r.ok = r.listening && r.hellos_seen == kAttachCount && r.hello_has_no_ns && r.opens_seen == kAttachCount &&
	       r.ns_present && r.ns_first != 0 && r.ns_second != 0 && r.distinct;
	stamp();
	return r;
}

#else

OpenNsResult RunOpenNsSelftest() {
	OpenNsResult r;
	r.mode = "wasm_no_ix";
	r.ok = true;
	return r;
}

#endif

struct N6kOpenNsBind : public TableFunctionData {};

struct N6kOpenNsState : public GlobalTableFunctionState {
	bool emitted = false;
};

unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"mode",       "ok",       "listening", "hellos_seen", "hello_has_no_ns", "opens_seen",
	         "ns_present", "ns_first", "ns_second", "ns_distinct", "elapsed_ms"};
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::INTEGER,
	                LogicalType::BOOLEAN, LogicalType::INTEGER, LogicalType::BOOLEAN, LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::BOOLEAN, LogicalType::INTEGER};
	return make_uniq<N6kOpenNsBind>();
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<N6kOpenNsState>();
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kOpenNsState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;

	const OpenNsResult r = RunOpenNsSelftest();
	output.SetValue(0, 0, Value(r.mode));
	output.SetValue(1, 0, Value::BOOLEAN(r.ok));
	output.SetValue(2, 0, Value::BOOLEAN(r.listening));
	output.SetValue(3, 0, Value::INTEGER(r.hellos_seen));
	output.SetValue(4, 0, Value::BOOLEAN(r.hello_has_no_ns));
	output.SetValue(5, 0, Value::INTEGER(r.opens_seen));
	output.SetValue(6, 0, Value::BOOLEAN(r.ns_present));
	output.SetValue(7, 0, Value::BIGINT(r.ns_first));
	output.SetValue(8, 0, Value::BIGINT(r.ns_second));
	output.SetValue(9, 0, Value::BOOLEAN(r.distinct));
	output.SetValue(10, 0, Value::INTEGER(static_cast<int32_t>(r.elapsed_ms)));
	output.SetCardinality(1);
}

} // namespace

void RegisterN6kSelftestOpenNsDistinct(ExtensionLoader &loader) {
	TableFunction fn("n6k_selftest_open_ns_distinct", {}, Scan, Bind, InitGlobal);
	loader.RegisterFunction(fn);
}

} // namespace duckdb
