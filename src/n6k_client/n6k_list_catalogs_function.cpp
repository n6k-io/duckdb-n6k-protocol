#include "n6k_list_catalogs_function.hpp"

#include "n6k_protocol_generated.hpp"
#include "n6k_served.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>

#if !defined(WASM_LOADABLE_EXTENSIONS) && !defined(_WIN32)
#define N6K_HAS_FD_TRANSPORT 1
#include "n6k_msgpack.hpp"
#include "ws_fd_hub.hpp"
#include "ws_msgpack.hpp"

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#endif

namespace duckdb {
namespace {

static constexpr const char *FN_NAME = "n6k_list_catalogs";

struct N6kListCatalogsBindData : public TableFunctionData {
	int fd = -1;
};

struct N6kListCatalogsState : public GlobalTableFunctionState {
	vector<string> catalogs;
	idx_t offset = 0;
};

#ifdef N6K_HAS_FD_TRANSPORT

static constexpr std::chrono::seconds LIST_TIMEOUT {10};

struct Waiter {
	std::mutex mu;
	std::condition_variable cv;
	vector<string> catalogs;
	string error;
	bool done = false;
};

static void Finish(Waiter &w, string error) {
	{
		std::lock_guard<std::mutex> lk(w.mu);
		if (w.done) {
			return;
		}
		w.error = std::move(error);
		w.done = true;
	}
	w.cv.notify_all();
}

static void OnFrame(Waiter &w, const std::string &frame) {
	msgpack::object_handle oh;
	try {
		size_t off = 0;
		oh = msgpack::unpack(frame.data(), frame.size(), off);
	} catch (const std::exception &e) {
		Finish(w, string("malformed reply: ") + e.what());
		return;
	}
	const msgpack::object &h = oh.get();
	if (h.type != msgpack::type::MAP) {
		Finish(w, "malformed reply: header is not a msgpack map");
		return;
	}
	const auto type = static_cast<n6k::FrameType>(n6k::GetInt(h, "t", -1));
	if (type == n6k::FrameType::RESP_CHUNK) {
		auto chunk = n6k::GetStrArray(h, "catalogs");
		std::lock_guard<std::mutex> lk(w.mu);
		w.catalogs.insert(w.catalogs.end(), chunk.begin(), chunk.end());
	} else if (type == n6k::FrameType::RESP_END) {
		Finish(w, string());
	} else if (type == n6k::FrameType::ERR) {
		Finish(w, n6k::GetStr(h, "exception_message"));
	}
}

static vector<string> ListCatalogs(int host_fd) {
	auto hub = n6k::FdHub::Acquire(host_fd);
	auto waiter = std::make_shared<Waiter>();
	const uint32_t req_id = hub->BindRequestAndStartReader(
	    [waiter](const std::string &frame, bool) { OnFrame(*waiter, frame); },
	    [waiter](const std::string &reason) { Finish(*waiter, "connection closed: " + reason); });

	const bool sent =
	    hub->SendLengthPrefixed(n6k::StreamFrameType::N6K, n6k::PackConnectionReq(static_cast<int>(n6k::FrameType::REQ),
	                                                                              req_id, n6k::OP_CATALOG_LIST));
	if (!sent) {
		hub->UnbindRequest(req_id);
		throw IOException("%s: writing the request to wsFd %d failed", FN_NAME, host_fd);
	}

	std::unique_lock<std::mutex> lk(waiter->mu);
	const bool done = waiter->cv.wait_for(lk, LIST_TIMEOUT, [&] { return waiter->done; });
	lk.unlock();
	hub->UnbindRequest(req_id);
	if (!done) {
		throw IOException("%s: no reply within %llu seconds", FN_NAME, static_cast<uint64_t>(LIST_TIMEOUT.count()));
	}
	if (!waiter->error.empty()) {
		throw IOException("%s: %s", FN_NAME, waiter->error);
	}
	return std::move(waiter->catalogs);
}

#endif

unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                              vector<string> &names) {
	names = {"catalog_name"};
	return_types = {LogicalType::VARCHAR};
	n6k::ThrowIfServed(context, FN_NAME);
#ifndef N6K_HAS_FD_TRANSPORT
	throw NotImplementedException("%s: wsFd is only supported in the native, non-Windows build", FN_NAME);
#else
	auto result = make_uniq<N6kListCatalogsBindData>();
	auto fd_it = input.named_parameters.find("wsFd");
	if (fd_it == input.named_parameters.end() || fd_it->second.IsNull()) {
		throw BinderException("%s requires wsFd: SELECT * FROM %s(wsFd => 7)", FN_NAME, FN_NAME);
	}
	const auto fd = fd_it->second.GetValue<int64_t>();
	if (fd < 0 || fd > static_cast<int64_t>(NumericLimits<int32_t>::Maximum())) {
		throw BinderException("%s: %s is not a valid file descriptor", FN_NAME, std::to_string(fd));
	}
	result->fd = static_cast<int>(fd);
	return std::move(result);
#endif
}

unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &, TableFunctionInitInput &input) {
	auto state = make_uniq<N6kListCatalogsState>();
#ifdef N6K_HAS_FD_TRANSPORT
	state->catalogs = ListCatalogs(input.bind_data->Cast<N6kListCatalogsBindData>().fd);
#else
	(void)input;
#endif
	return std::move(state);
}

void Scan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<N6kListCatalogsState>();
	idx_t count = 0;
	while (state.offset < state.catalogs.size() && count < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, count, Value(state.catalogs[state.offset]));
		state.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

void RegisterN6kListCatalogs(ExtensionLoader &loader) {
	TableFunction fn(FN_NAME, {}, Scan, Bind, InitGlobal);
	fn.named_parameters["wsFd"] = LogicalType::BIGINT;
	loader.RegisterFunction(fn);
}

} // namespace duckdb
