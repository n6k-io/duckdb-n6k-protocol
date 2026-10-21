#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace duckdb {
namespace n6k {

// Transport needs CloseInbound(): a reactor blocked reading it must wake with EOF.
template <class Transport>
class ServeClients {
public:
	ServeClients() = default;
	ServeClients(const ServeClients &) = delete;
	ServeClients &operator=(const ServeClients &) = delete;

	// Also on unwind: each worker holds references into the serve function's frame.
	~ServeClients() {
		CloseAndJoin();
	}

	// `run` must not throw.
	template <class Run>
	void Spawn(std::shared_ptr<Transport> transport, Run run) {
		auto client = std::make_shared<Client>();
		client->transport = std::move(transport);
		std::lock_guard<std::mutex> lk(mu_);
		all_.reserve(all_.size() + 1);
		auto *raw = client.get();
		client->worker = std::thread([raw, run]() {
			run(*raw->transport);
			raw->finished.store(true);
		});
		all_.push_back(std::move(client));
	}

	void ReapFinished() {
		std::lock_guard<std::mutex> lk(mu_);
		for (auto it = all_.begin(); it != all_.end();) {
			if ((*it)->finished.load()) {
				(*it)->worker.join();
				it = all_.erase(it);
			} else {
				++it;
			}
		}
	}

	void CloseAndJoin() {
		std::vector<std::shared_ptr<Client>> all;
		{
			std::lock_guard<std::mutex> lk(mu_);
			all.swap(all_);
		}
		for (auto &client : all) {
			client->transport->CloseInbound();
		}
		for (auto &client : all) {
			client->worker.join();
		}
	}

private:
	struct Client {
		std::shared_ptr<Transport> transport;
		std::thread worker;
		std::atomic<bool> finished {false};
	};

	std::mutex mu_;
	std::vector<std::shared_ptr<Client>> all_;
};

} // namespace n6k
} // namespace duckdb
