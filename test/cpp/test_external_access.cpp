#include "framework/n6k_twin.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <netinet/in.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace duckdb;

namespace {

const std::string REFUSED = "external access is disabled";

class CountingListener {
public:
	CountingListener() {
		fd = ::socket(AF_INET, SOCK_STREAM, 0);
		REQUIRE(fd >= 0);
		sockaddr_in addr {};
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr.sin_port = 0;
		REQUIRE(::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
		REQUIRE(::listen(fd, 8) == 0);
		socklen_t len = sizeof(addr);
		REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) == 0);
		port = ntohs(addr.sin_port);
		acceptor = std::thread([this] {
			while (!stopping.load()) {
				struct pollfd pfd {
					fd, POLLIN, 0
				};
				if (::poll(&pfd, 1, 50) <= 0) {
					continue;
				}
				const int conn = ::accept(fd, nullptr, nullptr);
				if (conn >= 0) {
					accepted.fetch_add(1);
					::close(conn);
				}
			}
		});
	}

	~CountingListener() {
		stopping.store(true);
		acceptor.join();
		::close(fd);
	}

	CountingListener(const CountingListener &) = delete;
	CountingListener &operator=(const CountingListener &) = delete;

	int Accepted() const {
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		return accepted.load();
	}

	int port = 0;

private:
	int fd = -1;
	std::atomic<int> accepted {0};
	std::atomic<bool> stopping {false};
	std::thread acceptor;
};

} // namespace

TEST_CASE("with external access disabled no new connection opens", "[external_access]") {
	TwinNear near;
	near.Rows("SET enable_external_access = false");

	SECTION("n6k_connect over a host socket") {
		int pair[2];
		REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0);
		near.Fails("SELECT * FROM n6k_connect(wsFd => " + std::to_string(pair[1]) + ", token => 't')",
		           ExceptionType::PERMISSION, REFUSED);
		::close(pair[0]);
		::close(pair[1]);
	}

	SECTION("a url attach") {
		CountingListener listener;
		near.Fails("ATTACH 'n6k://127.0.0.1:" + std::to_string(listener.port) + "' AS x (TYPE n6k, anonymous true)",
		           ExceptionType::PERMISSION, REFUSED);
		REQUIRE(listener.Accepted() == 0);
	}

	SECTION("n6k_connect to a url") {
		CountingListener listener;
		near.Fails("SELECT * FROM n6k_connect(url => 'n6k://127.0.0.1:" + std::to_string(listener.port) +
		               "', token => 't')",
		           ExceptionType::PERMISSION, REFUSED);
		REQUIRE(listener.Accepted() == 0);
	}
}
