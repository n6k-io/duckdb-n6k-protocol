#include "framework/n6k_twin.hpp"

#include <algorithm>
#include <chrono>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace duckdb;

namespace {

using RowList = std::vector<std::string>;

void SeedUsers(N6kTwin &twin) {
	twin.Far("CREATE TABLE db.main.users (id INTEGER, name VARCHAR, age INTEGER)");
	twin.Far("INSERT INTO db.main.users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");
}

const std::string COUNT_USERS = "SELECT count(*) FROM db.main.users";
const std::string CONNECTION_COUNT = "SELECT count(*) FROM n6k_connections()";

std::string ListCatalogs(const std::string &connection_id) {
	return "SELECT * FROM n6k_list_catalogs(connection => '" + connection_id + "')";
}

void SeedMarkers(N6kTwin &twin, const std::vector<std::string> &catalogs) {
	for (const auto &catalog : catalogs) {
		twin.Far("CREATE TABLE " + catalog + ".main.marker (catalog VARCHAR)");
		twin.Far("INSERT INTO " + catalog + ".main.marker VALUES ('" + catalog + "')");
	}
}

} // namespace

TEST_CASE("a connection over a host socket opens, shares and closes", "[twin][connection]") {
	N6kTwin twin;
	SeedUsers(twin);
	auto &client = twin.BareClient();

	SECTION("connect returns an id and the granted capabilities") {
		const auto row = client.Rows("SELECT connection_id, capabilities FROM n6k_connect(wsFd => " +
		                             std::to_string(client.OpenLink()) + ", token => '')");
		REQUIRE(row.size() == 1);
		const auto connection_id = row[0].substr(0, row[0].find('|'));
		REQUIRE(row[0].substr(row[0].find('|') + 1) == "[crossing]");
		REQUIRE(client.Rows("SELECT connection_id, alive, catalogs, leases FROM n6k_connections()") ==
		        RowList {connection_id + "|true|0|1"});
	}

	SECTION("a connection attaches and queries") {
		client.AttachAs("db", "db", client.Connect());
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});
		REQUIRE(client.Rows("SELECT catalogs FROM n6k_connections()") == RowList {"1"});
	}

	SECTION("the last lease released closes the connection") {
		const auto connection_id = client.Connect();
		client.CloseHostFd();
		client.AttachAs("db", "db", connection_id);
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});

		client.Near("DETACH db");
		REQUIRE(client.Rows("SELECT alive, catalogs FROM n6k_connections()") == RowList {"true|0"});
		client.AttachAs("db", "db", connection_id);
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});

		client.Rows("SELECT * FROM n6k_disconnect('" + connection_id + "')");
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});
		client.Near("DETACH db");
		REQUIRE(client.AwaitHostEof());
		REQUIRE(client.Rows(CONNECTION_COUNT) == RowList {"0"});
	}

	SECTION("closing the database releases the socket") {
		const auto connection_id = client.Connect();
		client.CloseHostFd();
		client.AttachAs("db", "db", connection_id);
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});
		client.CloseDatabase();
		REQUIRE(client.AwaitHostEof());
	}

	SECTION("connection ids are per database") {
		const auto connection_id = client.Connect();
		auto &other = twin.BareClient();
		other.NearErrors("ATTACH '' AS db (TYPE n6k, CONNECTION '" + connection_id + "')", "no open connection");
		REQUIRE(other.Rows(CONNECTION_COUNT) == RowList {"0"});
	}

	SECTION("bad attach options are bind errors") {
		const std::string id = "00000000-0000-4000-8000-000000000001";
		client.NearErrors("ATTACH 'n6k://localhost:1' AS db (TYPE n6k, CONNECTION '" + id + "')", "mutually exclusive");
		client.NearErrors("ATTACH '' AS db (TYPE n6k, CONNECTION '" + id + "', token 'x')", "cannot be combined");
	}

	SECTION("a second connect on the same socket shares it") {
		const auto fd = client.OpenLink();
		const auto connection_id = client.ConnectFd(fd);
		REQUIRE(client.ConnectFd(fd) == connection_id);
		REQUIRE(client.Rows("SELECT leases FROM n6k_connections()") == RowList {"2"});
	}

	SECTION("a non-socket fd is refused") {
		int pipe_fds[2];
		REQUIRE(::pipe(pipe_fds) == 0);
		client.NearErrors("SELECT * FROM n6k_connect(wsFd => " + std::to_string(pipe_fds[0]) + ", token => '')",
		                  "is not an open socket");
		::close(pipe_fds[0]);
		::close(pipe_fds[1]);
	}

	SECTION("a nonblocking fd works") {
		client.AttachAs("db", "db", client.ConnectFd(client.OpenLink(true)));
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});
	}

	SECTION("catalogs list before and after attach, repeatedly") {
		const auto connection_id = client.Connect();
		REQUIRE(client.Rows(ListCatalogs(connection_id)) == RowList {"db"});
		client.AttachAs("db", "db", connection_id);
		REQUIRE(client.Rows(ListCatalogs(connection_id)) == RowList {"db"});
		REQUIRE(client.Rows(ListCatalogs(connection_id)) == RowList {"db"});
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});
	}

	SECTION("listing after a disconnect raises") {
		const auto connection_id = client.Connect();
		client.Rows("SELECT * FROM n6k_disconnect('" + connection_id + "')");
		client.NearErrors(ListCatalogs(connection_id), "no open connection");
	}

	SECTION("a dropped socket is not redialed") {
		const auto connection_id = client.Connect();
		client.CloseHostFd();
		client.AttachAs("db", "db", connection_id);
		REQUIRE(client.Rows(COUNT_USERS) == RowList {"3"});

		client.DropSocket();
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (client.Rows("SELECT alive FROM n6k_connections()") != RowList {"false"} &&
		       std::chrono::steady_clock::now() < deadline) {
			std::this_thread::sleep_for(std::chrono::milliseconds(5));
		}
		REQUIRE(client.Rows("SELECT alive FROM n6k_connections()") == RowList {"false"});
		client.NearErrors(COUNT_USERS, "connection " + connection_id);
		client.NearErrors("SELECT * FROM n6k_reconnect('" + connection_id + "')", "cannot be redialed");
	}
}

TEST_CASE("catalogs on one socket share it and read across each other", "[twin][connection]") {
	const std::string crossed = "SELECT x.catalog, y.catalog FROM x1.main.marker x JOIN y1.main.marker y "
	                            "ON x.catalog <> y.catalog";

	SECTION("two attaches share one HELLO and take distinct namespaces") {
		N6kTwin twin({"a", "b"});
		SeedMarkers(twin, {"a", "b"});
		auto &client = twin.BareClient();
		const auto connection_id = client.Connect();
		client.AttachAs("a", "a", connection_id);
		client.AttachAs("b", "b", connection_id);

		auto listed = client.Rows(ListCatalogs(connection_id));
		std::sort(listed.begin(), listed.end());
		REQUIRE(listed == RowList {"a", "b"});
		REQUIRE(client.Rows("SELECT catalog FROM a.main.marker") == RowList {"a"});
		REQUIRE(client.Rows("SELECT catalog FROM b.main.marker") == RowList {"b"});
		REQUIRE(client.Rows("SELECT catalogs FROM n6k_connections()") == RowList {"2"});

		REQUIRE(client.SentNs(n6k::FrameType::HELLO) == std::vector<int64_t> {0});
		const auto opens = client.SentNs(n6k::FrameType::OPEN);
		REQUIRE(opens.size() == 2);
		REQUIRE(opens[0] != 0);
		REQUIRE(opens[1] != 0);
		REQUIRE(opens[0] != opens[1]);

		client.Near("DETACH a");
		REQUIRE(client.Rows("SELECT catalog FROM b.main.marker") == RowList {"b"});
		REQUIRE(client.Rows("SELECT catalogs FROM n6k_connections()") == RowList {"1"});
	}

	SECTION("reads, views and writes cross both catalogs") {
		N6kTwin twin({"a", "b"});
		SeedMarkers(twin, {"a", "b"});
		auto &client = twin.BareClient();
		const auto connection_id = client.Connect();
		client.AttachAs("x1", "a", connection_id);
		client.AttachAs("y1", "b", connection_id);
		const auto reads = [&](const std::string &sql) {
			const auto plan = client.Explain(sql);
			size_t count = 0;
			for (auto at = plan.find("Crossing Remote"); at != std::string::npos;
			     at = plan.find("Crossing Remote", at + 1)) {
				count++;
			}
			return count;
		};

		REQUIRE(reads(crossed) == 1);
		REQUIRE(client.Rows(crossed) == RowList {"a|b"});

		client.Near("CREATE VIEW x1.main.across AS " + crossed);
		REQUIRE(client.Rows("SELECT * FROM x1.main.across") == RowList {"a|b"});
		client.Near("CREATE VIEW x1.main.other_only AS SELECT catalog FROM y1.main.marker");
		REQUIRE(client.Rows("SELECT * FROM x1.main.other_only") == RowList {"b"});

		client.Near("BEGIN");
		REQUIRE(client.Rows(crossed + " ORDER BY 2") == RowList {"a|b"});
		client.Near("INSERT INTO y1.main.marker VALUES ('new')");
		REQUIRE(client.Rows(crossed + " ORDER BY 2") == RowList {"a|b", "a|new"});
		client.Near("COMMIT");
		REQUIRE(client.Rows(crossed + " ORDER BY 2") == RowList {"a|b", "a|new"});

		twin.Far("DELETE FROM b.main.marker WHERE catalog = 'new'");
		client.Near("INSERT INTO x1.main.marker SELECT y.catalog FROM y1.main.marker y JOIN x1.main.marker x "
		            "ON x.catalog <> y.catalog");
		REQUIRE(client.Rows("SELECT catalog FROM x1.main.marker ORDER BY 1") == RowList {"a", "b"});
	}

	SECTION("attaches on separate sockets read each apart") {
		N6kTwin twin({"db", "other"});
		for (const char *catalog : {"db", "other"}) {
			twin.Far(std::string("CREATE TABLE ") + catalog + ".main.users (id INTEGER)");
			twin.Far(std::string("INSERT INTO ") + catalog + ".main.users VALUES (1), (2), (3)");
		}
		auto &client = twin.BareClient();
		client.AttachAs("p1", "db", client.Connect());
		client.AttachAs("p2", "other", client.Connect());

		const std::string sql = "SELECT count(*) FROM p1.main.users u JOIN p2.main.users v ON u.id = v.id";
		const auto plan = client.Explain(sql);
		INFO(plan);
		const auto first = plan.find("Crossing Remote");
		REQUIRE(first != std::string::npos);
		REQUIRE(plan.find("Crossing Remote", first + 1) != std::string::npos);
		REQUIRE(client.Rows(sql) == RowList {"3"});
	}
}
