#include "framework/n6k_twin.hpp"

#include <string>

using namespace duckdb;

namespace {

const std::string ENDLESS_COUNT = "SELECT count(*) c FROM range(100000000000000) a WHERE far_tick(a.range) % 7 = 3";
const std::string SLOW_COUNT = "SELECT count(*) c FROM range(4000000000) a WHERE far_tick(a.range) % 7 = 3";

std::string Quoted(const std::string &sql) {
	std::string quoted;
	for (char c : sql) {
		quoted += c;
		if (c == '\'') {
			quoted += c;
		}
	}
	return quoted;
}

} // namespace

TEST_CASE("cancel stops the query on the server", "[twin][cancel]") {
	N6kTwin twin;

	SECTION("a scan of a view") {
		twin.Far("CREATE VIEW db.main.endless AS " + ENDLESS_COUNT);
		twin.Attach();
		twin.NearInterrupted("SELECT * FROM db.main.endless");
	}

	SECTION("a passthrough query") {
		twin.Attach();
		twin.NearInterrupted("SELECT * FROM n6k_catalog_query('db', '" + Quoted(ENDLESS_COUNT) + "')");
	}

	REQUIRE(twin.FarQuiet());
	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a cancelled write leaves no row once the server is quiet", "[twin][cancel]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.out (c BIGINT)");
	twin.Far("CREATE VIEW db.main.slow AS " + SLOW_COUNT);
	twin.Attach();
	const std::string insert = "INSERT INTO db.main.out SELECT c FROM db.main.slow";

	SECTION("a crossing insert") {
		twin.NearInterrupted(insert);
	}

	SECTION("a passthrough insert") {
		twin.NearInterrupted("SELECT * FROM n6k_catalog_exec('db', 'INSERT INTO db.main.out " + Quoted(SLOW_COUNT) +
		                     "')");
	}

	for (const char *ending : {"ROLLBACK", "COMMIT"}) {
		DYNAMIC_SECTION("an insert in a transaction, then " << ending) {
			twin.Near("BEGIN");
			twin.NearInterrupted(insert);
			twin.NearIgnoringError(ending);
		}
	}

	REQUIRE(twin.FarQuiet());
	REQUIRE(twin.FarRows("SELECT * FROM db.main.out").empty());
	REQUIRE(twin.Client().Rows("SELECT * FROM db.main.out").empty());
	REQUIRE(twin.ServeError().empty());
}
