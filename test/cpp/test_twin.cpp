#include "framework/n6k_twin.hpp"

using namespace duckdb;

TEST_CASE("a filter crosses to the server and the rows match", "[twin]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.users (id INTEGER, name VARCHAR, age INTEGER)");
	twin.Far("INSERT INTO db.main.users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");
	twin.Attach();

	twin.Same("SELECT name FROM db.main.users WHERE age > 26 ORDER BY id");

	const auto plan = twin.LastPlan();
	REQUIRE(plan.Has(FILTER));
	REQUIRE(plan.Tables() == std::vector<std::string> {"users"});
	REQUIRE(twin.ServeError().empty());
}
