#include "framework/n6k_twin.hpp"

#include <string>
#include <vector>

using namespace duckdb;

namespace {

using RowList = std::vector<std::string>;

void SeedUsers(N6kTwin &twin, const std::string &catalog = "db") {
	twin.Far("CREATE TABLE " + catalog + ".main.users (id INTEGER, name VARCHAR, age INTEGER)");
	twin.Far("INSERT INTO " + catalog + ".main.users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");
}

void SeedMacros(N6kTwin &twin) {
	twin.Far("CREATE MACRO db.main.echo(a) AS TABLE (SELECT a AS arg0), (a, b) AS TABLE (SELECT a AS arg0, b AS arg1)");
	twin.Far("CREATE MACRO db.main.xy_labeled() AS TABLE (SELECT * FROM (VALUES (1.0::DOUBLE, 10.0::DOUBLE, 'a'), "
	         "(2.0::DOUBLE, 20.0::DOUBLE, 'b'), (3.0::DOUBLE, 30.0::DOUBLE, 'a')) t(x, y, label))");
	twin.Far("CREATE MACRO db.main.doubler(n) AS TABLE (SELECT n * 2 AS doubled)");
	twin.Far("CREATE MACRO db.main.sum_table(t) AS TABLE (SELECT sum(id)::BIGINT AS total FROM query_table(t))");
	twin.Far("CREATE MACRO db.main.stream_counter(cnt) AS TABLE (SELECT * FROM n6k_testing_stream_counter(cnt))");
}

} // namespace

TEST_CASE("server errors arrive typed with the catalog and op", "[twin][ops]") {
	SECTION("exec, query and local binding") {
		N6kTwin twin;
		SeedUsers(twin);
		twin.Attach();

		twin.NearFails("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM nonexistent_xyz WHERE 1=1')",
		               ExceptionType::CATALOG, "n6k[db] EXEC:");
		twin.NearFails("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM nonexistent_xyz WHERE 1=1')",
		               ExceptionType::CATALOG, "nonexistent_xyz");
		twin.NearFails("SELECT * FROM n6k_catalog_exec('db', 'NOT VALID SQL AT ALL;')", ExceptionType::PARSER,
		               "n6k[db]");
		twin.NearFails("SELECT * FROM db.main.nonexistent_table_xyz", ExceptionType::CATALOG, "nonexistent_table_xyz");

		twin.NearFails("SELECT * FROM n6k_catalog_query('db', 'SELECT * FROM nonexistent_streaming_xyz')",
		               ExceptionType::CATALOG, "n6k[db] QUERY:");
		twin.NearFails("SELECT * FROM n6k_catalog_query('db', 'SELECT * FROM nonexistent_streaming_xyz')",
		               ExceptionType::CATALOG, "nonexistent_streaming_xyz");
		twin.NearFails("SELECT * FROM n6k_catalog_query('db', 'SELECT bogus_col_xyz')", ExceptionType::BINDER,
		               "n6k[db] QUERY:");
		twin.NearFails("SELECT * FROM n6k_catalog_query('db', 'SELECT bogus_col_xyz')", ExceptionType::BINDER,
		               "bogus_col_xyz");

		twin.NearFails("SELECT * FROM n6k_catalog_query('db', 'SELECT * FROM definitely_not_a_table')",
		               ExceptionType::CATALOG, "definitely_not_a_table");
		twin.NearFails("SELECT * FROM n6k_catalog_query('db', 'SELEC 1')", ExceptionType::PARSER, "n6k[db]");
		twin.NearFails("SELECT * FROM n6k_catalog_query('db', 'SELECT no_such_column FROM db.main.users')",
		               ExceptionType::BINDER, "no_such_column");
		REQUIRE(twin.ServeError().empty());
	}

	SECTION("the prefix names the catalog that failed") {
		N6kTwin twin({"prod", "staging"});
		twin.Attach();

		const auto error =
		    twin.NearError("SELECT * FROM n6k_catalog_exec('prod', 'DELETE FROM nonexistent_xyz WHERE 1=1')");
		INFO(error);
		REQUIRE(error.find("n6k[prod]") != std::string::npos);
		REQUIRE(error.find("n6k[staging]") == std::string::npos);
	}
}

TEST_CASE("exec, query and rpc reach the server", "[twin][ops]") {
	N6kTwin twin;
	SeedUsers(twin);
	SeedMacros(twin);
	twin.Far("CREATE SCHEMA db.test_schema");
	twin.Far("CREATE TABLE db.test_schema.products (id INTEGER)");
	twin.Far("CREATE TABLE db.main.marker (catalog VARCHAR)");
	twin.Far("INSERT INTO db.main.marker VALUES ('db')");
	twin.Attach();

	SECTION("catalog query") {
		REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_query('db', 'SELECT * FROM db.main.users ORDER BY id')") ==
		        RowList {"1|Alice|30", "2|Bob|25", "3|Charlie|35"});
		REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_query('db', 'SELECT name FROM db.main.users WHERE id = 2')") ==
		        RowList {"Bob"});
		REQUIRE(twin.Rows("SELECT * FROM db.query('SELECT name FROM db.main.users ORDER BY id')") ==
		        RowList {"Alice", "Bob", "Charlie"});
	}

	SECTION("catalog exec") {
		REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_exec('db', 'UPDATE db.main.users SET name = ''Updated'' WHERE "
		                  "id = 2')") == RowList {"1"});
		REQUIRE(twin.FarRows("SELECT name FROM db.main.users WHERE id = 2") == RowList {"Updated"});
		REQUIRE(twin.Rows("SELECT * FROM db.exec('UPDATE db.main.users SET name = ''Changed'' WHERE id = 3')") ==
		        RowList {"1"});
		REQUIRE(twin.Rows("SELECT * FROM db.query('SELECT name FROM db.main.users WHERE id = 3')") ==
		        RowList {"Changed"});
	}

	SECTION("rpc dispatch") {
		REQUIRE(twin.Rows("SELECT * FROM db.echo('hello')") == RowList {"hello"});
		REQUIRE(twin.Rows("SELECT * FROM db.echo('hello', 42)") == RowList {"hello|42"});
		REQUIRE(twin.Rows("SELECT arg0, arg1 FROM db.echo('a', 'b')") == RowList {"a|b"});
		REQUIRE(twin.Rows("SELECT * FROM db.rpc('echo', 'world')") == RowList {"world"});
		REQUIRE(twin.Rows("SELECT * FROM db.rpc('echo', 'test', 99)") == RowList {"test|99"});
		REQUIRE(twin.Rows("SELECT * FROM db.main.echo('explicit')") == RowList {"explicit"});
		REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_rpc('db', 'echo', 'global')") == RowList {"global"});
		REQUIRE(twin.Rows("SELECT doubled FROM db.doubler(21)") == RowList {"42"});
	}

	SECTION("numeric rpc args arrive as numbers") {
		auto decimal = twin.Near("SELECT * FROM db.echo(1.1)");
		REQUIRE(decimal->GetTypes()[0].id() == LogicalTypeId::DECIMAL);
		REQUIRE(twin.Rows("SELECT * FROM db.echo(1.1)") == RowList {"1.1"});

		auto hugeint = twin.Near("SELECT * FROM db.echo(42::HUGEINT)");
		REQUIRE(hugeint->GetTypes()[0].id() != LogicalTypeId::VARCHAR);
		REQUIRE(twin.Rows("SELECT * FROM db.echo(42::HUGEINT)") == RowList {"42"});
	}

	SECTION("a filter on a varchar after numeric rpc columns") {
		REQUIRE(twin.Rows("SELECT * FROM db.xy_labeled() WHERE label = 'a' ORDER BY x") ==
		        RowList {"1.0|10.0|a", "3.0|30.0|a"});
	}

	SECTION("streaming rpc") {
		const RowList counted = {"0", "1", "2", "3", "4"};
		REQUIRE(twin.Rows("SELECT n FROM db.stream_counter(5) ORDER BY n") == counted);
		REQUIRE(twin.Rows("SELECT n FROM db.n6k_testing_stream_counter(5) ORDER BY n") == counted);
	}

	SECTION("table rpc binds and streams") {
		twin.Near("CREATE TABLE local_rows (id BIGINT)");
		twin.Near("INSERT INTO local_rows VALUES (10), (20), (30)");
		REQUIRE(twin.Rows("SELECT total FROM n6k_catalog_rpc_table('db', 'sum_table', (SELECT id FROM local_rows))") ==
		        RowList {"60"});
		REQUIRE(twin.Rows("SELECT total FROM n6k_catalog_rpc_table('db', 'n6k_testing_sum_table', "
		                  "(SELECT id FROM local_rows))") == RowList {"60"});
		REQUIRE(twin.Rows("SELECT total FROM n6k_catalog_rpc_table('db', 'sum_table', "
		                  "(SELECT id FROM db.main.users ORDER BY id LIMIT 3))") == RowList {"6"});
	}

	SECTION("catalog reads and inserts") {
		REQUIRE(twin.Rows("SELECT count(*) FROM db.main.users") == RowList {"3"});
		REQUIRE(twin.Rows("SELECT catalog FROM db.main.marker") == RowList {"db"});
		REQUIRE(twin.Rows("SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = 'db' "
		                  "AND table_schema NOT IN ('information_schema', 'pg_catalog') ORDER BY 1, 2") ==
		        RowList {"main|marker", "main|users", "test_schema|products"});
		REQUIRE(twin.Rows("SELECT column_name FROM (DESCRIBE db.main.users)") == RowList {"id", "name", "age"});

		twin.Near("INSERT INTO db.main.users VALUES (777, 'Ned', 77)");
		REQUIRE(twin.FarRows("SELECT count(*) FROM db.main.users WHERE id = 777") == RowList {"1"});
		twin.Near("INSERT INTO db.main.users (id, name) VALUES (778, 'Partial')");
		REQUIRE(twin.Rows("SELECT id, name, age FROM db.main.users WHERE id = 778") == RowList {"778|Partial|NULL"});
		twin.Near("INSERT INTO db.main.users (age, id, name) VALUES (42, 779, 'Reord')");
		REQUIRE(twin.Rows("SELECT id, name, age FROM db.main.users WHERE id = 779") == RowList {"779|Reord|42"});
		twin.NearErrors("INSERT INTO db.main.users VALUES (780, 'X', 1) ON CONFLICT DO NOTHING", "ON CONFLICT");
	}

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a long stream is paced by credits and a full drain sends no cancel", "[twin][ops]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.rows25 AS SELECT i AS id FROM range(25) t(i)");
	twin.Attach();

	REQUIRE(twin.Rows("SELECT count(*) FROM db.main.rows25") == RowList {"25"});
	REQUIRE(twin.Rows("SELECT id FROM db.main.rows25 ORDER BY id").size() == 25);
	REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_query('db', 'SELECT id FROM db.main.rows25 ORDER BY id')").size() ==
	        25);
	REQUIRE(twin.SentCount(n6k::FrameType::CANCEL) == 0);
	REQUIRE(twin.FarRows("SELECT cancels FROM n6k_serve_stats()") == RowList {"0"});

	twin.HoldCreditsUntilPause();
	REQUIRE(twin.Rows("SELECT n FROM db.n6k_testing_stream_counter(40000)").size() == 40000);
	REQUIRE(twin.SentCount(n6k::FrameType::CREDIT) >= 1);
	REQUIRE(twin.SentCount(n6k::FrameType::CANCEL) == 0);
	REQUIRE(twin.FarRows("SELECT credit_pauses >= 1, cancels FROM n6k_serve_stats()") == RowList {"true|0"});

	REQUIRE(twin.ServeError().empty());
}
