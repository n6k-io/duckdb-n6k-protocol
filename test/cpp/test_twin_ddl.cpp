#include "framework/n6k_twin.hpp"

#include <string>
#include <vector>

using namespace duckdb;

namespace {

using RowList = std::vector<std::string>;

} // namespace

TEST_CASE("a created schema reaches the server and accepts tables", "[twin][ddl][!shouldfail]") {
	N6kTwin twin;
	twin.Attach();

	twin.Near("CREATE SCHEMA db.s1");
	REQUIRE(twin.FarRows("SELECT schema_name FROM duckdb_schemas() WHERE schema_name = 's1'") == RowList {"s1"});

	twin.Near("CREATE TABLE db.s1.x (i INT)");
	REQUIRE(twin.FarRows("SELECT table_name FROM duckdb_tables() WHERE schema_name = 's1'") == RowList {"x"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("table and view DDL work under an alias", "[twin][ddl]") {
	SECTION("alter renders each kind and refuses the rest") {
		N6kTwin twin;
		twin.Attach();
		const std::string table = "db.main.t";
		const std::string columns = "SELECT column_name FROM (DESCRIBE " + table + ")";
		twin.Near("CREATE TABLE " + table +
		          " AS SELECT * FROM VALUES (1, 'alice', 30), (2, 'bob', 25) v(id, name, age)");

		twin.NearFails("ALTER TABLE " + table + " ALTER COLUMN age TYPE BIGINT", ExceptionType::BINDER,
		               "not supported");
		twin.NearFails("ALTER TABLE " + table + " RENAME TO renamed", ExceptionType::BINDER, "not supported");

		twin.Near("ALTER TABLE " + table + " ADD COLUMN score DOUBLE");
		REQUIRE(twin.Rows(columns) == RowList {"id", "name", "age", "score"});
		twin.Near("ALTER TABLE " + table + " DROP COLUMN age");
		REQUIRE(twin.Rows(columns) == RowList {"id", "name", "score"});
		twin.Near("ALTER TABLE " + table + " RENAME COLUMN name TO label");
		REQUIRE(twin.Rows(columns) == RowList {"id", "label", "score"});
		twin.Near("ALTER TABLE " + table + " ADD COLUMN note VARCHAR");
		REQUIRE(twin.Rows("SELECT id, note FROM " + table + " ORDER BY id") == RowList {"1|NULL", "2|NULL"});

		REQUIRE(twin.ServeError().empty());
	}

	for (const std::string alias : {"other", "db"}) {
		DYNAMIC_SECTION("views under the alias " << alias) {
			N6kTwin twin;
			twin.Far("CREATE TABLE db.main.users (id INTEGER, name VARCHAR, age INTEGER)");
			twin.Far("INSERT INTO db.main.users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");
			twin.Attach();
			if (alias != "db") {
				twin.AttachAs(alias, "db", twin.ConnectionId());
			}
			auto &other_client = twin.Client();

			const auto schema = alias + ".main.";
			const auto users = schema + "users";
			const RowList ids = {"1", "2", "3"};
			const auto stored = [&](const std::string &view) {
				return twin.FarRows("SELECT sql FROM duckdb_views() WHERE view_name = '" + view + "'");
			};

			twin.Near("CREATE VIEW " + schema + "v1 AS SELECT id, name FROM " + users);
			REQUIRE(twin.Rows("SELECT id, name FROM " + schema + "v1 ORDER BY id") ==
			        RowList {"1|Alice", "2|Bob", "3|Charlie"});

			const auto stored_sql = stored("v1");
			REQUIRE(stored_sql.size() == 1);
			INFO(stored_sql[0]);
			REQUIRE(stored_sql[0].find(alias + ".") == std::string::npos);
			REQUIRE(stored_sql[0].find("db.") == std::string::npos);

			twin.Near("CREATE VIEW " + schema + "v3 AS SELECT id FROM " + users);
			twin.NearErrors("CREATE VIEW " + schema + "v3 AS SELECT name FROM " + users);
			twin.Near("CREATE VIEW IF NOT EXISTS " + schema + "v3 AS SELECT name FROM " + users);
			REQUIRE(twin.Rows("SELECT * FROM " + schema + "v3 ORDER BY 1") == ids);
			twin.Near("CREATE OR REPLACE VIEW " + schema + "v3 AS SELECT name FROM " + users);
			REQUIRE(twin.Rows("SELECT * FROM " + schema + "v3 ORDER BY 1") == RowList {"Alice", "Bob", "Charlie"});

			twin.Near("CREATE VIEW " + schema + "v4 AS SELECT id FROM " + users);
			twin.Near("DROP VIEW " + schema + "v4");
			REQUIRE(stored("v4").empty());
			twin.NearErrors("SELECT * FROM " + schema + "v4");
			twin.Near("DROP VIEW IF EXISTS " + schema + "v4");
			twin.NearErrors("DROP VIEW " + schema + "v4");

			twin.Near("CREATE VIEW " + schema + "v5(a, b) AS SELECT u.id, v.name FROM " + users + " u JOIN " + users +
			          " v ON u.id = v.id WHERE u.age > 26");
			REQUIRE(twin.Rows("SELECT column_name FROM (DESCRIBE " + schema + "v5)") == RowList {"a", "b"});
			REQUIRE(twin.Rows("SELECT a, b FROM " + schema + "v5 ORDER BY a") == RowList {"1|Alice", "3|Charlie"});

			twin.Near("USE " + alias);
			twin.Near("CREATE VIEW v6 AS SELECT id FROM users");
			REQUIRE(twin.Rows("SELECT * FROM v6 ORDER BY 1") == ids);
			twin.Near("USE memory");

			twin.Near("BEGIN");
			twin.Near("CREATE VIEW " + schema + "v7 AS SELECT id FROM " + users);
			REQUIRE(twin.Rows("SELECT count(*) FROM " + schema + "v7") == RowList {"3"});
			const auto listed = "SELECT table_name FROM duckdb_tables() WHERE database_name = '";
			REQUIRE(twin.Rows(std::string(listed) + alias + "' AND table_name = 'v7'") == RowList {"v7"});
			REQUIRE(other_client.Rows(std::string(listed) + "db' AND table_name = 'v7'").empty());
			other_client.NearErrors("SELECT * FROM db.main.v7");
			twin.Near("DROP VIEW " + schema + "v7");
			twin.NearErrors("SELECT * FROM " + schema + "v7");
			twin.Near("COMMIT");
			REQUIRE(stored("v7").empty());

			twin.Near("BEGIN");
			twin.Near("CREATE VIEW " + schema + "v8 AS SELECT id FROM " + users);
			twin.Near("ROLLBACK");
			REQUIRE(stored("v8").empty());
			twin.NearErrors("SELECT * FROM " + schema + "v8");

			const std::string refused = "does not run wholly on the source";
			twin.Near("CREATE TABLE memory.main.local_t (x INTEGER)");
			twin.NearErrors("CREATE VIEW " + schema + "v9 AS SELECT 1 AS x", refused);
			twin.NearErrors("CREATE VIEW " + schema + "v9 AS SELECT u.id FROM " + users +
			                    " u JOIN memory.main.local_t l ON u.id = l.x",
			                refused);
			twin.AttachAs("elsewhere", "db", twin.Connect());
			twin.NearErrors("CREATE VIEW " + schema + "v9 AS SELECT id FROM elsewhere.main.users", refused);
			REQUIRE(stored("v9").empty());

			REQUIRE(twin.ServeError().empty());
		}
	}
}
