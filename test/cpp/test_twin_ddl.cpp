#include "framework/n6k_twin.hpp"

#include <cstdio>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

using namespace duckdb;

namespace {

using RowList = std::vector<std::string>;

} // namespace

TEST_CASE("a created schema reaches the server and accepts tables", "[twin][ddl]") {
	N6kTwin twin;
	twin.Attach();

	twin.Near("CREATE SCHEMA db.s1");
	REQUIRE(twin.FarRows("SELECT schema_name FROM duckdb_schemas() WHERE schema_name = 's1'") == RowList {"s1"});

	twin.Near("CREATE TABLE db.s1.x (i INT)");
	REQUIRE(twin.FarRows("SELECT table_name FROM duckdb_tables() WHERE schema_name = 's1'") == RowList {"x"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("create schema conflicts follow the server", "[twin][ddl]") {
	N6kTwin twin;
	twin.Attach();
	const std::string schemas = "SELECT schema_name FROM duckdb_schemas() WHERE schema_name = 's1'";

	SECTION("a second create of the client's own schema fails") {
		twin.Near("CREATE SCHEMA db.s1");
		twin.NearFails("CREATE SCHEMA db.s1", ExceptionType::CATALOG, "already exists");
		twin.Near("CREATE SCHEMA IF NOT EXISTS db.s1");
		REQUIRE(twin.FarRows(schemas) == RowList {"s1"});
	}

	SECTION("a schema only the server knows conflicts") {
		twin.Far("CREATE SCHEMA db.s1");
		twin.NearFails("CREATE SCHEMA db.s1", ExceptionType::CATALOG, "already exists");
	}

	SECTION("if not exists adopts a schema only the server knows") {
		twin.Far("CREATE SCHEMA db.s1");
		twin.Near("CREATE SCHEMA IF NOT EXISTS db.s1");
		twin.Near("CREATE TABLE db.s1.x (i INT)");
		REQUIRE(twin.FarRows("SELECT table_name FROM duckdb_tables() WHERE schema_name = 's1'") == RowList {"x"});
	}

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("create schema refusals leave the server alone", "[twin][ddl]") {
	N6kTwin twin;
	twin.Attach();
	const std::string schemas = "SELECT schema_name FROM duckdb_schemas() WHERE schema_name = 's1'";

	SECTION("inside a transaction") {
		twin.Near("BEGIN");
		twin.NearFails("CREATE SCHEMA db.s1", ExceptionType::BINDER, "not supported in a transaction");
		twin.Near("ROLLBACK");
		twin.Near("BEGIN");
		twin.NearFails("CREATE SCHEMA IF NOT EXISTS db.s1", ExceptionType::BINDER, "not supported in a transaction");
		twin.Near("ROLLBACK");
		REQUIRE(twin.FarRows(schemas).empty());
		REQUIRE(twin.Rows(schemas).empty());
	}

	SECTION("a nested schema") {
		twin.NearErrors("CREATE SCHEMA db.s1.s2");
		REQUIRE(twin.FarRows(schemas).empty());
	}

	SECTION("or replace") {
		twin.NearErrors("CREATE OR REPLACE SCHEMA db.s1");
		REQUIRE(twin.FarRows(schemas).empty());
	}

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a created schema is usable however it is reached", "[twin][ddl]") {
	SECTION("in one multi-statement query") {
		N6kTwin twin;
		twin.Attach();
		twin.Near("CREATE SCHEMA db.s1; CREATE TABLE db.s1.x (i INT)");
		REQUIRE(twin.FarRows("SELECT table_name FROM duckdb_tables() WHERE schema_name = 's1'") == RowList {"x"});
		REQUIRE(twin.ServeError().empty());
	}

	SECTION("from a client that attaches afterwards") {
		N6kTwin twin;
		twin.Attach();
		twin.Near("CREATE SCHEMA db.s1");
		auto &fresh = twin.Client();
		fresh.Near("CREATE TABLE db.s1.x (i INT)");
		REQUIRE(twin.FarRows("SELECT table_name FROM duckdb_tables() WHERE schema_name = 's1'") == RowList {"x"});
		REQUIRE(twin.ServeError().empty());
	}

	SECTION("under an alias") {
		N6kTwin twin;
		twin.Attach();
		twin.AttachAs("other", "db", twin.ConnectionId());
		twin.Near("CREATE SCHEMA other.s1");
		twin.Near("CREATE TABLE other.s1.x (i INT)");
		REQUIRE(twin.FarRows("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'db'") ==
		        RowList {"s1|x"});
		REQUIRE(twin.ServeError().empty());
	}
}

TEST_CASE("table and view DDL work under an alias", "[twin][ddl]") {
	SECTION("alter reaches the server") {
		N6kTwin twin;
		twin.Attach();
		const std::string table = "db.main.t";
		const std::string columns = "SELECT column_name FROM (DESCRIBE " + table + ")";
		twin.Near("CREATE TABLE " + table +
		          " AS SELECT * FROM VALUES (1, 'alice', 30), (2, 'bob', 25) v(id, name, age)");

		twin.Near("ALTER TABLE " + table + " ALTER COLUMN age TYPE BIGINT");
		REQUIRE(twin.FarRows("SELECT typeof(age) FROM " + table + " LIMIT 1") == RowList {"BIGINT"});
		twin.Near("ALTER TABLE " + table + " RENAME TO renamed");
		REQUIRE(twin.Rows("SELECT count(*) FROM db.main.renamed") == RowList {"2"});
		twin.Near("ALTER TABLE db.main.renamed RENAME TO t");

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

TEST_CASE("table permissions report a remote table and view", "[twin][ddl]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.t (id INTEGER PRIMARY KEY, label VARCHAR)");
	twin.Far("CREATE VIEW db.main.v AS SELECT id FROM db.main.t");
	twin.Attach();

	REQUIRE(
	    twin.Rows("SELECT name, kind, verbs, primary_key FROM n6k_table_permissions('db', schema := 'main') "
	              "WHERE name IN ('t', 'v') ORDER BY name") ==
	    RowList {"t|n6k_remote|[select, insert, update, delete, alter, drop]|[id]", "v|n6k_remote|[select, drop]|[]"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("one execute creates a schema then a table in it", "[twin][ddl]") {
	N6kTwin twin;
	twin.Attach();

	twin.Near("CREATE SCHEMA db.s1; CREATE TABLE db.s1.x (i INT);");
	REQUIRE(twin.Rows("SELECT table_name FROM duckdb_tables() WHERE schema_name = 's1'") == RowList {"x"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a read-only catalog refuses create schema", "[twin][ddl]") {
	const auto path =
	    (std::filesystem::temp_directory_path() / ("n6k_twin_ro_" + std::to_string(::getpid()) + ".duckdb")).string();
	std::remove(path.c_str());
	{
		N6kTwin twin({"ro"});
		twin.Far("ATTACH '" + path + "' AS ro_seed");
		twin.Far("DETACH ro_seed");
		twin.Far("DETACH ro");
		twin.Far("ATTACH '" + path + "' AS ro (READ_ONLY)");
		twin.Attach();

		twin.NearFails("CREATE SCHEMA ro.refused", ExceptionType::PERMISSION, "does not allow CREATE SCHEMA");

		REQUIRE(twin.ServeError().empty());
	}
	std::remove(path.c_str());
}
