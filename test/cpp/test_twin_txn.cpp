#include "framework/n6k_twin.hpp"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;

namespace {

using RowList = std::vector<std::string>;

const std::string AGES = "SELECT id, age FROM db.main.keyed ORDER BY id";
const std::string OUTSIDE_COUNT = "SELECT * FROM n6k_catalog_query('db', 'SELECT count(*) FROM db.main.keyed')";

} // namespace

TEST_CASE("write transactions pin to the client's", "[twin][txn]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.keyed (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)");
	twin.Far("INSERT INTO db.main.keyed VALUES (1, 'alice', 30), (2, 'bob', 25)");
	twin.Attach();

	SECTION("rollback discards the write") {
		twin.Near("BEGIN");
		twin.Near("UPDATE db.main.keyed SET age = 99 WHERE id = 1");
		REQUIRE(twin.Rows(AGES) == RowList {"1|99", "2|25"});
		twin.Near("ROLLBACK");
		REQUIRE(twin.Rows(AGES) == RowList {"1|30", "2|25"});
	}

	SECTION("commit keeps the write") {
		twin.Near("BEGIN");
		twin.Near("UPDATE db.main.keyed SET age = 99 WHERE id = 1");
		twin.Near("INSERT INTO db.main.keyed VALUES (3, 'carol', 40)");
		twin.Near("COMMIT");
		REQUIRE(twin.Rows(AGES) == RowList {"1|99", "2|25", "3|40"});
		REQUIRE(twin.Rows(OUTSIDE_COUNT) == RowList {"3"});
	}

	SECTION("an uncommitted write is invisible outside the transaction") {
		twin.Near("BEGIN");
		twin.Near("DELETE FROM db.main.keyed WHERE id = 2");
		REQUIRE(twin.Rows(AGES) == RowList {"1|30"});
		REQUIRE(twin.Rows(OUTSIDE_COUNT) == RowList {"2"});
		REQUIRE(twin.FarRows("SELECT count(*) FROM db.main.keyed") == RowList {"2"});
		twin.Near("ROLLBACK");
		REQUIRE(twin.Rows(AGES) == RowList {"1|30", "2|25"});
	}

	SECTION("autocommit statements do not pin") {
		twin.Near("UPDATE db.main.keyed SET age = 1 WHERE id = 2");
		REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_query('db', 'SELECT age FROM db.main.keyed WHERE id = 2')") ==
		        RowList {"1"});
	}

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("DDL in a transaction is hidden from a second client until commit", "[twin][txn]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.a (id INTEGER)");
	twin.Far("INSERT INTO db.main.a VALUES (1)");
	twin.Attach();

	SECTION("rollback undoes create table") {
		twin.Near("BEGIN");
		twin.Near("CREATE TABLE db.main.r (id INTEGER)");
		twin.Near("ROLLBACK");
		twin.NearFails("SELECT * FROM db.main.r", ExceptionType::CATALOG, "r");
		twin.Client().NearFails("SELECT * FROM db.main.r", ExceptionType::CATALOG, "r");
	}

	SECTION("rollback undoes alter table") {
		twin.Near("BEGIN");
		twin.Near("ALTER TABLE db.main.a ADD COLUMN d INTEGER");
		REQUIRE(twin.Rows("SELECT * FROM db.main.a") == RowList {"1|NULL"});
		REQUIRE(twin.Client().Rows("SELECT * FROM db.main.a") == RowList {"1"});
		twin.Near("ROLLBACK");
		REQUIRE(twin.Rows("SELECT * FROM db.main.a") == RowList {"1"});
		REQUIRE(twin.Client().Rows("SELECT * FROM db.main.a") == RowList {"1"});
	}

	SECTION("commit keeps create table as") {
		twin.Near("BEGIN");
		twin.Near("CREATE TABLE db.main.c AS SELECT 1 AS id");
		REQUIRE(twin.Rows("SELECT * FROM db.main.c") == RowList {"1"});
		twin.Near("COMMIT");
		REQUIRE(twin.Client().Rows("SELECT * FROM db.main.c") == RowList {"1"});
	}

	SECTION("rollback undoes create table as") {
		twin.Near("BEGIN");
		twin.Near("CREATE TABLE db.main.c AS SELECT 1 AS id");
		twin.Near("ROLLBACK");
		twin.Client().NearFails("SELECT * FROM db.main.c", ExceptionType::CATALOG, "c");
	}

	SECTION("a new table is hidden from other clients until commit") {
		auto &connected_before = twin.Client();
		twin.Near("BEGIN");
		twin.Near("CREATE TABLE db.main.v (id INTEGER)");
		connected_before.NearFails("SELECT * FROM db.main.v", ExceptionType::CATALOG, "v");
		twin.Client().NearFails("SELECT * FROM db.main.v", ExceptionType::CATALOG, "v");
		twin.Near("COMMIT");
		REQUIRE(twin.Client().Rows("SELECT * FROM db.main.v").empty());
	}

	SECTION("a table created in a transaction reports its constraints there") {
		const std::string describe = "SELECT column_name, \"null\", key FROM (DESCRIBE db.main.t)";
		const std::string listed = "SELECT table_name FROM duckdb_tables() WHERE database_name = 'db' "
		                           "AND table_name = 't'";
		auto &other = twin.Client();
		twin.Near("BEGIN");
		twin.Near("CREATE TABLE db.main.t (id INTEGER PRIMARY KEY, code VARCHAR UNIQUE, n INTEGER NOT NULL)");
		REQUIRE(twin.Rows(describe) == RowList {"id|NO|PRI", "code|YES|UNI", "n|NO|NULL"});
		REQUIRE(twin.Rows(listed) == RowList {"t"});
		REQUIRE(other.Rows(listed).empty());
		twin.Near("ROLLBACK");
		REQUIRE(twin.Rows(listed).empty());
	}

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a stale client errors and invalidation refreshes it", "[twin][txn]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.t (a INTEGER, b INTEGER, c INTEGER)");
	twin.Far("INSERT INTO db.main.t VALUES (1, 2, 3)");
	twin.Attach();

	const std::vector<std::vector<std::string>> column_changes = {
	    {"ALTER TABLE db.main.t DROP COLUMN b"},
	    {"ALTER TABLE db.main.t DROP COLUMN c"},
	    {"ALTER TABLE db.main.t DROP COLUMN b", "ALTER TABLE db.main.t ADD COLUMN d INTEGER"},
	    {"ALTER TABLE db.main.t DROP COLUMN b", "ALTER TABLE db.main.t ADD COLUMN b INTEGER"},
	};
	for (const auto &alters : column_changes) {
		DYNAMIC_SECTION(alters.back() << " after " << alters.size() << " alters") {
			REQUIRE(twin.Rows("SELECT * FROM db.main.t") == RowList {"1|2|3"});
			auto &other = twin.Client();
			for (const auto &alter : alters) {
				other.Near(alter);
			}
			twin.NearFails("SELECT * FROM db.main.t", ExceptionType::CATALOG, "changed on the source");
		}
	}

	SECTION("n6k_invalidate_cache reloads the tables") {
		REQUIRE(twin.Rows("SELECT count(*) FROM db.main.t") == RowList {"1"});
		twin.Near("CALL n6k_invalidate_cache('db', 'main')");
		REQUIRE(twin.Rows("SELECT count(*) FROM db.main.t") == RowList {"1"});
		REQUIRE(twin.Rows("SELECT table_name FROM duckdb_tables() WHERE database_name = 'db'") == RowList {"t"});
	}

	SECTION("a push makes a new table visible") {
		REQUIRE(twin.Rows("SELECT count(*) FROM db.main.t") == RowList {"1"});
		twin.Far("CREATE TABLE db.main.appeared (i INTEGER)");
		twin.NearErrors("SELECT * FROM db.main.appeared");

		const auto notified = twin.FarRows("CALL n6k_serve_push_invalidate('db', 'main')");
		REQUIRE(notified == RowList {"1"});

		bool visible = false;
		for (int attempt = 0; attempt < 500 && !visible; attempt++) {
			visible = twin.Rows("SELECT table_name FROM duckdb_tables() WHERE database_name = 'db' "
			                    "AND table_name = 'appeared'") == RowList {"appeared"};
			if (!visible) {
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}
		REQUIRE(visible);
		REQUIRE(twin.Rows("SELECT * FROM db.main.appeared").empty());
	}

	REQUIRE(twin.ServeError().empty());
}
