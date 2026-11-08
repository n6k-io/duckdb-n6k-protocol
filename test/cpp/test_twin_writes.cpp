#include "framework/n6k_twin.hpp"

#include <string>
#include <vector>

using namespace duckdb;

namespace {

using RowList = std::vector<std::string>;

struct WriteCase {
	std::string table;
	std::string statement;
	RowList returned;
	RowList final_rows;
};

const std::string PEOPLE = "db.main.people";
const std::string KV = "db.main.kv";
const std::string NOTES = "db.main.notes";

void SeedWriteTables(N6kTwin &twin) {
	twin.Far("CREATE TABLE " + PEOPLE + " (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)");
	twin.Far("INSERT INTO " + PEOPLE + " VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35)");
	twin.Far("CREATE TABLE " + KV + " (id INTEGER PRIMARY KEY, v INTEGER)");
	twin.Far("INSERT INTO " + KV + " VALUES (1, 10), (2, 20)");
	twin.Far("CREATE TABLE " + NOTES + " (id INTEGER PRIMARY KEY, n VARCHAR)");
	twin.Far("INSERT INTO " + NOTES + " VALUES (1, 'a')");
	twin.Far("CREATE TABLE db.main.keyless (id INTEGER, age INTEGER)");
	twin.Far("INSERT INTO db.main.keyless VALUES (1, 30)");
}

} // namespace

TEST_CASE("update, delete, insert, returning, merge and conflict clauses run on the server", "[twin][writes]") {
	const RowList people = {"1|alice|30", "2|bob|25", "3|charlie|35"};
	const std::vector<WriteCase> cases = {
	    {PEOPLE,
	     "UPDATE " + PEOPLE + " SET name = 'updated' WHERE id = 2",
	     {"1"},
	     {"1|alice|30", "2|updated|25", "3|charlie|35"}},
	    {PEOPLE,
	     "UPDATE " + PEOPLE + " SET age = age + 10 WHERE id = 3",
	     {"1"},
	     {"1|alice|30", "2|bob|25", "3|charlie|45"}},
	    {PEOPLE,
	     "UPDATE " + PEOPLE + " SET age = 0 WHERE regexp_matches(name, '^[ab]')",
	     {"2"},
	     {"1|alice|0", "2|bob|0", "3|charlie|35"}},
	    {PEOPLE, "DELETE FROM " + PEOPLE + " WHERE id = 1", {"1"}, {"2|bob|25", "3|charlie|35"}},
	    {PEOPLE, "DELETE FROM " + PEOPLE + " WHERE regexp_matches(name, 'li')", {"2"}, {"2|bob|25"}},
	    {PEOPLE,
	     "INSERT INTO " + PEOPLE + " SELECT id + 10, name, age FROM " + PEOPLE + " WHERE id < 3",
	     {"2"},
	     {"1|alice|30", "2|bob|25", "3|charlie|35", "11|alice|30", "12|bob|25"}},
	    {PEOPLE,
	     "INSERT INTO " + PEOPLE + " VALUES (4, 'dave', 40), (5, 'eve', 50)",
	     {"2"},
	     {"1|alice|30", "2|bob|25", "3|charlie|35", "4|dave|40", "5|eve|50"}},
	    {PEOPLE,
	     "INSERT INTO " + PEOPLE + " VALUES (6, 'fay', 60) RETURNING id, name",
	     {"6|fay"},
	     {"1|alice|30", "2|bob|25", "3|charlie|35", "6|fay|60"}},
	    {PEOPLE,
	     "UPDATE " + PEOPLE + " SET age = 99 WHERE id = 1 RETURNING id, age",
	     {"1|99"},
	     {"1|alice|99", "2|bob|25", "3|charlie|35"}},
	    {PEOPLE, "DELETE FROM " + PEOPLE + " WHERE id = 1 RETURNING name", {"alice"}, {"2|bob|25", "3|charlie|35"}},
	    {KV, "UPDATE " + KV + " SET v = 99 WHERE id = 1", {"1"}, {"1|99", "2|20"}},
	    {KV, "DELETE FROM " + KV + " WHERE id = 1", {"1"}, {"2|20"}},
	    {KV, "DELETE FROM " + KV, {"2"}, {}},
	    {KV, "UPDATE " + KV + " SET v = 5", {"2"}, {"1|5", "2|5"}},
	    {KV, "UPDATE " + KV + " SET v = u.v FROM (VALUES (1, 7)) u(id, v) WHERE kv.id = u.id", {"1"}, {"1|7", "2|20"}},
	    {KV, "DELETE FROM " + KV + " USING (VALUES (1)) u(id) WHERE kv.id = u.id", {"1"}, {"2|20"}},
	    {KV,
	     "INSERT INTO " + KV + " SELECT * FROM (VALUES (3, 30), (4, 40)) u(id, v) WHERE id = 3",
	     {"1"},
	     {"1|10", "2|20", "3|30"}},
	    {NOTES, "INSERT INTO " + NOTES + " VALUES (1, 'x') ON CONFLICT DO NOTHING", {"0"}, {"1|a"}},
	    {NOTES, "INSERT INTO " + NOTES + " VALUES (1, 'x') ON CONFLICT (id) DO NOTHING", {"0"}, {"1|a"}},
	    {NOTES,
	     "INSERT INTO " + NOTES + " VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET n = excluded.n",
	     {"1"},
	     {"1|x"}},
	    {NOTES, "INSERT OR REPLACE INTO " + NOTES + " VALUES (1, 'x')", {"1"}, {"1|x"}},
	    {NOTES, "INSERT OR IGNORE INTO " + NOTES + " VALUES (1, 'x')", {"0"}, {"1|a"}},
	};

	for (const auto &write : cases) {
		DYNAMIC_SECTION(write.statement) {
			N6kTwin twin;
			SeedWriteTables(twin);
			twin.Attach();

			REQUIRE(twin.Rows(write.statement) == write.returned);
			REQUIRE(twin.Rows("SELECT * FROM " + write.table + " ORDER BY id") == write.final_rows);
			REQUIRE(twin.FarRows("SELECT * FROM " + write.table + " ORDER BY id") == write.final_rows);
			REQUIRE(twin.ServeError().empty());
		}
	}

	SECTION("a merge from a local table") {
		N6kTwin twin;
		SeedWriteTables(twin);
		twin.Attach();
		twin.Near("CREATE TABLE incoming(id INTEGER, name VARCHAR, age INTEGER)");
		twin.Near("INSERT INTO incoming VALUES (2, 'bob2', 26), (7, 'gus', 70)");

		twin.Near("MERGE INTO " + PEOPLE +
		          " AS t USING incoming AS i ON t.id = i.id "
		          "WHEN MATCHED THEN UPDATE SET name = i.name, age = i.age "
		          "WHEN NOT MATCHED THEN INSERT VALUES (i.id, i.name, i.age)");

		REQUIRE(twin.FarRows("SELECT * FROM " + PEOPLE + " ORDER BY id") ==
		        RowList {"1|alice|30", "2|bob2|26", "3|charlie|35", "7|gus|70"});
	}

	SECTION("an update and a delete joined to a local table") {
		N6kTwin twin;
		SeedWriteTables(twin);
		twin.Attach();
		twin.Near("CREATE TABLE incoming(id INTEGER, v INTEGER)");
		twin.Near("INSERT INTO incoming VALUES (1, 11), (9, 99)");

		REQUIRE(twin.Rows("UPDATE " + KV + " SET v = i.v FROM incoming i WHERE kv.id = i.id") == RowList {"1"});
		REQUIRE(twin.FarRows("SELECT * FROM " + KV + " ORDER BY id") == RowList {"1|11", "2|20"});

		REQUIRE(twin.Rows("DELETE FROM " + KV + " USING incoming i WHERE kv.id = i.id") == RowList {"1"});
		REQUIRE(twin.FarRows("SELECT * FROM " + KV + " ORDER BY id") == RowList {"2|20"});
		REQUIRE(twin.ServeError().empty());
	}

	SECTION("a merge from a constant") {
		N6kTwin twin;
		SeedWriteTables(twin);
		twin.Attach();

		twin.Near("MERGE INTO " + KV +
		          " t USING (SELECT 1 id, 5 v) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v");

		REQUIRE(twin.FarRows("SELECT * FROM " + KV + " ORDER BY id") == RowList {"1|5", "2|20"});
	}

	SECTION("a keyless table updates and deletes") {
		N6kTwin twin;
		SeedWriteTables(twin);
		twin.Attach();

		twin.Near("UPDATE db.main.keyless SET age = 1 WHERE id = 1");
		REQUIRE(twin.FarRows("SELECT age FROM db.main.keyless") == RowList {"1"});
		twin.Near("DELETE FROM db.main.keyless WHERE id IN (SELECT id FROM db.main.keyless)");
		REQUIRE(twin.FarRows("SELECT age FROM db.main.keyless") == RowList {});
	}
}

TEST_CASE("an insert into an ENUM column keeps valid labels and refuses others", "[twin][writes]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.e (i INTEGER, v ENUM('x', 'y'))");
	twin.Attach();

	twin.Near("INSERT INTO db.main.e VALUES (1, 'x')");
	REQUIRE(twin.FarRows("SELECT * FROM db.main.e") == RowList {"1|x"});

	twin.NearErrors("INSERT INTO db.main.e VALUES (2, 'nope')");
	REQUIRE(twin.FarRows("SELECT * FROM db.main.e") == RowList {"1|x"});
	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("constraints, defaults and generated columns reach the server and are enforced", "[twin][writes]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.a (id INTEGER)");
	twin.Far("INSERT INTO db.main.a VALUES (1)");
	twin.Far("CREATE TABLE db.main.empty_table (id INTEGER)");
	twin.Far("CREATE TABLE db.main.described (id INTEGER PRIMARY KEY, n VARCHAR NOT NULL, x INTEGER)");
	twin.Attach();

	SECTION("a primary key rejects duplicates") {
		twin.Near("CREATE TABLE db.main.k (id INTEGER PRIMARY KEY, v INTEGER)");
		twin.Near("INSERT INTO db.main.k VALUES (1, 10)");
		twin.NearFails("INSERT INTO db.main.k VALUES (1, 20)", ExceptionType::CONSTRAINT, "");
		REQUIRE(twin.Rows("SELECT * FROM db.main.k") == RowList {"1|10"});
	}

	SECTION("not null is enforced") {
		twin.Near("CREATE TABLE db.main.k (id INTEGER, v INTEGER NOT NULL)");
		twin.NearFails("INSERT INTO db.main.k VALUES (1, NULL)", ExceptionType::CONSTRAINT, "");
	}

	SECTION("unique is enforced") {
		twin.Near("CREATE TABLE db.main.k (id INTEGER, u INTEGER UNIQUE)");
		twin.Near("INSERT INTO db.main.k VALUES (1, 10)");
		twin.NearFails("INSERT INTO db.main.k VALUES (2, 10)", ExceptionType::CONSTRAINT, "");
	}

	SECTION("a default applies to a column an insert leaves out") {
		twin.Near("CREATE TABLE db.main.k (id INTEGER, v INTEGER DEFAULT 7)");
		twin.Near("INSERT INTO db.main.k (id) VALUES (1)");
		REQUIRE(twin.Rows("SELECT * FROM db.main.k") == RowList {"1|7"});
	}

	SECTION("a check is enforced") {
		twin.Near("CREATE TABLE db.main.k (id INTEGER, v INTEGER CHECK (v > 0))");
		twin.Near("INSERT INTO db.main.k VALUES (1, 1)");
		twin.NearFails("INSERT INTO db.main.k VALUES (2, -1)", ExceptionType::CONSTRAINT, "");
		REQUIRE(twin.Rows("SELECT * FROM db.main.k") == RowList {"1|1"});
	}

	SECTION("a check refuses the first row inserted") {
		twin.Near("CREATE TABLE db.main.t_c (i INTEGER CHECK (i > 0))");
		twin.NearFails("INSERT INTO db.main.t_c VALUES (-1)", ExceptionType::CONSTRAINT, "");
		REQUIRE(twin.Rows("SELECT count(*) FROM db.main.t_c") == RowList {"0"});
	}

	SECTION("a generated column reads back") {
		twin.Near("CREATE TABLE db.main.k (id INTEGER, v INTEGER GENERATED ALWAYS AS (id + 1))");
		twin.Near("INSERT INTO db.main.k (id) VALUES (1)");
		REQUIRE(twin.Rows("SELECT * FROM db.main.k") == RowList {"1|2"});
	}

	SECTION("adding a column with a default fills existing rows") {
		twin.Near("ALTER TABLE db.main.a ADD COLUMN d INTEGER DEFAULT 7");
		REQUIRE(twin.Client().Rows("SELECT * FROM db.main.a") == RowList {"1|7"});
	}

	SECTION("drop table reaches the server") {
		twin.Near("DROP TABLE db.main.a");
		twin.Client().NearFails("SELECT * FROM db.main.a", ExceptionType::CATALOG, "a");
		REQUIRE(twin.FarRows("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'a'") == RowList {"0"});
	}

	SECTION("adding a column without a default works") {
		twin.Near("ALTER TABLE db.main.a ADD COLUMN d INTEGER");
		REQUIRE(twin.Client().Rows("SELECT * FROM db.main.a") == RowList {"1|NULL"});
	}

	SECTION("create index is refused and the client survives") {
		twin.NearFails("CREATE INDEX idx ON db.main.empty_table(id)", ExceptionType::BINDER, "not supported");
		twin.NearFails("CREATE INDEX idx ON db.main.a(id)", ExceptionType::BINDER, "not supported");
		REQUIRE(twin.Rows("SELECT 1") == RowList {"1"});
	}

	SECTION("describe shows the server's constraints") {
		REQUIRE(twin.Rows("SELECT column_name, \"null\", key FROM (DESCRIBE db.main.described)") ==
		        RowList {"id|NO|PRI", "n|NO|NULL", "x|YES|NULL"});
	}

	SECTION("describe shows crossing's source default on every column") {
		twin.Near("CREATE TABLE db.main.t_k (i BIGINT PRIMARY KEY, v VARCHAR)");
		REQUIRE(twin.Rows("SELECT column_name, \"default\" FROM (DESCRIBE db.main.t_k)") ==
		        RowList {"i|set_by_source()", "v|set_by_source()"});
	}

	REQUIRE(twin.ServeError().empty());
}
