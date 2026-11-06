#include "catch.hpp"
#include "n6k_sql_builder.hpp"
#include "ws_json.hpp"

using namespace duckdb::n6k;

namespace {

std::string CreateTable(const std::string &columns) {
	JsonDoc doc(columns);
	return BuildCreateTableSql("db", "main", "t", doc.Root());
}

std::string Alter(const std::string &kind, const std::string &details) {
	JsonDoc doc(details);
	return BuildAlterSql("db", "main", "t", kind, doc.Root());
}

} // namespace

TEST_CASE("create table quotes names and leaves type expressions verbatim", "[sql_builder]") {
	REQUIRE(CreateTable(R"([{"name":"a","type":"INTEGER"},{"name":"b\"c","type":"VARCHAR"}])") ==
	        R"(CREATE TABLE "db"."main"."t" ("a" INTEGER, "b""c" VARCHAR))");
}

TEST_CASE("create table renders not_null only when true", "[sql_builder]") {
	REQUIRE(CreateTable(
	            R"([{"name":"a","type":"INTEGER","not_null":true},{"name":"b","type":"VARCHAR","not_null":false}])") ==
	        R"(CREATE TABLE "db"."main"."t" ("a" INTEGER NOT NULL, "b" VARCHAR))");
}

TEST_CASE("alter renders each whitelisted kind", "[sql_builder]") {
	REQUIRE(Alter("add_column", R"({"name":"a","type":"INTEGER"})") ==
	        R"(ALTER TABLE "db"."main"."t" ADD COLUMN "a" INTEGER)");
	REQUIRE(Alter("drop_column", R"({"name":"a"})") == R"(ALTER TABLE "db"."main"."t" DROP COLUMN "a")");
	REQUIRE(Alter("rename_column", R"({"old_name":"a\"b","new_name":"c"})") ==
	        R"(ALTER TABLE "db"."main"."t" RENAME COLUMN "a""b" TO "c")");
}

TEST_CASE("alter refuses a kind outside the whitelist", "[sql_builder]") {
	REQUIRE_THROWS_WITH(Alter("drop_table", "{}"), Catch::Contains("unsupported alter kind"));
}

TEST_CASE("a table schema probe selects zero rows", "[sql_builder]") {
	REQUIRE(BuildTableSchemaSql("db", "main", "t") == R"(SELECT * FROM "db"."main"."t" LIMIT 0)");
}

TEST_CASE("a qualified table quotes every part and doubles embedded quotes", "[sql_builder]") {
	REQUIRE(QuoteQualifiedTable(R"(ca"t)", R"(we"ird)", R"(ta"ble)") == R"("ca""t"."we""ird"."ta""ble")");
}
