#include "catch.hpp"
#include "n6k_sql_builder.hpp"
#include "ws_json.hpp"

using namespace duckdb::n6k;

TEST_CASE("a table schema probe selects zero rows", "[sql_builder]") {
	REQUIRE(BuildTableSchemaSql("db", "main", "t") == R"(SELECT * FROM "db"."main"."t" LIMIT 0)");
}

TEST_CASE("create schema quotes names and whitelists on_conflict", "[sql_builder]") {
	REQUIRE(BuildCreateSchemaSql(R"(ca"t)", R"(s"1)", "error") == R"(CREATE SCHEMA "ca""t"."s""1")");
	REQUIRE(BuildCreateSchemaSql("db", "s1", "ignore") == R"(CREATE SCHEMA IF NOT EXISTS "db"."s1")");
	REQUIRE_THROWS(BuildCreateSchemaSql("db", "s1", "replace"));
	REQUIRE_THROWS(BuildCreateSchemaSql("db", "s1", ""));
}

TEST_CASE("a qualified table quotes every part and doubles embedded quotes", "[sql_builder]") {
	REQUIRE(QuoteQualifiedTable(R"(ca"t)", R"(we"ird)", R"(ta"ble)") == R"("ca""t"."we""ird"."ta""ble")");
}
