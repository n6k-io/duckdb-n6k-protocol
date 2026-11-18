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

TEST_CASE("an rpc call renders positional args, then named args quoted as identifiers", "[sql_builder]") {
	JsonDoc args(std::string(R"([1, "a'b"])"));
	JsonDoc named(std::string(R"({"c": 5, "we\"ird": [1, 2]})"));
	REQUIRE(BuildRpcCallSql("db", "s", "f", args.Root(), named.Root()) ==
	        R"(SELECT * FROM "db"."s"."f"(1, 'a''b', "c" := 5, "we""ird" := [1, 2]))");
	REQUIRE(BuildRpcCallSql("", "main", "f", nullptr, named.Root(), "(SELECT 1)") ==
	        R"(SELECT * FROM "f"((SELECT 1), "c" := 5, "we""ird" := [1, 2]))");
	JsonDoc not_object(std::string("[1]"));
	REQUIRE_THROWS(BuildRpcCallSql("db", "s", "f", nullptr, not_object.Root()));
}
