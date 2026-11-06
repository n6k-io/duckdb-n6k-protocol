#include "catch.hpp"
#include "ws_json.hpp"

using namespace duckdb::n6k;

TEST_CASE("a string array reads in order", "[ws_json]") {
	JsonDoc doc(R"({"schemas":["main","x'y",""]})");

	REQUIRE(JsonGetStrArray(doc.Root(), "schemas") == std::vector<std::string> {"main", "x'y", ""});
}

TEST_CASE("a missing key or null object reads as empty", "[ws_json]") {
	JsonDoc doc("{}");

	REQUIRE(JsonGetStrArray(doc.Root(), "schemas").empty());
	REQUIRE(JsonGetStrArray(nullptr, "schemas").empty());
}

TEST_CASE("anything but an array of strings is refused", "[ws_json]") {
	const std::string json = GENERATE(as<std::string> {}, R"({"schemas":{"a":1}})", R"({"schemas":[1,null,{"a":1}]})",
	                                  R"({"schemas":"main"})", R"({"schemas":7})");
	INFO(json);
	JsonDoc doc(json);

	REQUIRE_THROWS_WITH(JsonGetStrArray(doc.Root(), "schemas"), Catch::Contains("must be an array of strings"));
}
