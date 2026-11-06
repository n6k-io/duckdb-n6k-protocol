#include "catch.hpp"
#include "n6k_server_extension.hpp"
#include "n6k_table_function.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

TEST_CASE("the client and the server load into their own databases", "[extensions]") {
	DuckDB near(nullptr);
	DuckDB far(nullptr);
	ExtensionLoader near_loader(*near.instance, "n6k_client");
	ExtensionLoader far_loader(*far.instance, "n6k_server");

	LoadInternal(near_loader);
	N6kServerLoadInternal(far_loader);

	Connection near_con(near);
	Connection far_con(far);
	auto connections = near_con.Query("SELECT count(*) FROM n6k_connections()");
	auto types = far_con.Query("SELECT count(*) FROM n6k_exception_types()");
	REQUIRE(!connections->HasError());
	REQUIRE(!types->HasError());
	REQUIRE(connections->Collection().GetValue(0, 0) == Value::BIGINT(0));
}
