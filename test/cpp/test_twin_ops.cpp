#include "framework/n6k_twin.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"

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

struct KwEchoData : public TableFunctionData {
	vector<Value> row;
};

struct KwEchoState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<FunctionData> KwEchoBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &types,
                                    vector<Identifier> &names) {
	auto data = make_uniq<KwEchoData>();
	names = {Identifier("a"), Identifier("scale"), Identifier("label")};
	types = {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR};
	data->row.push_back(input.inputs[0]);
	for (auto option : {"scale", "label"}) {
		auto it = input.named_parameters.find(Identifier(option));
		data->row.push_back(it == input.named_parameters.end() ? Value() : it->second);
	}
	data->row[1] = data->row[1].DefaultCastAs(LogicalType::INTEGER);
	data->row[2] = data->row[2].DefaultCastAs(LogicalType::VARCHAR);
	return std::move(data);
}

unique_ptr<GlobalTableFunctionState> KwEchoInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<KwEchoState>();
}

void KwEchoScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<KwEchoState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &row = input.bind_data->Cast<KwEchoData>().row;
	for (idx_t i = 0; i < row.size(); i++) {
		output.SetValue(i, 0, row[i]);
	}
	output.SetCardinality(1);
}

TableFunction KwEcho() {
	TableFunction function("kw_echo", {}, KwEchoScan, KwEchoBind, KwEchoInit);
	function.GetSignature().AddParameter(Identifier("a"), LogicalType::INTEGER);
	function.GetSignature().WithTypedKwargs(Identifier("options"), [](TypedKwargs &options) {
		options.Add(Identifier("scale"), LogicalType::INTEGER);
		options.Add(Identifier("label"), LogicalType::VARCHAR);
	});
	return function;
}

void SeedSignatures(N6kTwin &twin) {
	SeedMacros(twin);
	twin.Far("CREATE MACRO db.main.greet(name, greeting := 'hi') AS TABLE (SELECT greeting || ' ' || name AS msg)");
	twin.Far("CREATE MACRO db.main.three(a, b := 2, c := 3) AS TABLE (SELECT a, b, c)");
	twin.Far("CREATE MACRO db.main.typed_add(a INTEGER, b INTEGER := 10) AS TABLE (SELECT a + b AS total)");
	twin.Far("CREATE MACRO db.main.pick(a INTEGER) AS TABLE (SELECT 1 AS x), (a BIGINT) AS TABLE (SELECT 2 AS x)");
	twin.Far("COMMENT ON MACRO TABLE db.main.greet IS 'says hello'");
	twin.Far("CREATE SCHEMA db.other");
	twin.Far("CREATE MACRO db.other.echo_other(x) AS TABLE (SELECT x AS other_arg)");
	twin.FarTableFunction("db", "main", KwEcho());
}

const char *FUNCTIONS_SQL = "SELECT schema_name, function_name, parameters, varargs, comment, description, examples, "
                            "internal "
                            "FROM duckdb_functions() WHERE database_name = 'db' ORDER BY ALL";

} // namespace

TEST_CASE("attached table functions bind like the server's", "[twin][ops][functions]") {
	N6kTwin twin;
	SeedSignatures(twin);
	twin.Attach();

	SECTION("duckdb_functions lists what the server has, right after ATTACH") {
		twin.Same(FUNCTIONS_SQL);
		twin.Same("SELECT function_name, function_type, parameters, parameter_types FROM duckdb_functions() "
		          "WHERE database_name = 'db' AND function_name = 'kw_echo' ORDER BY ALL");
		REQUIRE(twin.Rows("SELECT count(*) FROM duckdb_functions() WHERE database_name = 'db' AND function_name IN "
		                  "('exec', 'query', 'rpc')") == RowList {"0"});
	}

	SECTION("calls") {
		for (const std::string sql : {
		         "SELECT * FROM db.greet('bob')",
		         "SELECT * FROM db.greet('bob', 'yo')",
		         "SELECT * FROM db.greet(greeting := 'hey', name := 'al')",
		         "SELECT * FROM db.greet(name := 'z')",
		         "SELECT * FROM db.three(1)",
		         "SELECT * FROM db.three(1, 7, 8)",
		         "SELECT * FROM db.three(1, c := 5)",
		         "SELECT * FROM db.three(a := 1, b := 9)",
		         "SELECT * FROM db.typed_add(1)",
		         "SELECT * FROM db.typed_add(1, b := 2)",
		         "SELECT * FROM db.pick(1)",
		         "SELECT * FROM db.other.echo_other(4)",
		         "SELECT * FROM db.main.echo('a', 'b')",
		         "SELECT * FROM db.kw_echo(3)",
		         "SELECT * FROM db.kw_echo(3, scale := 2)",
		         "SELECT * FROM db.kw_echo(3, label := 'x', scale := 4)",
		     }) {
			twin.Same(sql);
		}
	}

	SECTION("errors") {
		for (const std::string sql : {
		         "SELECT * FROM db.greet()",
		         "SELECT * FROM db.greet('a', 'b', 'c')",
		         "SELECT * FROM db.greet(nme := 'x')",
		         "SELECT * FROM db.pick(1.5)",
		         "SELECT * FROM db.kw_echo(3, sclae := 2)",
		         "SELECT * FROM db.nope(1)",
		         "SELECT * FROM db.main.nope(1)",
		         "SELECT * FROM db.other.greet('x')",
		         "SELECT * FROM db.n6k_testing_stream_counter(5)",
		         "SELECT * FROM db.main.range(2)",
		     }) {
			twin.SameError(sql);
		}
		twin.NearFails("SELECT * FROM db.nope(1)", ExceptionType::CATALOG, "does not exist");
	}

	SECTION("drop, create and a changed signature follow an invalidate") {
		const auto invalidate = "SELECT * FROM n6k_invalidate_cache('db', 'main')";
		twin.Far("DROP MACRO TABLE db.main.greet");
		twin.Rows(invalidate);
		twin.SameError("SELECT * FROM db.greet('x')");
		twin.Same(FUNCTIONS_SQL);

		twin.Far("CREATE MACRO db.main.fresh(x) AS TABLE (SELECT x + 1 AS y)");
		twin.Rows(invalidate);
		twin.Same("SELECT * FROM db.fresh(1)");
		twin.Same(FUNCTIONS_SQL);

		twin.Far("CREATE OR REPLACE MACRO db.main.doubler(n, m := 3) AS TABLE (SELECT n * m AS doubled)");
		twin.Rows(invalidate);
		twin.Same("SELECT * FROM db.doubler(2)");
		twin.Same("SELECT * FROM db.doubler(2, m := 5)");
		twin.Same(FUNCTIONS_SQL);
	}

	SECTION("a statement prepared before an invalidate still runs") {
		auto prepared = twin.NearPrepare("SELECT * FROM db.greet('bob')");
		twin.Far(
		    "CREATE OR REPLACE MACRO db.main.greet(name, greeting := 'yo') AS TABLE (SELECT greeting || name AS m)");
		twin.Rows("SELECT * FROM n6k_invalidate_cache('db', 'main')");
		auto result = prepared->Execute();
		INFO((result->HasError() ? result->GetError() : "ran"));
		twin.Same(FUNCTIONS_SQL);
		twin.Same("SELECT * FROM db.greet('bob')");
	}

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a query that fails after its schema raises the server's error", "[twin][ops]") {
	const std::string big = "SELECT i, CASE WHEN i < 5000000 THEN i %s END AS x FROM range(10000000) t(i)";
	const auto query = [](const std::string &sql) {
		return "SELECT * FROM n6k_catalog_query('db', '" + StringUtil::Replace(sql, "'", "''") + "')";
	};
	const auto count = [](const std::string &sql) {
		return "SELECT count(*) FROM n6k_catalog_query('db', '" + StringUtil::Replace(sql, "'", "''") + "')";
	};

	SECTION("before any row") {
		N6kTwin twin;
		twin.Attach();
		twin.NearFails(query("SELECT error('boom') FROM range(1)"), ExceptionType::INVALID_INPUT, "n6k[db] QUERY:");
		twin.NearFails(query("SELECT error('boom') FROM range(1)"), ExceptionType::INVALID_INPUT, "boom");
		REQUIRE(twin.Rows(query("SELECT 1")) == RowList {"1"});
		REQUIRE(twin.ServeError().empty());
	}

	SECTION("after some rows") {
		N6kTwin twin;
		twin.Attach();
		twin.NearFails(count(StringUtil::Replace(big, "%s", "ELSE error('boom')")), ExceptionType::INVALID_INPUT,
		               "boom");
		REQUIRE(twin.Rows(query("SELECT 1")) == RowList {"1"});
		REQUIRE(twin.ServeError().empty());
	}

	SECTION("a local file system the server has disabled") {
		N6kTwin twin;
		twin.Far("SET extension_directory = '/nonexistent/n6k_test_extensions'");
		twin.Far("SET disabled_filesystems = 'LocalFileSystem'");
		twin.Attach();
		twin.NearFails(query("SELECT count(*) FROM duckdb_extensions()"), ExceptionType::PERMISSION, "LocalFileSystem");
		REQUIRE(twin.Rows(query("SELECT 1")) == RowList {"1"});
		for (const std::string sql : {"LOAD inet", "INSTALL inet"}) {
			twin.NearFails(query(sql), ExceptionType::PERMISSION, "n6k[db] QUERY:");
			REQUIRE(twin.Rows(query("SELECT 1")) == RowList {"1"});
		}
		REQUIRE(twin.ServeError().empty());
	}

	SECTION("a query that does not fail keeps every row") {
		N6kTwin twin;
		twin.Attach();
		REQUIRE(twin.Rows(count(StringUtil::Replace(big, "%s", "ELSE 0"))) == RowList {"10000000"});
		REQUIRE(twin.Rows(query("SET VARIABLE n6k_twin_ops = 1")).empty());
		REQUIRE(twin.ServeError().empty());
	}
}

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
		REQUIRE(twin.Rows("SELECT n FROM db.rpc('n6k_testing_stream_counter', 5) ORDER BY n") == counted);
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
	REQUIRE(twin.Rows("SELECT n FROM db.rpc('n6k_testing_stream_counter', 40000)").size() == 40000);
	REQUIRE(twin.SentCount(n6k::FrameType::CREDIT) >= 1);
	REQUIRE(twin.SentCount(n6k::FrameType::CANCEL) == 0);
	REQUIRE(twin.FarRows("SELECT credit_pauses >= 1, cancels FROM n6k_serve_stats()") == RowList {"true|0"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a stream released before its RESP_END arrives sends no cancel", "[twin][ops]") {
	N6kTwin twin;
	twin.Attach();

	twin.HoldRespEnd();
	REQUIRE(twin.Rows("SELECT n FROM db.rpc('n6k_testing_stream_counter', 40000)").size() == 40000);
	twin.ReleaseRespEnd();
	// A later round trip on the same socket: once it answers, every frame sent before it was recorded.
	REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_query('db', 'SELECT 1')") == RowList {"1"});
	REQUIRE(twin.SentCount(n6k::FrameType::CANCEL) == 0);

	REQUIRE(twin.FarRows("SELECT cancels FROM n6k_serve_stats()") == RowList {"0"});
	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("a stream released before its end-of-stream marker sends a cancel", "[twin][ops]") {
	N6kTwin twin;
	twin.Attach();

	REQUIRE(twin.Rows("SELECT n FROM db.rpc('n6k_testing_stream_counter', 1000000) LIMIT 1").size() == 1);
	REQUIRE(twin.Rows("SELECT * FROM n6k_catalog_query('db', 'SELECT 1')") == RowList {"1"});
	REQUIRE(twin.SentCount(n6k::FrameType::CANCEL) == 1);
	REQUIRE(twin.ServeError().empty());
}
