#include "framework/n6k_twin.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <algorithm>

using namespace duckdb;

namespace {

void SeedUsersAndProducts(N6kTwin &twin) {
	twin.Far("CREATE TABLE db.main.users (id INTEGER, name VARCHAR, age INTEGER)");
	twin.Far("INSERT INTO db.main.users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");
	twin.Far("CREATE SCHEMA db.test_schema");
	twin.Far("CREATE TABLE db.test_schema.products (id INTEGER, name VARCHAR, price DOUBLE)");
	twin.Far("INSERT INTO db.test_schema.products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)");
}

ScalarFunction IntUdf(const char *name, int32_t factor, int32_t offset) {
	return ScalarFunction(name, {LogicalType::INTEGER}, LogicalType::INTEGER,
	                      [factor, offset](DataChunk &args, ExpressionState &, Vector &result) {
		                      UnaryExecutor::Execute<int32_t, int32_t>(
		                          args.data[0], result, [&](int32_t value) { return value * factor + offset; });
	                      });
}

ScalarFunction VarcharUdf(const char *name, bool upper) {
	return ScalarFunction(name, {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      [upper](DataChunk &args, ExpressionState &, Vector &result) {
		                      UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, [&](string_t value) {
			                      const auto text = value.GetString();
			                      return StringVector::AddString(result, upper ? StringUtil::Upper(text)
			                                                                   : StringUtil::Lower(text));
		                      });
	                      });
}

} // namespace

TEST_CASE("relational ops cross and subqueries fold", "[twin][pushdown]") {
	N6kTwin twin;
	SeedUsersAndProducts(twin);
	twin.Far("CREATE TABLE db.main.scratch (id INTEGER PRIMARY KEY, name VARCHAR)");
	twin.Far("INSERT INTO db.main.scratch VALUES (1, 'a'), (2, 'b'), (3, 'c')");
	twin.Attach();

	for (const char *sql : {
	         "SELECT name FROM db.main.users WHERE age > 26 ORDER BY id LIMIT 1",
	         "SELECT upper(name), age * 2 FROM db.main.users ORDER BY id",
	         "SELECT age > 26, count(*) FROM db.main.users GROUP BY 1 ORDER BY 1",
	         "SELECT DISTINCT age > 26 FROM db.main.users ORDER BY 1",
	         "SELECT DISTINCT ON (age > 26) age > 26, name FROM db.main.users ORDER BY age > 26, id",
	         "SELECT id, row_number() OVER (ORDER BY age DESC) FROM db.main.users ORDER BY id",
	         "SELECT count(*) FROM (SELECT id FROM db.main.users USING SAMPLE 2 ROWS)",
	         "SELECT id, unnest([id, age]) AS v FROM db.main.users WHERE id = 1 ORDER BY v",
	     }) {
		twin.Crosses(sql);
		twin.Same(sql);
	}

	const std::string aggregate = "SELECT count(*), sum(age), min(name) FROM db.main.users";
	twin.Crosses(aggregate);
	twin.Same(aggregate);
	REQUIRE(twin.LastPlan().Has(AGGREGATE));

	const std::string join =
	    "SELECT u.name, p.name FROM db.main.users u JOIN db.test_schema.products p ON u.id = p.id ORDER BY u.id";
	twin.Crosses(join);
	twin.Same(join);
	auto tables = twin.LastPlan().Tables();
	std::sort(tables.begin(), tables.end());
	REQUIRE(tables == std::vector<std::string> {"products", "users"});

	const std::string product_ids = "(SELECT id FROM db.test_schema.products)";
	for (const std::string &sql : std::vector<std::string> {
	         "SELECT name FROM db.main.users WHERE id IN " + product_ids + " ORDER BY id",
	         "SELECT name FROM db.main.users WHERE id NOT IN " + product_ids + " ORDER BY id",
	         "SELECT id, id IN " + product_ids + " FROM db.main.users ORDER BY id",
	         "SELECT name FROM db.main.users u WHERE EXISTS "
	         "(SELECT 1 FROM db.test_schema.products p WHERE p.id = u.id) ORDER BY u.id",
	         "SELECT name FROM db.main.users u WHERE NOT EXISTS "
	         "(SELECT 1 FROM db.test_schema.products p WHERE p.id = u.id) ORDER BY u.id",
	         "SELECT name FROM db.main.users u WHERE EXISTS "
	         "(SELECT 1 FROM db.test_schema.products p WHERE p.id > u.id) ORDER BY u.id",
	         "SELECT name FROM db.main.users u WHERE id IN "
	         "(SELECT id FROM db.test_schema.products p WHERE p.price < u.age) ORDER BY u.id",
	         "SELECT name FROM db.main.users u WHERE id NOT IN "
	         "(SELECT id FROM db.test_schema.products p WHERE p.price < u.age) ORDER BY u.id",
	         "SELECT id, (SELECT name FROM db.test_schema.products p WHERE p.id = u.id) "
	         "FROM db.main.users u ORDER BY id",
	         "SELECT age + 1, age * 2, age / 2, age % 2, -age FROM db.main.users ORDER BY id",
	         "SELECT count(*), count(name), sum(age), avg(age), min(name), max(age) FROM db.main.users",
	         "SELECT string_agg(name, ',') FROM db.main.users",
	         "SELECT id FROM db.main.users WHERE name LIKE 'A%' ORDER BY id",
	         "SELECT id FROM db.main.users WHERE name ILIKE 'a%' ORDER BY id",
	         "SELECT id FROM db.main.users WHERE regexp_matches(name, '^A') ORDER BY id",
	         "SELECT round(price, 1), sqrt(price) FROM db.test_schema.products ORDER BY id",
	         "SELECT sum(price::DECIMAL(10, 2)) FROM db.test_schema.products",
	         "SELECT concat(name, '!', name), name || '?', length(name), substr(name, 1, 2) "
	         "FROM db.main.users ORDER BY id",
	         "SELECT list_contains([id, age], 30), [id, age][1], len([id, age]) FROM db.main.users ORDER BY id",
	         "SELECT id, row_number() OVER (ORDER BY age), sum(age) OVER () FROM db.main.users ORDER BY id",
	         "SELECT date_part('year', DATE '2024-01-02' + id), year(DATE '2024-01-02' + id) "
	         "FROM db.main.users ORDER BY id",
	         "SELECT abs(age::DECIMAL(10, 2)), sum(age::DECIMAL(10, 2)) OVER () FROM db.main.users ORDER BY id",
	         "SELECT {'a': id, 'b': name}.b FROM db.main.users ORDER BY id",
	         "SELECT date_trunc('day', TIMESTAMP '2024-01-02 03:04:05') + INTERVAL 1 DAY, id "
	         "FROM db.main.users ORDER BY id",
	     }) {
		twin.Folds(sql);
		twin.Same(sql);
	}

	twin.Near("CREATE TABLE local_ids(id INTEGER)");
	twin.Near("INSERT INTO local_ids VALUES (2), (3)");
	const std::string local_join = "SELECT u.name FROM db.main.users u JOIN local_ids l ON u.id = l.id ORDER BY u.id";
	twin.Crosses(local_join);
	REQUIRE(twin.Rows(local_join) == std::vector<std::string> {"Bob", "Charlie"});

	const std::string delete_in = "DELETE FROM db.main.scratch WHERE id IN " + product_ids;
	const auto delete_plan = twin.Explain(delete_in);
	INFO(delete_plan);
	REQUIRE(delete_plan.find("Hash Join") == std::string::npos);
	REQUIRE(twin.Rows(delete_in) == std::vector<std::string> {"2"});
	REQUIRE(twin.Rows("SELECT id FROM db.main.scratch") == std::vector<std::string> {"3"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("the per-attach operator list controls what crosses", "[twin][pushdown]") {
	const std::string query = "SELECT name FROM db.main.users WHERE age > 26 ORDER BY id LIMIT 1";
	const std::string get = "SELECT * FROM n6k_client_get('db', 'crossing')";
	const auto set = [](const std::string &action, const std::string &operators) {
		return "SELECT * FROM n6k_client_set('db', 'crossing', '" + action + "', " + operators + "::VARCHAR[])";
	};

	N6kTwin twin({"db", "db2"});
	SeedUsersAndProducts(twin);
	twin.Attach();

	const auto local = [&]() {
		const auto plan = twin.Explain(query);
		INFO(plan);
		REQUIRE(plan.find("Crossing Remote") != std::string::npos);
		return plan.substr(0, plan.find("Crossing Remote"));
	};
	const auto has = [](const std::vector<std::string> &values, const char *value) {
		return std::find(values.begin(), values.end(), value) != values.end();
	};
	const std::vector<std::string> nothing;

	SECTION("every operator crosses by default") {
		const auto operators = twin.Rows(get);
		for (const char *name : {"filter", "projection", "limit", "order_by", "top_n", "aggregate"}) {
			INFO(name);
			REQUIRE(has(operators, name));
		}
		REQUIRE(local().find("─ Filter ") == std::string::npos);
	}

	SECTION("an empty list is scan only") {
		REQUIRE(twin.Rows(set("set", "[]")) == nothing);
		REQUIRE(twin.Rows(get) == nothing);
		const auto plan = local();
		INFO(plan);
		REQUIRE(plan.find("─ Filter ") != std::string::npos);
		REQUIRE(plan.find("─ Top N ") != std::string::npos);
		REQUIRE(twin.Rows(query) == std::vector<std::string> {"Alice"});
	}

	SECTION("adding one operator crosses only that one") {
		twin.Rows(set("set", "[]"));
		REQUIRE(twin.Rows(set("add", "['filter']")) == std::vector<std::string> {"filter"});
		const auto plan = local();
		INFO(plan);
		REQUIRE(plan.find("─ Filter ") == std::string::npos);
		REQUIRE(plan.find("─ Top N ") != std::string::npos);
		REQUIRE(twin.Rows(query) == std::vector<std::string> {"Alice"});
	}

	SECTION("removing one operator keeps it local") {
		const auto after = twin.Rows(set("remove", "['filter']"));
		REQUIRE_FALSE(has(after, "filter"));
		REQUIRE(has(after, "limit"));
		REQUIRE(twin.Rows(get) == after);
		REQUIRE(local().find("─ Filter ") != std::string::npos);
	}

	SECTION("names and actions are case insensitive") {
		REQUIRE(twin.Rows(set("SET", "['FILTER']")) == std::vector<std::string> {"filter"});
	}

	SECTION("bad settings are refused") {
		twin.NearFails(set("add", "['bogus']"), ExceptionType::INVALID_INPUT, "unknown crossing operator");
		twin.NearFails(set("toggle", "['filter']"), ExceptionType::INVALID_INPUT, "unknown action");
		twin.NearFails("SELECT * FROM n6k_client_get('db', 'bogus')", ExceptionType::INVALID_INPUT, "unknown setting");
		twin.NearFails("SELECT * FROM n6k_client_get('memory', 'crossing')", ExceptionType::INVALID_INPUT,
		               "not an n6k database");
	}

	SECTION("the list belongs to one attach") {
		twin.Rows(set("set", "[]"));
		REQUIRE(twin.Rows(get) == nothing);
		REQUIRE(has(twin.Rows("SELECT * FROM n6k_client_get('db2', 'crossing')"), "filter"));
	}

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("client UDFs run on the client and overloads resolve", "[twin][pushdown]") {
	N6kTwin twin;
	SeedUsersAndProducts(twin);
	for (const char *table : {"updated", "deleted", "inserted"}) {
		twin.Far(std::string("CREATE TABLE db.main.") + table + " (id INTEGER PRIMARY KEY, name VARCHAR)");
		twin.Far(std::string("INSERT INTO db.main.") + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
	}
	twin.Attach();
	twin.NearFunction(IntUdf("myudf", 1, 1));
	twin.NearFunction(VarcharUdf("shout", true));

	const auto crossing_rows = [&](const std::string &sql, const std::vector<std::string> &expected) {
		twin.Crosses(sql);
		REQUIRE(twin.Rows(sql) == expected);
	};
	crossing_rows("SELECT myudf(id) FROM db.main.users ORDER BY id", {"2", "3", "4"});
	crossing_rows("SELECT id FROM db.main.users WHERE myudf(id) > 2 ORDER BY id", {"2", "3"});
	crossing_rows("SELECT id FROM db.main.users WHERE upper(name) <> 'BOB' AND myudf(id) > 2 ORDER BY id", {"3"});

	twin.Near("UPDATE db.main.updated SET name = shout(name) WHERE id = 2");
	REQUIRE(twin.Rows("SELECT id, name FROM db.main.updated ORDER BY id") ==
	        std::vector<std::string> {"1|a", "2|B", "3|c"});
	twin.Near("DELETE FROM db.main.deleted WHERE myudf(id) = 3");
	REQUIRE(twin.Rows("SELECT id FROM db.main.deleted ORDER BY id") == std::vector<std::string> {"1", "3"});
	twin.Near("INSERT INTO db.main.inserted SELECT myudf(id) + 10, name FROM db.main.inserted WHERE id = 1");
	REQUIRE(twin.Rows("SELECT id, name FROM db.main.inserted WHERE id > 3") == std::vector<std::string> {"12|a"});

	const std::string server_function = "SELECT upper(name) FROM db.main.users ORDER BY id";
	const std::vector<std::string> shouted = {"ALICE", "BOB", "CHARLIE"};
	twin.Folds(server_function);
	REQUIRE(twin.Rows(server_function) == shouted);

	twin.NearFunction(IntUdf("sqrt", 100, 0));
	REQUIRE(twin.Rows("SELECT sqrt(id) FROM db.main.users ORDER BY id") ==
	        std::vector<std::string> {"100", "200", "300"});

	twin.NearFunction(VarcharUdf("abs", false));
	REQUIRE(twin.Rows("SELECT abs(name) FROM db.main.users ORDER BY id") ==
	        std::vector<std::string> {"alice", "bob", "charlie"});

	twin.NearFunction(IntUdf("upper", 100, 0));
	twin.Folds(server_function);
	REQUIRE(twin.Rows(server_function) == shouted);

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("or, in and null filters match the server", "[twin][pushdown]") {
	N6kTwin twin;
	SeedUsersAndProducts(twin);
	twin.Attach();

	for (const char *sql : {
	         "SELECT id FROM db.main.users WHERE age > 30 OR age < 26 ORDER BY id",
	         "SELECT id FROM db.main.users WHERE (age > 26 AND age < 34) OR age = 35 ORDER BY id",
	         "SELECT id FROM db.main.users WHERE age >= 30 AND name != 'Alice' ORDER BY id",
	         "SELECT id FROM db.main.users WHERE name IN ('Alice', 'Charlie') ORDER BY id",
	         "SELECT id FROM db.main.users WHERE age NOT IN (25, 30) ORDER BY id",
	         "SELECT id FROM db.main.users WHERE name IS NOT NULL ORDER BY id",
	         "SELECT id FROM db.main.users WHERE name IS NULL",
	         "SELECT id, name, age FROM db.main.users WHERE age > 30 OR age < 26 ORDER BY id",
	     }) {
		twin.Same(sql);
	}
	REQUIRE(twin.Rows("SELECT id FROM db.main.users WHERE (age > 26 AND age < 34) OR age = 35 ORDER BY id") ==
	        std::vector<std::string> {"1", "3"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("aggregates match the server", "[twin][pushdown]") {
	N6kTwin twin;
	SeedUsersAndProducts(twin);
	twin.Attach();

	for (const char *sql : {
	         "SELECT age, count(*) AS c, sum(id) AS s FROM db.main.users GROUP BY age ORDER BY age",
	         "SELECT sum(id) AS s, typeof(sum(id)) AS t FROM db.main.users",
	         "SELECT count(*) AS c0, count(name) AS c1, sum(id) AS s, min(name) AS mn, max(age) AS mx, "
	         "avg(age) AS av FROM db.main.users",
	         "SELECT age, sum(id) AS s FROM db.main.users WHERE age > 26 GROUP BY age ORDER BY age",
	         "SELECT count(*) AS c, min(name) AS mn FROM db.main.users WHERE id < 0",
	         "SELECT age, count(*) AS c FROM db.main.users WHERE id < 0 GROUP BY age",
	         "SELECT lower(name) AS g, count(*) AS c FROM db.main.users GROUP BY lower(name) ORDER BY 1",
	         "SELECT count(DISTINCT age) AS c FROM db.main.users",
	         "SELECT median(age) AS m FROM db.main.users",
	         "SELECT string_agg(name, ',') AS s FROM (SELECT name FROM db.main.users ORDER BY name)",
	         "SELECT sum(id + age) AS s FROM db.main.users",
	         "SELECT count(*) FILTER (WHERE age > 26) AS c FROM db.main.users",
	         "SELECT count(*) AS c FROM db.main.users WHERE age > 30 OR age < 26",
	         "SELECT age, count(*) AS c FROM db.main.users GROUP BY age HAVING count(*) > 0 ORDER BY age",
	     }) {
		twin.Same(sql);
	}
	REQUIRE(twin.Rows("SELECT typeof(sum(id)) FROM db.main.users") == std::vector<std::string> {"HUGEINT"});
	REQUIRE(twin.Rows("SELECT count(*), min(name) FROM db.main.users WHERE id < 0") ==
	        std::vector<std::string> {"0|NULL"});

	REQUIRE(twin.ServeError().empty());
}

TEST_CASE("has_function answers are cached until invalidated", "[twin][pushdown]") {
	const auto plus = [](int64_t offset) {
		return ScalarFunction("shared_name", {LogicalType::BIGINT}, LogicalType::BIGINT,
		                      [offset](DataChunk &args, ExpressionState &, Vector &result) {
			                      UnaryExecutor::Execute<int64_t, int64_t>(
			                          args.data[0], result, [&](int64_t value) { return value + offset; });
		                      });
	};
	const std::string sql = "SELECT shared_name(i) FROM db.main.t";

	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.t AS SELECT 1::BIGINT AS i");
	twin.Attach();
	twin.NearFunction(plus(1));
	REQUIRE(twin.Rows(sql) == std::vector<std::string> {"2"});

	twin.FarFunction(plus(1000));
	REQUIRE(twin.Rows(sql) == std::vector<std::string> {"2"});

	twin.Near("CALL n6k_invalidate_cache('db', 'main')");
	REQUIRE(twin.Rows(sql) == std::vector<std::string> {"1001"});

	REQUIRE(twin.ServeError().empty());
}
