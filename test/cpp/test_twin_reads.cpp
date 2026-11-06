#include "framework/n6k_twin.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

using namespace duckdb;

namespace {

constexpr int64_t BIG_ROWS = 100003;

std::vector<std::string> Sequence(bool reversed = false) {
	std::vector<std::string> values;
	values.reserve(BIG_ROWS);
	for (int64_t i = 0; i < BIG_ROWS; i++) {
		values.push_back(std::to_string(reversed ? BIG_ROWS - 1 - i : i));
	}
	return values;
}

} // namespace

TEST_CASE("every column type and odd column name reads back identical", "[twin][reads]") {
	const std::vector<std::pair<std::string, std::string>> columns = {
	    {"BOOLEAN", "true"},
	    {"TINYINT", "-128"},
	    {"SMALLINT", "-32768"},
	    {"INTEGER", "-2147483648"},
	    {"BIGINT", "-9223372036854775808"},
	    {"HUGEINT", "-170141183460469231731687303715884105727"},
	    {"UTINYINT", "255"},
	    {"USMALLINT", "65535"},
	    {"UINTEGER", "4294967295"},
	    {"UBIGINT", "18446744073709551615"},
	    {"UHUGEINT", "340282366920938463463374607431768211455"},
	    {"UHUGEINT", "100000000000000000000000000000000000000"},
	    {"UHUGEINT", "5"},
	    {"FLOAT", "1.5"},
	    {"DOUBLE", "-2.25"},
	    {"DECIMAL(4,1)", "123.4"},
	    {"DECIMAL(10,2)", "12345678.90"},
	    {"DECIMAL(18,4)", "12345678901234.5678"},
	    {"DECIMAL(38,10)", "1234567890123456789012345678.0123456789"},
	    {"VARCHAR", "'héllo'"},
	    {"BLOB", "'\\xAA\\x00\\xBB'"},
	    {"DATE", "'2024-01-02'"},
	    {"TIME", "'03:04:05.678901'"},
	    {"TIMETZ", "'12:00:00+05:30'"},
	    {"TIMESTAMP", "'2024-01-02 03:04:05.678901'"},
	    {"TIMESTAMP_S", "'2024-01-02 03:04:05'"},
	    {"TIMESTAMP_MS", "'2024-01-02 03:04:05.678'"},
	    {"TIMESTAMP_NS", "'2024-01-02 03:04:05.678901234'"},
	    {"TIMESTAMPTZ", "'2024-01-02 03:04:05.678901+00'"},
	    {"INTERVAL", "INTERVAL '1 month 2 days 3 seconds'"},
	    {"UUID", "'12345678-1234-1234-1234-123456789abc'"},
	    {"BIT", "'10101'"},
	    {"INTEGER[]", "[1, NULL, 3]"},
	    {"INTEGER[3]", "[1, 2, 3]"},
	    {"VARCHAR[][]", "[['a'], [], ['b', NULL]]"},
	    {"UHUGEINT[]", "[340282366920938463463374607431768211455::UHUGEINT]"},
	    {"MAP(VARCHAR, INTEGER)", "MAP {'a': 1, 'b': 2}"},
	    {"STRUCT(a INTEGER, b VARCHAR)", "{'a': 1, 'b': 'x'}"},
	    {"STRUCT(t TIMETZ, b BIT)", "{'t': '12:00:00+05:30'::TIMETZ, 'b': '101'::BIT}"},
	    {"UNION(n INTEGER, s VARCHAR)", "union_value(s := 'x')"},
	};

	for (const auto &column : columns) {
		DYNAMIC_SECTION(column.first << " " << column.second) {
			N6kTwin twin;
			twin.Far("CREATE TABLE db.main.t (v " + column.first + ")");
			twin.Far("INSERT INTO db.main.t VALUES ((" + column.second + ")::" + column.first + "), (NULL)");
			twin.Attach();

			twin.Same("SELECT v FROM db.main.t ORDER BY v NULLS LAST");
			REQUIRE(twin.ServeError().empty());
		}
	}

	SECTION("an ENUM result is served as VARCHAR") {
		N6kTwin twin;
		twin.Far("CREATE TABLE db.main.users (id INTEGER, name VARCHAR)");
		twin.Far("INSERT INTO db.main.users VALUES (1, 'Alice'), (2, 'Bob')");
		twin.Attach();

		const std::string sql = "SELECT * FROM n6k_catalog_query('db', 'SELECT id, name::ENUM(''Alice'',''Bob'') AS "
		                        "who FROM db.main.users ORDER BY id')";
		auto result = twin.Near(sql);
		REQUIRE(result->GetTypes()[1] == LogicalType::VARCHAR);
		REQUIRE(twin.Rows(sql) == std::vector<std::string> {"1|Alice", "2|Bob"});
	}

	SECTION("a column name holding query syntax projects as itself") {
		N6kTwin twin;
		const std::vector<std::string> names = {"a,b", "a&b", "a=b", "a%41b"};
		for (size_t i = 0; i < names.size(); i++) {
			const auto table = "db.main.t" + std::to_string(i);
			twin.Far("CREATE TABLE " + table + " (\"" + names[i] + "\" INTEGER, plain INTEGER)");
			twin.Far("INSERT INTO " + table + " VALUES (11, 22)");
		}
		twin.Attach();

		for (size_t i = 0; i < names.size(); i++) {
			const auto table = "db.main.t" + std::to_string(i);
			const auto single = "SELECT \"" + names[i] + "\" FROM " + table;
			const auto both = "SELECT plain, \"" + names[i] + "\" FROM " + table;
			twin.Same(single);
			REQUIRE(twin.Rows(single) == std::vector<std::string> {"11"});
			twin.Same(both);
			REQUIRE(twin.Rows(both) == std::vector<std::string> {"22|11"});
		}
	}
}

TEST_CASE("a parallel scan keeps order and a mid-stream error raises", "[twin][reads]") {
	N6kTwin twin;
	twin.Far("CREATE TABLE db.main.big AS SELECT i AS seq, (i * 7919) % " + std::to_string(BIG_ROWS) +
	         " AS k FROM range(" + std::to_string(BIG_ROWS) + ") t(i)");
	twin.Attach();
	twin.Near("SET threads = 4");
	const auto ascending = Sequence();

	const std::string scan = "SELECT seq FROM db.main.big";
	twin.Crosses(scan);
	REQUIRE((twin.Rows(scan) == ascending));

	const std::string ordered = "SELECT k, seq FROM db.main.big ORDER BY k";
	twin.Crosses(ordered);
	const auto parallel = twin.Rows(ordered);
	twin.Near("SET threads = 1");
	REQUIRE((twin.Rows(ordered) == parallel));
	std::vector<std::string> keys;
	for (const auto &row : parallel) {
		keys.push_back(row.substr(0, row.find('|')));
	}
	REQUIRE((keys == ascending));
	twin.Near("SET threads = 4");

	twin.Near("CREATE TABLE near_copy AS SELECT seq FROM db.main.big");
	REQUIRE((twin.Rows("SELECT seq FROM near_copy") == ascending));

	const auto path = (std::filesystem::temp_directory_path() /
	                   ("n6k_twin_" + std::to_string(reinterpret_cast<uintptr_t>(&twin)) + ".csv"))
	                      .string();
	twin.Near("COPY (SELECT seq FROM db.main.big) TO '" + path + "' (HEADER false)");
	std::vector<std::string> copied;
	{
		std::ifstream file(path);
		for (std::string line; std::getline(file, line);) {
			copied.push_back(line);
		}
	}
	std::remove(path.c_str());
	REQUIRE((copied == ascending));

	const auto total = std::to_string(BIG_ROWS * (BIG_ROWS - 1) / 2);
	REQUIRE(twin.Rows("SELECT count(*), sum(seq), sum(k) FROM (SELECT seq, k FROM db.main.big WHERE seq >= 0)") ==
	        std::vector<std::string> {std::to_string(BIG_ROWS) + "|" + total + "|" + total});

	const auto limited = twin.Rows("SELECT b.seq FROM db.main.big b JOIN range(" + std::to_string(BIG_ROWS) +
	                               ") l(r) ON b.seq = l.r LIMIT 10");
	REQUIRE(limited.size() == 10);
	REQUIRE(twin.Rows("SELECT count(*) FROM db.main.big") == std::vector<std::string> {std::to_string(BIG_ROWS)});

	twin.NearFails("SELECT seq, CASE WHEN seq = " + std::to_string(BIG_ROWS - 5) +
	                   " THEN error('boom') ELSE seq END FROM db.main.big",
	               ExceptionType::INVALID_INPUT, "boom");

	twin.Near("SET preserve_insertion_order = false");
	const std::string descending = "SELECT k FROM db.main.big ORDER BY k DESC";
	twin.Crosses(descending);
	REQUIRE((twin.Rows(descending) == Sequence(true)));

	REQUIRE(twin.ServeError().empty());
}
