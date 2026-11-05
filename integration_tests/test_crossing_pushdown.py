"""Crossing pushdown: the fragment DuckDB ships to the server, and its answers.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_crossing_pushdown.py -v
"""

import os
import time
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_URL = "http://localhost:8099"


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip(f"test server not running on {SERVER_URL}")


@pytest.fixture
def con():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.execute(f"LOAD '{EXT_PATH}'")
    c.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    return c


def _explain(con, sql: str) -> str:
    rows = con.execute(f"EXPLAIN {sql}").fetchall()
    return "\n".join(r[1] for r in rows)


def _crossed(con, sql: str) -> str:
    plan = _explain(con, sql)
    assert "Crossing Read" in plan, plan
    return plan


def test_filter_and_limit_cross(con):
    _crossed(con, "SELECT name FROM db.main.users WHERE age > 26 ORDER BY id LIMIT 1")
    assert con.execute("SELECT name FROM db.main.users WHERE age > 26 ORDER BY id LIMIT 1").fetchall() == [("Alice",)]


def test_projection_of_expression_crosses(con):
    _crossed(con, "SELECT upper(name), age * 2 FROM db.main.users ORDER BY id")
    assert con.execute("SELECT upper(name), age * 2 FROM db.main.users ORDER BY id").fetchall() == [
        ("ALICE", 60),
        ("BOB", 50),
        ("CHARLIE", 70),
    ]


def test_aggregate_crosses(con):
    _crossed(con, "SELECT count(*), sum(age), min(name) FROM db.main.users")
    assert con.execute("SELECT count(*), sum(age), min(name) FROM db.main.users").fetchall() == [(3, 90, "Alice")]


def test_group_by_crosses(con):
    _crossed(con, "SELECT age > 26, count(*) FROM db.main.users GROUP BY 1 ORDER BY 1")
    assert con.execute("SELECT age > 26, count(*) FROM db.main.users GROUP BY 1 ORDER BY 1").fetchall() == [
        (False, 1),
        (True, 2),
    ]


def test_join_of_two_served_tables_crosses(con):
    sql = "SELECT u.name, p.name FROM db.main.users u JOIN db.test_schema.products p ON u.id = p.id ORDER BY u.id"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [("Alice", "Widget"), ("Bob", "Gadget")]


def test_local_join_stays_local(con):
    con.execute("CREATE TABLE local_ids(id INTEGER)")
    con.execute("INSERT INTO local_ids VALUES (2), (3)")
    sql = "SELECT u.name FROM db.main.users u JOIN local_ids l ON u.id = l.id ORDER BY u.id"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [("Bob",), ("Charlie",)]


def test_unshippable_function_stays_local(con):
    sql = "SELECT id FROM db.main.users WHERE regexp_matches(name, '^A') ORDER BY id"
    assert con.execute(sql).fetchall() == [(1,)]


PRODUCT_IDS = "(SELECT id FROM db.test_schema.products)"


def _folded(con, sql: str) -> None:
    plan = _explain(con, sql)
    assert "Crossing Read" in plan.splitlines()[0], plan


def test_in_subquery_folds(con):
    sql = f"SELECT name FROM db.main.users WHERE id IN {PRODUCT_IDS} ORDER BY id"
    _folded(con, sql)
    assert con.execute(sql).fetchall() == [("Alice",), ("Bob",)]


def test_not_in_subquery_folds(con):
    sql = f"SELECT name FROM db.main.users WHERE id NOT IN {PRODUCT_IDS} ORDER BY id"
    _folded(con, sql)
    assert con.execute(sql).fetchall() == [("Charlie",)]


def test_in_subquery_as_value_folds(con):
    sql = f"SELECT id, id IN {PRODUCT_IDS} FROM db.main.users ORDER BY id"
    _folded(con, sql)
    assert con.execute(sql).fetchall() == [(1, True), (2, True), (3, False)]


@pytest.fixture
def scratch(con):
    table = f"pushdown_{int(time.time() * 1000)}"
    create = f'CREATE TABLE db.main."{table}"(id INTEGER PRIMARY KEY, name VARCHAR)'
    con.execute("SELECT * FROM n6k_catalog_exec('db', ?)", [create]).fetchall()
    insert = f"INSERT INTO db.main.\"{table}\" VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    con.execute("SELECT * FROM n6k_catalog_exec('db', ?)", [insert]).fetchall()
    con.execute("SELECT * FROM n6k_invalidate_cache('db', 'main')").fetchall()
    yield f'db.main."{table}"'
    drop = f'DROP TABLE IF EXISTS db.main."{table}"'
    con.execute("SELECT * FROM n6k_catalog_exec('db', ?)", [drop]).fetchall()


def test_delete_with_in_subquery_joins_on_server(con, scratch):
    sql = f"DELETE FROM {scratch} WHERE id IN {PRODUCT_IDS}"
    plan = _explain(con, sql)
    assert "Hash Join" not in plan, plan
    assert con.execute(sql).fetchall() == [(2,)]
    assert con.execute(f"SELECT id FROM {scratch}").fetchall() == [(3,)]


@pytest.mark.parametrize(
    "sql, expected",
    [
        (
            "SELECT name FROM db.main.users u WHERE EXISTS "
            "(SELECT 1 FROM db.test_schema.products p WHERE p.id = u.id) ORDER BY u.id",
            [("Alice",), ("Bob",)],
        ),
        (
            "SELECT name FROM db.main.users u WHERE NOT EXISTS "
            "(SELECT 1 FROM db.test_schema.products p WHERE p.id = u.id) ORDER BY u.id",
            [("Charlie",)],
        ),
        (
            "SELECT name FROM db.main.users u WHERE EXISTS "
            "(SELECT 1 FROM db.test_schema.products p WHERE p.id > u.id) ORDER BY u.id",
            [("Alice",)],
        ),
        (
            "SELECT name FROM db.main.users u WHERE id IN "
            "(SELECT id FROM db.test_schema.products p WHERE p.price < u.age) ORDER BY u.id",
            [("Alice",), ("Bob",)],
        ),
        (
            "SELECT name FROM db.main.users u WHERE id NOT IN "
            "(SELECT id FROM db.test_schema.products p WHERE p.price < u.age) ORDER BY u.id",
            [("Charlie",)],
        ),
        (
            "SELECT id, (SELECT name FROM db.test_schema.products p WHERE p.id = u.id) "
            "FROM db.main.users u ORDER BY id",
            [(1, "Widget"), (2, "Gadget"), (3, None)],
        ),
    ],
    ids=["exists", "not_exists", "exists_non_equality", "in", "not_in", "scalar"],
)
def test_correlated_subquery_folds(con, sql, expected):
    _folded(con, sql)
    assert con.execute(sql).fetchall() == expected


def _add_one_udf(con):
    con.create_function("myudf", lambda x: x + 1, ["INTEGER"], "INTEGER")


def test_client_udf_in_projection_runs_on_client(con):
    _add_one_udf(con)
    sql = "SELECT myudf(id) FROM db.main.users ORDER BY id"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(2,), (3,), (4,)]


def test_client_udf_in_filter_runs_on_client(con):
    _add_one_udf(con)
    sql = "SELECT id FROM db.main.users WHERE myudf(id) > 2 ORDER BY id"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(2,), (3,)]


def test_client_udf_beside_server_condition(con):
    _add_one_udf(con)
    sql = "SELECT id FROM db.main.users WHERE upper(name) <> 'BOB' AND myudf(id) > 2 ORDER BY id"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(3,)]


def test_client_udf_in_update(con, scratch):
    con.create_function("shout", lambda x: x.upper(), ["VARCHAR"], "VARCHAR")
    con.execute(f"UPDATE {scratch} SET name = shout(name) WHERE id = 2")
    assert con.execute(f"SELECT id, name FROM {scratch} ORDER BY id").fetchall() == [(1, "a"), (2, "B"), (3, "c")]


def test_client_udf_in_delete(con, scratch):
    _add_one_udf(con)
    con.execute(f"DELETE FROM {scratch} WHERE myudf(id) = 3")
    assert con.execute(f"SELECT id FROM {scratch} ORDER BY id").fetchall() == [(1,), (3,)]


def test_client_udf_in_insert(con, scratch):
    _add_one_udf(con)
    con.execute(f"INSERT INTO {scratch} SELECT myudf(id) + 10, name FROM {scratch} WHERE id = 1")
    assert con.execute(f"SELECT id, name FROM {scratch} WHERE id > 3").fetchall() == [(12, "a")]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT age + 1, age * 2, age / 2, age % 2, -age FROM db.main.users ORDER BY id",
        "SELECT count(*), count(name), sum(age), avg(age), min(name), max(age) FROM db.main.users",
        "SELECT string_agg(name, ',') FROM db.main.users",
        "SELECT id FROM db.main.users WHERE name LIKE 'A%' ORDER BY id",
        "SELECT id FROM db.main.users WHERE name ILIKE 'a%' ORDER BY id",
        "SELECT id FROM db.main.users WHERE regexp_matches(name, '^A') ORDER BY id",
        "SELECT abs(age::DECIMAL(10, 2)), sum(age::DECIMAL(10, 2)) OVER () FROM db.main.users ORDER BY id",
        "SELECT round(price, 1), sqrt(price) FROM db.test_schema.products ORDER BY id",
        "SELECT sum(price::DECIMAL(10, 2)) FROM db.test_schema.products",
        "SELECT concat(name, '!', name), name || '?', length(name), substr(name, 1, 2) FROM db.main.users ORDER BY id",
        "SELECT list_contains([id, age], 30), [id, age][1], len([id, age]) FROM db.main.users ORDER BY id",
        "SELECT {'a': id, 'b': name}.b FROM db.main.users ORDER BY id",
        "SELECT date_trunc('day', TIMESTAMP '2024-01-02 03:04:05') + INTERVAL 1 DAY, id FROM db.main.users ORDER BY id",
        "SELECT id, row_number() OVER (ORDER BY age), sum(age) OVER () FROM db.main.users ORDER BY id",
        "SELECT date_part('year', DATE '2024-01-02' + id), year(DATE '2024-01-02' + id) FROM db.main.users ORDER BY id",
    ],
)
def test_builtin_functions_still_fold(con, sql):
    _folded(con, sql)
    assert con.execute(sql).fetchall()


def test_server_function_still_folds_with_client_udf_registered(con):
    _add_one_udf(con)
    sql = "SELECT upper(name) FROM db.main.users ORDER BY id"
    assert "Crossing Read" in _explain(con, sql).splitlines()[0]
    assert con.execute(sql).fetchall() == [("ALICE",), ("BOB",), ("CHARLIE",)]


def test_client_overload_the_server_can_bind_by_cast_runs_on_client(con):
    con.create_function("sqrt", lambda x: x * 100, ["INTEGER"], "INTEGER")
    sql = "SELECT sqrt(id) FROM db.main.users ORDER BY id"
    assert con.execute(sql).fetchall() == [(100,), (200,), (300,)]


def test_client_overload_the_server_cannot_bind_runs_on_client(con):
    con.create_function("abs", lambda x: x.lower(), ["VARCHAR"], "VARCHAR")
    sql = "SELECT abs(name) FROM db.main.users ORDER BY id"
    assert con.execute(sql).fetchall() == [("alice",), ("bob",), ("charlie",)]


def test_server_overload_of_client_overloaded_name_still_folds(con):
    con.create_function("upper", lambda x: x * 100, ["INTEGER"], "INTEGER")
    sql = "SELECT upper(name) FROM db.main.users ORDER BY id"
    assert "Crossing Read" in _explain(con, sql).splitlines()[0]
    assert con.execute(sql).fetchall() == [("ALICE",), ("BOB",), ("CHARLIE",)]


def test_distinct_and_union_cross(con):
    sql = "SELECT DISTINCT age > 26 FROM db.main.users ORDER BY 1"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(False,), (True,)]


def test_invalidation_reloads_tables(con):
    assert con.execute("SELECT count(*) FROM db.main.users").fetchall() == [(3,)]
    con.execute("CALL n6k_invalidate_cache('db', 'main')")
    assert con.execute("SELECT count(*) FROM db.main.users").fetchall() == [(3,)]
    assert "users" in [
        r[0] for r in con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'db'").fetchall()
    ]


def test_window_crosses(con):
    sql = "SELECT id, row_number() OVER (ORDER BY age DESC) FROM db.main.users ORDER BY id"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(1, 2), (2, 3), (3, 1)]


def test_distinct_on_crosses(con):
    sql = "SELECT DISTINCT ON (age > 26) age > 26, name FROM db.main.users ORDER BY age > 26, id"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(False, "Bob"), (True, "Alice")]


def test_sample_crosses(con):
    sql = "SELECT count(*) FROM (SELECT id FROM db.main.users USING SAMPLE 2 ROWS)"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(2,)]


def test_unnest_crosses(con):
    sql = "SELECT id, unnest([id, age]) AS v FROM db.main.users WHERE id = 1 ORDER BY v"
    _crossed(con, sql)
    assert con.execute(sql).fetchall() == [(1, 1), (1, 30)]
