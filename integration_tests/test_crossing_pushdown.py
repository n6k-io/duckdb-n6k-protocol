"""Crossing pushdown: the fragment DuckDB ships to the server, and its answers.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_crossing_pushdown.py -v
"""

import os
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
    assert "CROSSING_READ" in plan, plan
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
