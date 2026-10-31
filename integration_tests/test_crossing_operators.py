"""Per-attach crossing operator list: n6k_client_set / n6k_client_get.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_crossing_operators.py -v
"""

import os
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_URL = "http://localhost:8099"
QUERY = "SELECT name FROM db.main.users WHERE age > 26 ORDER BY id LIMIT 1"


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


def _local(con, sql: str) -> str:
    plan = _explain(con, sql)
    assert "CROSSING_READ" in plan, plan
    return plan.split("CROSSING_READ")[0]


def _get(con, name: str = "db") -> list[str]:
    return [r[0] for r in con.execute(f"SELECT * FROM n6k_client_get('{name}', 'crossing')").fetchall()]


def _set(con, action: str, operators: list[str], name: str = "db") -> list[str]:
    rows = con.execute(
        "SELECT * FROM n6k_client_set($1, 'crossing', $2, $3::VARCHAR[])",
        [name, action, operators],
    ).fetchall()
    return [r[0] for r in rows]


def test_default_allows_every_operator(con):
    operators = _get(con)
    for name in ("filter", "projection", "limit", "order_by", "top_n", "aggregate"):
        assert name in operators
    assert "FILTER" not in _local(con, QUERY)


def test_empty_list_is_scan_only(con):
    assert _set(con, "set", []) == []
    assert _get(con) == []
    local = _local(con, QUERY)
    assert "FILTER" in local, local
    assert "TOP_N" in local, local
    assert con.execute(QUERY).fetchall() == [("Alice",)]


def test_add_filter_only(con):
    _set(con, "set", [])
    assert _set(con, "add", ["filter"]) == ["filter"]
    local = _local(con, QUERY)
    assert "FILTER" not in local, local
    assert "TOP_N" in local, local
    assert con.execute(QUERY).fetchall() == [("Alice",)]


def test_remove(con):
    after = _set(con, "remove", ["filter"])
    assert "filter" not in after
    assert "limit" in after
    assert _get(con) == after
    assert "FILTER" in _local(con, QUERY)


def test_names_are_case_insensitive(con):
    assert _set(con, "SET", ["FILTER"]) == ["filter"]


def test_unknown_operator_raises(con):
    with pytest.raises(duckdb.InvalidInputException, match="unknown crossing operator"):
        _set(con, "add", ["bogus"])


def test_unknown_action_raises(con):
    with pytest.raises(duckdb.InvalidInputException, match="unknown action"):
        _set(con, "toggle", ["filter"])


def test_unknown_setting_raises(con):
    with pytest.raises(duckdb.InvalidInputException, match="unknown setting"):
        con.execute("SELECT * FROM n6k_client_get('db', 'bogus')").fetchall()


def test_non_n6k_database_raises(con):
    with pytest.raises(duckdb.InvalidInputException, match="not an n6k database"):
        _get(con, "memory")


def test_per_attach(con):
    con.execute("ATTACH 'n6k://localhost:8099' AS db2 (TYPE n6k)")
    _set(con, "set", [])
    assert _get(con) == []
    assert "filter" in _get(con, "db2")
