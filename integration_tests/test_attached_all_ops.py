"""Every non-streaming attached-catalog op routes over the WS.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_attached_all_ops.py -v
"""

import json
import os
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"


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


def _reset_counts():
    urllib.request.urlopen(urllib.request.Request(f"{SERVER_URL}/debug/counts/reset", method="POST"), timeout=2).read()


def _counts() -> dict:
    with urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2) as r:
        data = json.loads(r.read())
    # The asserts here read the http counts; a response without an "http" key is already flat.
    if "http" in data:
        return data["http"]
    return data


# ─────────────────────────────────────────────────────────────────────────────


def test_tables_list(con):
    _reset_counts()
    rows = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog = 'db' AND table_schema NOT IN ('information_schema','pg_catalog') "
        "ORDER BY table_schema, table_name"
    ).fetchall()
    names = {r[1] for r in rows}
    assert {"users", "products"}.issubset(names)
    counts = _counts()
    assert counts.get("GET /tables", 0) == 0, counts


def test_table_schema_probe(con):
    _reset_counts()
    rows = con.execute("DESCRIBE db.main.users").fetchall()
    col_names = {r[0] for r in rows}
    assert {"id", "name", "age"}.issubset(col_names)
    counts = _counts()
    assert counts.get("GET /main/users/schema", 0) == 0, counts


def test_exec_via_ws(con):
    _reset_counts()
    row = con.execute(
        "SELECT * FROM n6k_catalog_exec('db', 'UPDATE db.main.users SET age = age WHERE id = 1')"
    ).fetchone()
    assert row[0] is not None
    counts = _counts()
    assert counts.get("POST /exec", 0) == 0, counts


def test_insert_via_ws(con):
    _reset_counts()
    con.execute("INSERT INTO db.main.users VALUES (777, 'Ned', 77)")
    counts = _counts()
    assert counts.get("POST /main/users/rows", 0) == 0, counts
    n = con.execute("SELECT count(*) FROM db.main.users WHERE id = 777").fetchone()[0]
    assert n == 1
    con.execute("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id = 777')").fetchone()


def test_insert_partial_column_list(con):
    con.execute("INSERT INTO db.main.users (id, name) VALUES (778, 'Partial')")
    row = con.execute("SELECT id, name, age FROM db.main.users WHERE id = 778").fetchone()
    assert row == (778, "Partial", None)
    con.execute("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id = 778')").fetchone()


def test_insert_reordered_column_list(con):
    con.execute("INSERT INTO db.main.users (age, id, name) VALUES (42, 779, 'Reord')")
    row = con.execute("SELECT id, name, age FROM db.main.users WHERE id = 779").fetchone()
    assert row == (779, "Reord", 42)
    con.execute("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id = 779')").fetchone()


def test_insert_on_conflict_rejected(con):
    with pytest.raises(Exception, match="ON CONFLICT"):
        con.execute("INSERT INTO db.main.users VALUES (780, 'X', 1) ON CONFLICT DO NOTHING")


def test_rpc_scalar_via_ws(con):
    _reset_counts()
    row = con.execute("SELECT * FROM n6k_catalog_rpc('db', 'echo', 'hello')").fetchone()
    assert row is not None
    counts = _counts()
    assert counts.get("POST /rpc/echo", 0) == 0, counts


def test_rpc_table_via_ws(con):
    _reset_counts()
    # sum_table takes an Arrow table arg.
    rows = con.execute(
        "SELECT * FROM n6k_catalog_rpc_table('db', 'sum_table', (SELECT id FROM db.main.users LIMIT 3))"
    ).fetchall()
    assert len(rows) == 1
    counts = _counts()
    # RPC POST shouldn't hit HTTP in strict mode.
    assert counts.get("POST /rpc/sum_table", 0) == 0, counts


def test_scan_via_ws(con):
    """SCAN streams over the WS, not POST /{schema}/{table}/scan."""
    _reset_counts()
    rows = con.execute("SELECT id FROM db.main.users ORDER BY id LIMIT 3").fetchall()
    assert len(rows) == 3
    counts = _counts()
    scan_hits = sum(v for k, v in counts.items() if k.startswith("POST /main/users/scan"))
    assert scan_hits == 0, counts


def test_query_via_ws(con):
    """QUERY streams over the WS, not POST /query."""
    _reset_counts()
    rows = con.execute(
        "SELECT * FROM n6k_catalog_query('db', 'SELECT id FROM db.main.users ORDER BY id LIMIT 3')"
    ).fetchall()
    assert len(rows) == 3
    counts = _counts()
    assert counts.get("POST /query", 0) == 0, counts
