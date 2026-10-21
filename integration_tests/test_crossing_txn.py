"""Server-side transactions pinned to a DuckDB transaction on the client.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_crossing_txn.py -v
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


def _connect():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.execute(f"LOAD '{EXT_PATH}'")
    c.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    return c


@pytest.fixture
def con():
    return _connect()


def _exec(con, sql: str):
    con.execute("SELECT * FROM n6k_catalog_exec('db', ?)", [sql]).fetchall()


@pytest.fixture
def keyed(con):
    name = f"ct_{int(time.time() * 1000)}"
    _exec(con, f'CREATE TABLE db.main."{name}"(id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)')
    _exec(con, f"INSERT INTO db.main.\"{name}\" VALUES (1, 'alice', 30), (2, 'bob', 25)")
    con.execute("SELECT * FROM n6k_invalidate_cache('db', 'main')").fetchall()
    yield name
    _exec(con, f'DROP TABLE IF EXISTS db.main."{name}"')


def _ages(con, name):
    return con.execute(f'SELECT id, age FROM db.main."{name}" ORDER BY id').fetchall()


def test_rollback_discards_the_write(con, keyed):
    con.execute("BEGIN")
    con.execute(f'UPDATE db.main."{keyed}" SET age = 99 WHERE id = 1')
    assert _ages(con, keyed) == [(1, 99), (2, 25)]
    con.execute("ROLLBACK")
    assert _ages(con, keyed) == [(1, 30), (2, 25)]


def test_commit_keeps_the_write(con, keyed):
    con.execute("BEGIN")
    con.execute(f'UPDATE db.main."{keyed}" SET age = 99 WHERE id = 1')
    con.execute(f"INSERT INTO db.main.\"{keyed}\" VALUES (3, 'carol', 40)")
    con.execute("COMMIT")
    assert _ages(con, keyed) == [(1, 99), (2, 25), (3, 40)]
    assert con.execute(
        "SELECT * FROM n6k_catalog_query('db', 'SELECT count(*) FROM db.main.\"" + keyed + "\"')"
    ).fetchall() == [(3,)]


def test_uncommitted_write_is_invisible_outside_the_transaction(con, keyed):
    con.execute("BEGIN")
    con.execute(f'DELETE FROM db.main."{keyed}" WHERE id = 2')
    assert _ages(con, keyed) == [(1, 30)]
    assert con.execute(
        "SELECT * FROM n6k_catalog_query('db', 'SELECT count(*) FROM db.main.\"" + keyed + "\"')"
    ).fetchall() == [(2,)]
    con.execute("ROLLBACK")
    assert _ages(con, keyed) == [(1, 30), (2, 25)]


def test_autocommit_statements_do_not_pin(con, keyed):
    con.execute(f'UPDATE db.main."{keyed}" SET age = 1 WHERE id = 2')
    assert con.execute(
        "SELECT * FROM n6k_catalog_query('db', 'SELECT age FROM db.main.\"" + keyed + "\" WHERE id = 2')"
    ).fetchall() == [(1,)]
