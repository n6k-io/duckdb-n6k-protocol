"""Crossing reads and writes over a served catalog whose storage is a sqlite file.

A sqlite scan cannot be serialized, so the server must bind the table where the plan
runs rather than carry a bound scan. The `sqlite_db` alias routes the test server's
session factory to such a catalog (see test_server/seeding.seed_sqlite).

Requires the test server running on :8099.
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
    try:
        c.execute("ATTACH 'n6k://localhost:8099' AS sqlite_db (TYPE n6k)")
    except duckdb.Error as error:
        if "sqlite" in str(error):
            pytest.skip(f"the server could not serve a sqlite catalog: {error}")
        raise
    return c


def _exec(con, sql: str):
    con.execute("SELECT * FROM n6k_catalog_exec('sqlite_db', ?)", [sql]).fetchall()


@pytest.fixture
def people(con):
    """The seeded table, reset after each test so the served file starts the same."""
    yield "people"
    try:
        _exec(con, "TRUNCATE TABLE sqlite_db.main.people")
        _exec(con, "INSERT INTO sqlite_db.main.people VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35)")
    except duckdb.Error as e:
        if "Connection has already been closed" in str(e):
            return
        raise


def _rows(con):
    return con.execute("SELECT id, name, age FROM sqlite_db.main.people ORDER BY id").fetchall()


def test_read(con, people):
    assert _rows(con) == [(1, "alice", 30), (2, "bob", 25), (3, "charlie", 35)]


def test_pushed_filter(con, people):
    rows = con.execute("SELECT name FROM sqlite_db.main.people WHERE age >= 30 ORDER BY id").fetchall()
    assert rows == [("alice",), ("charlie",)]


def test_insert_values(con, people):
    affected = con.execute("INSERT INTO sqlite_db.main.people VALUES (4, 'dave', 40)").fetchall()
    assert affected == [(1,)]
    assert _rows(con)[-1] == (4, "dave", 40)


def test_update_by_key(con, people):
    affected = con.execute("UPDATE sqlite_db.main.people SET name = 'updated' WHERE id = 2").fetchall()
    assert affected == [(1,)]
    assert _rows(con)[1] == (2, "updated", 25)


def test_delete_by_key(con, people):
    affected = con.execute("DELETE FROM sqlite_db.main.people WHERE id = 1").fetchall()
    assert affected == [(1,)]
    assert [r[0] for r in _rows(con)] == [2, 3]


def test_insert_select_from_served_table(con, people):
    affected = con.execute(
        "INSERT INTO sqlite_db.main.people SELECT id + 10, name, age FROM sqlite_db.main.people WHERE id < 3"
    ).fetchall()
    assert affected == [(2,)]
    assert [r[0] for r in _rows(con)] == [1, 2, 3, 11, 12]
