"""Test n6k_catalog_exec / n6k_catalog_query table functions and catalog-scoped variants.

Run:
    uv run pytest integration_tests/test_exec_query.py -v
"""

import os
import urllib.request
import urllib.error

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
        pytest.skip("Test server not running on port 8099")


@pytest.fixture()
def conn():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db (TYPE n6k)")
    return c


# ── n6k_catalog_query (global) ──────────────────────────────────────────────────────


def test_n6k_catalog_query_select(conn):
    result = conn.execute("SELECT * FROM n6k_catalog_query('db', 'SELECT * FROM db.main.users ORDER BY id')").fetchall()
    assert len(result) == 3
    assert result[0][1] == "Alice"


def test_n6k_catalog_query_with_where(conn):
    result = conn.execute(
        "SELECT * FROM n6k_catalog_query('db', 'SELECT name FROM db.main.users WHERE id = 2')"
    ).fetchall()
    assert len(result) == 1
    assert result[0][0] == "Bob"


# ── catalog.query ────────────────────────────────────────────────────────────


def test_catalog_query(conn):
    result = conn.execute("SELECT * FROM db.query('SELECT * FROM db.main.users ORDER BY id')").fetchall()
    assert len(result) == 3
    assert result[2][1] == "Charlie"


# ── n6k_catalog_exec (global) ───────────────────────────────────────────────────────


def test_n6k_catalog_exec_update(conn):
    result = conn.execute(
        "SELECT * FROM n6k_catalog_exec('db', 'UPDATE db.main.users SET name = ''Updated'' WHERE id = 2')"
    ).fetchall()
    assert len(result) == 1
    assert result[0][0] == 1

    check = conn.execute(
        "SELECT * FROM n6k_catalog_query('db', 'SELECT name FROM db.main.users WHERE id = 2')"
    ).fetchall()
    assert check[0][0] == "Updated"

    conn.execute("SELECT * FROM n6k_catalog_exec('db', 'UPDATE db.main.users SET name = ''Bob'' WHERE id = 2')")


# ── catalog.exec ─────────────────────────────────────────────────────────────


def test_catalog_exec_update(conn):
    result = conn.execute(
        "SELECT * FROM db.exec('UPDATE db.main.users SET name = ''Changed'' WHERE id = 3')"
    ).fetchall()
    assert len(result) == 1
    assert result[0][0] == 1

    check = conn.execute("SELECT * FROM db.query('SELECT name FROM db.main.users WHERE id = 3')").fetchall()
    assert check[0][0] == "Changed"

    conn.execute("SELECT * FROM db.exec('UPDATE db.main.users SET name = ''Charlie'' WHERE id = 3')")
