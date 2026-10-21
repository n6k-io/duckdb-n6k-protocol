"""A served provider table with a dictionary-encoded (pandas-Categorical-shaped)
column must attach and scan over the websocket instead of tearing down the
serving connection (close 1011) during the schema exchange.

The `slowdb` catalog's `cats` schema routes to the test server's
CategoricalProvider, whose `cat` column is dictionary<values=string, indices=int8>.
Requires the test server on :8099.
"""

import os
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_URL = "http://localhost:8099"
DSN = "n6k://localhost:8099"


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


def attach():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.execute(f"LOAD '{EXT_PATH}'")
    c.execute(f"ATTACH '{DSN}' AS slowdb (TYPE n6k)")
    return c


def test_a_dictionary_encoded_provider_column_scans_as_varchar():
    con = attach()
    try:
        rows = con.execute("SELECT id, cat FROM slowdb.cats.cats ORDER BY id").fetchall()
        assert rows == [(1, "x"), (2, "y"), (3, "x")]
    finally:
        con.close()


def test_the_catalog_stays_usable_after_touching_the_dictionary_table():
    con = attach()
    try:
        con.execute("SELECT count(*) FROM slowdb.cats.cats").fetchone()
        described = con.execute("DESCRIBE slowdb.cats.cats").fetchall()
        assert [(row[0], row[1]) for row in described] == [("id", "BIGINT"), ("cat", "VARCHAR")]
    finally:
        con.close()


def test_an_enum_column_is_served_as_varchar():
    """The other direction: DuckDB's Arrow export renders ENUM as a dictionary field,
    which nanoarrow's IPC writer cannot emit, so the server casts ENUM to VARCHAR
    before encoding. The cast here runs per row server-side, so the result column
    really is an ENUM until it hits the encoder."""
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("LOAD httpfs")
    con.execute(f"LOAD '{EXT_PATH}'")
    con.execute(f"ATTACH '{DSN}' AS db (TYPE n6k)")
    try:
        rows = con.execute(
            "SELECT * FROM n6k_catalog_query('db', "
            "\"SELECT id, name::ENUM('Alice','Bob','Charlie') AS who FROM db.main.users ORDER BY id\")"
        ).fetchall()
        assert rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    finally:
        con.close()
