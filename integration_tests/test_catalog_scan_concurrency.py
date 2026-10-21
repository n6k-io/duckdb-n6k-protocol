"""Regression: concurrent catalog scans of one n6k schema must not free live entries.

`SHOW ALL TABLES` expands to `duckdb_tables JOIN duckdb_columns` UNION ALL
`duckdb_views JOIN duckdb_columns` (duckdb's PragmaShowTablesExpanded). Both sides bind a
`duckdb_columns` scan, and duckdb runs them on separate threads, so `N6kSchemaEntry::Scan`
gets called twice concurrently against the same schema.

Two things used to break there. `EnsureFresh` read `fetched_tables` with no lock, so both
scans decided the cache was cold and both ran `LoadMissingTableEntries`; the second one's
`tables[name] = std::move(entry)` destroyed the entry the first had already handed out. And
`duckdb_columns` keeps the `CatalogEntry &` from Scan() in its global state for the whole
query, so that destruction left it reading freed memory -- which surfaced as
`Not implemented Error: Unsupported catalog type for duckdb_columns` with catalog_type
INVALID, INVALID being no real CatalogType at all.

The failure needed >1 thread; `SET threads=1` always passed. Requires the test server on :8099.

Run:
    uv run pytest integration_tests/test_catalog_scan_concurrency.py -v
"""

import os
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_URL = "http://localhost:8099"
DSN = "n6k://localhost:8099"

# One connection reproduced it every time, but the bug is a data race; a few rounds keeps
# the test meaningful if scheduling ever shifts.
ROUNDS = 5

SHOW_TABLES_EXPANSION = """
SELECT t.table_oid FROM duckdb_tables() t JOIN duckdb_columns() c USING (table_oid)
UNION ALL
SELECT v.view_oid FROM duckdb_views() v JOIN duckdb_columns() c ON (v.view_oid = c.table_oid)
"""


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
    c.execute(f"ATTACH '{DSN}' AS db (TYPE n6k)")
    return c


def test_show_all_tables_on_a_cold_catalog():
    for _ in range(ROUNDS):
        con = attach()
        rows = con.execute("SHOW ALL TABLES").fetchall()
        assert any(r[0] == "db" for r in rows), rows
        con.close()


def test_show_all_tables_after_a_query_has_warmed_the_catalog():
    for _ in range(ROUNDS):
        con = attach()
        con.execute("SELECT count(*) FROM db.main.users").fetchone()
        rows = con.execute("SHOW ALL TABLES").fetchall()
        assert any(r[0] == "db" for r in rows), rows
        con.close()


def test_two_joined_column_scans_in_one_query():
    for _ in range(ROUNDS):
        con = attach()
        con.execute(f"SELECT count(*) FROM ({SHOW_TABLES_EXPANSION})").fetchone()
        con.close()


def test_each_half_of_the_expansion_alone_still_works():
    con = attach()
    assert (
        con.execute("SELECT count(*) FROM duckdb_tables() t " "JOIN duckdb_columns() c USING (table_oid)").fetchone()[0]
        > 0
    )
    con.execute(
        "SELECT count(*) FROM duckdb_views() v " "JOIN duckdb_columns() c ON (v.view_oid = c.table_oid)"
    ).fetchone()
    con.close()
