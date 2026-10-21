"""Projection pushdown must survive column names that collide with the query syntax.

A projected scan is requested over a URL-style query string,
`columns=a,b&filters=<urlencoded-json>`, built by
src/n6k_client/n6k_async_scan.cpp (the default path) and
src/n6k_client/include/n6k_pushdown.hpp (the blocking arrow-scan path). The
server splits it on `&`, then `=`, then `,`, and URL-decodes each piece
(src/n6k_client/n6k_str_utils.cpp, UrlQueryToOpScanBody).

The `filters=` half is passed through n6k::UrlEncode. The `columns=` half is
not. DuckDB permits `,`, `&`, `=` and `%` inside a quoted identifier, so a
column whose name contains one is torn into pieces that name other columns.

Not a cosmetic failure: ArrowToDuckDB decodes each batch's children
positionally while looking types up by base column id, so a split that happens
to preserve the column *count* returns wrong values under the right names
rather than an error.

`columns=` is only sent for a projection that is not the identity, hence the
two-column tables and single-column SELECTs below.

Run:
    uv run pytest integration_tests/test_scan_exotic_column_names.py -v
"""

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


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _make_table(con, table: str, odd_column: str) -> None:
    """Create the fixture server-side.

    Raw SQL over n6k_catalog_exec rather than CREATE TABLE through the attached
    catalog: the client-side path would have to encode the column name on the
    way out too, so using it here would test the bug with the bug.

    Each served connection gets its own in-memory catalog (see
    packages/python/src/n6k_server/test_server/seeding.py), so these tables are
    private to this test's connection.
    """
    con.execute(
        "SELECT * FROM n6k_catalog_exec('db', ?)",
        [f"CREATE OR REPLACE TABLE db.main.{table} ({_quote(odd_column)} INTEGER, plain INTEGER)"],
    ).fetchall()
    con.execute(
        "SELECT * FROM n6k_catalog_exec('db', ?)",
        [f"INSERT INTO db.main.{table} VALUES (11, 22)"],
    ).fetchall()


# The three characters UrlQueryToOpScanBody splits on, plus the escape it decodes.
EXOTIC = [
    ("comma", "a,b"),
    ("ampersand", "a&b"),
    ("equals", "a=b"),
    ("percent", "a%41b"),
]


@pytest.mark.parametrize("label,column", EXOTIC)
def test_projected_scan_reads_exotic_column(con, label, column):
    table = f"odd_{label}"
    _make_table(con, table, column)

    rows = con.execute(f"SELECT {_quote(column)} FROM db.main.{table}").fetchall()

    assert rows == [(11,)], f"projected scan of column {column!r} returned {rows!r}"


@pytest.mark.parametrize("label,column", EXOTIC)
def test_projected_scan_does_not_cross_columns(con, label, column):
    """The count-preserving case, which no width check can catch.

    `a,b` splits into exactly two names, so a server that resolved both would
    answer with the right number of columns and the wrong values. Selecting the
    plain column alongside pins which value belongs to which name.
    """
    table = f"mix_{label}"
    _make_table(con, table, column)

    rows = con.execute(f"SELECT plain, {_quote(column)} FROM db.main.{table}").fetchall()

    assert rows == [(22, 11)], f"columns crossed for {column!r}: {rows!r}"
