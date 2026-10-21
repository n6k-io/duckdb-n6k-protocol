"""End-to-end: typed exception class + message round-trip through WS.

Server errors must surface on the client with the matching DuckDB
exception subclass and a `n6k[<catalog>] <OP>: ` prefix identifying
catalog + op.

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
    c.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    return c


def test_server_side_catalog_error_surfaces_typed(con: duckdb.DuckDBPyConnection) -> None:
    """n6k_catalog_exec forces the server to run SQL; a missing-table
    reference there raises CatalogException on the server's DuckDB and
    must propagate as CatalogException on the client with the n6k prefix."""
    with pytest.raises(duckdb.CatalogException) as ei:
        con.execute("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM nonexistent_xyz WHERE 1=1')").fetchall()
    msg = str(ei.value)
    assert "n6k[db]" in msg
    assert " EXEC:" in msg
    assert "nonexistent_xyz" in msg


def test_server_side_parser_error_surfaces_typed(con: duckdb.DuckDBPyConnection) -> None:
    """Malformed SQL on the server surfaces with its specific class
    (Parser/Syntax), not IOException."""
    with pytest.raises((duckdb.ParserException, duckdb.SyntaxException)) as ei:
        con.execute("SELECT * FROM n6k_catalog_exec('db', 'NOT VALID SQL AT ALL;')").fetchall()
    assert "n6k[db]" in str(ei.value)


def test_error_prefix_disambiguates_multiple_attached_catalogs() -> None:
    """When multiple catalogs are attached, the prefix identifies which one failed."""
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.execute(f"LOAD '{EXT_PATH}'")
    c.execute("ATTACH 'n6k://localhost:8099' AS prod (TYPE n6k)")
    c.execute("ATTACH 'n6k://localhost:8099' AS staging (TYPE n6k)")

    with pytest.raises(duckdb.CatalogException) as ei:
        c.execute("SELECT * FROM n6k_catalog_exec('prod', 'DELETE FROM nonexistent_xyz WHERE 1=1')").fetchall()
    msg = str(ei.value)
    assert "n6k[prod]" in msg
    assert "n6k[staging]" not in msg


def test_local_binder_catches_unknown_attached_table(con: duckdb.DuckDBPyConnection) -> None:
    """SELECT from a missing attached-catalog table is caught by DuckDB's local
    binder before hitting the server. Class is still CatalogException — verifies
    the typed-error contract holds regardless of where the error originated."""
    with pytest.raises(duckdb.CatalogException) as ei:
        con.execute("SELECT * FROM db.main.nonexistent_table_xyz")
    assert "nonexistent_table_xyz" in str(ei.value)


def test_streaming_query_first_frame_error_surfaces_typed(con: duckdb.DuckDBPyConnection) -> None:
    """A QUERY op (streaming) that errors on the server BEFORE any data flows
    must throw typed on the client (the first-frame peek in
    StartStreamingArrowFromRequestState). This was previously flattened to
    IOException because nanoarrow's ArrowError channel only carries a string."""
    with pytest.raises(duckdb.CatalogException) as ei:
        con.execute("SELECT * FROM n6k_catalog_query('db', 'SELECT * FROM nonexistent_streaming_xyz')").fetchall()
    msg = str(ei.value)
    assert "n6k[db]" in msg
    assert " QUERY:" in msg
    assert "nonexistent_streaming_xyz" in msg


def test_streaming_query_binder_error_surfaces_typed(con: duckdb.DuckDBPyConnection) -> None:
    """Bad column reference in a QUERY — server-side binder error on the
    streaming path must reach the client as BinderException."""
    with pytest.raises(duckdb.BinderException) as ei:
        con.execute("SELECT * FROM n6k_catalog_query('db', 'SELECT bogus_col_xyz')").fetchall()
    msg = str(ei.value)
    assert "n6k[db]" in msg
    assert " QUERY:" in msg
    assert "bogus_col_xyz" in msg
