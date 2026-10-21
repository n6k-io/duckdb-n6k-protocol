"""Test catalog-scoped RPC table functions.

Requires the test server running:
    uv run python -m n6k_server.test_server --port 8099

Then run:
    uv run pytest integration_tests/test_catalog_rpc.py -v
"""

import os
from decimal import Decimal
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
        pytest.skip("Test server not running on port 8099")


@pytest.fixture()
def conn():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db (TYPE n6k)")
    return c


def test_dynamic_dispatch_echo(conn):
    """foo.echo(args) dispatches to /rpc/echo."""
    result = conn.execute("SELECT * FROM db.echo('hello')").fetchall()
    assert len(result) == 1
    assert result[0][0] == "hello"


def test_dynamic_dispatch_echo_multiple_args(conn):
    """foo.echo(arg1, arg2) passes multiple args."""
    result = conn.execute("SELECT * FROM db.echo('hello', 42)").fetchall()
    assert len(result) == 1
    assert result[0][0] == "hello"
    assert result[0][1] == 42


def test_named_rpc_dispatch(conn):
    """foo.rpc('echo', args) dispatches to /rpc/echo."""
    result = conn.execute("SELECT * FROM db.rpc('echo', 'world')").fetchall()
    assert len(result) == 1
    assert result[0][0] == "world"


def test_named_rpc_multiple_args(conn):
    """foo.rpc('echo', arg1, arg2) passes multiple args."""
    result = conn.execute("SELECT * FROM db.rpc('echo', 'test', 99)").fetchall()
    assert len(result) == 1
    assert result[0][0] == "test"
    assert result[0][1] == 99


def test_decimal_scalar_arg_serialized_as_number(conn):
    """Regression: a DECIMAL scalar arg must reach the server as a JSON number,
    not a quoted string.

    A non-integer literal like 1.1 is typed DECIMAL(2,1) by DuckDB, and the RPC
    table function's `varargs = LogicalType::ANY` lets it reach RpcValueToYYJSON
    still typed DECIMAL. That switch had no DECIMAL case, so it fell to the
    `default:` arm and emitted `"1.1"` (a string) instead of `1.1` (a number).
    The `echo` RPC mirrors its args back typed, so the returned Python type tells us
    which was sent: a JSON number comes back numeric, a JSON string as VARCHAR (i.e.
    `str`). Before the fix this came back as the str '1.1'.

    It is `Decimal` rather than `float` because `echo` is now a SQL macro selecting
    its argument straight back, so the server's own DECIMAL survives. The old Python
    registry rebuilt the value through pyarrow and flattened it to float64 — a
    detail of that fixture, never of the wire."""
    result = conn.execute("SELECT * FROM db.echo(1.1)").fetchall()
    assert len(result) == 1
    assert result[0][0] == Decimal("1.1")
    assert not isinstance(result[0][0], str)


def test_hugeint_scalar_arg_serialized_as_number(conn):
    """Regression: HUGEINT shared the DECIMAL defect — numeric but absent from the
    serializer's switch, so it fell to `default:` and was emitted as a string.
    Echo mirrors a JSON number back as an integer column."""
    result = conn.execute("SELECT * FROM db.echo(42::HUGEINT)").fetchall()
    assert len(result) == 1
    assert result[0][0] == 42
    assert not isinstance(result[0][0], str)


def test_dynamic_dispatch_with_explicit_schema(conn):
    """foo.main.echo(args) with explicit schema."""
    result = conn.execute("SELECT * FROM db.main.echo('explicit')").fetchall()
    assert len(result) == 1
    assert result[0][0] == "explicit"


def test_global_rpc_still_works(conn):
    """Global n6k_catalog_rpc('db', 'echo', ...) works."""
    result = conn.execute("SELECT * FROM n6k_catalog_rpc('db', 'echo', 'global')").fetchall()
    assert len(result) == 1
    assert result[0][0] == "global"


def test_filter_on_varchar_after_numeric_columns(conn):
    """Regression: a WHERE on a VARCHAR column that follows numeric columns in an
    RPC table-function result segfaulted the extension.

    The eager Arrow producer streams every column in schema order but the RPC
    table functions claimed `projection_pushdown`, which they never honored. With
    a filter present, DuckDB reordered `column_ids` (filter column first) and the
    scan mapped the VARCHAR output vector onto a numeric Arrow buffer — reading a
    double as a string offset and faulting in SetVectorString. `xy_labeled`
    returns (double x, double y, varchar label); filtering on `label` reproduces
    it deterministically."""
    rows = conn.execute("SELECT * FROM db.xy_labeled() WHERE label = 'a' ORDER BY x").fetchall()
    assert rows == [(1.0, 10.0, "a"), (3.0, 30.0, "a")]


def test_streaming_rpc_drips_through_native_reader(conn):
    """A streaming server RPC (its handler yields batches one at a time) is
    consumed by the native extension via the lazy Arrow stream reader, not
    buffered whole before returning rows."""
    rows = conn.execute("SELECT n FROM db.stream_counter(5) ORDER BY n").fetchall()
    assert [r[0] for r in rows] == [0, 1, 2, 3, 4]
