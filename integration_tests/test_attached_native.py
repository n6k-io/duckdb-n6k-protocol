"""A real DuckDB client ATTACHed to the C++ server, over the in-process `/tuned` mount.

Everything else that drives `/tuned` speaks the wire directly, which proves the server
answers correctly but never exercises the *client* against it: the extension's bind paths,
its schema probes and its Arrow decoding only ever meet the Python engine. RPC is where
that gap bites — `n6k_catalog_rpc_table` has to learn a result schema at bind time, and how
it asks for one is only correct if the server can actually bind the call.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_attached_native.py -v
"""

import json
import os
import time
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_URL = "http://localhost:8099"
# The mount is a path on the same server; the driver turns n6k://host/path into ws://host/path/ws.
TUNED_DSN = "n6k://localhost:8099/tuned"


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
    c.execute(f"LOAD '{EXT_PATH}'")
    # The alias must name a served catalog: the HELLO carries it, and `db` is what the
    # mount attaches by default.
    c.execute(f"ATTACH '{TUNED_DSN}' AS db (TYPE n6k)")
    return c


def test_scan_over_the_native_server(con):
    assert con.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


def test_marker_confirms_the_native_catalog(con):
    assert con.execute("SELECT catalog FROM db.main.marker").fetchone()[0] == "db"


def test_rpc_scalar_through_the_client(con):
    """`db.<fn>(...)` binds any unknown table-function name into an RPC."""
    assert con.execute("SELECT doubled FROM db.doubler(21)").fetchone()[0] == 42


def test_rpc_scalar_with_several_args(con):
    rows = con.execute("SELECT arg0, arg1 FROM db.echo('a', 'b')").fetchall()
    assert rows == [("a", "b")]


def test_rpc_table_binds_and_streams(con):
    """The bind-time schema probe must be answerable by a server that really binds.

    The probe used to send RPC_SCALAR with empty args, which only worked while the
    server answered from a registry carrying a declared schema. Against DuckDB
    dispatch `sum_table()` does not bind, so the probe now sends the real args plus a
    schema-only Arrow body.
    """
    con.execute("CREATE TABLE local_rows (id BIGINT)")
    con.execute("INSERT INTO local_rows VALUES (10), (20), (30)")
    total = con.execute(
        "SELECT total FROM n6k_catalog_rpc_table('db', 'sum_table', (SELECT id FROM local_rows))"
    ).fetchone()[0]
    assert total == 60


def test_rpc_table_against_a_declared_table_parameter(con):
    """Same call shape, but a native in-out function — the other calling convention."""
    con.execute("CREATE TABLE local_rows2 (id BIGINT)")
    con.execute("INSERT INTO local_rows2 VALUES (1), (2), (3), (4)")
    total = con.execute(
        "SELECT total FROM n6k_catalog_rpc_table('db', 'n6k_testing_sum_table', (SELECT id FROM local_rows2))"
    ).fetchone()[0]
    assert total == 10


def test_invalidation_makes_a_new_table_visible(con):
    """End to end: the push has to make the client drop its cached schema and refetch.

    The wire tests prove the frame arrives; this proves it does something. The client
    caches a schema's table list on first lookup, so a table created server-side after
    that stays invisible until an invalidation lands.
    """
    # Populate the client's cache for `main`.
    assert con.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3

    # Create a table on the server, over the same session. The client is not told.
    # Catalog-qualified deliberately: OP_EXEC is raw passthrough SQL and is NOT rewritten to the
    # session's catalog, so a bare `main.appeared` lands in the server connection's default
    # catalog instead of the served one.
    con.execute("SELECT * FROM n6k_catalog_exec('db', 'CREATE TABLE db.main.appeared (i INTEGER)')").fetchall()

    request = urllib.request.Request(f"{SERVER_URL}/debug/push_invalidate?catalog=db&schemas=main", method="POST")
    with urllib.request.urlopen(request, timeout=5) as response:
        assert json.loads(response.read())["sent"] >= 1

    # The push is asynchronous, so give the client a moment to process it.
    for _ in range(50):
        try:
            con.execute("SELECT count(*) FROM db.main.appeared").fetchone()
            return
        except duckdb.Error:
            time.sleep(0.1)
    pytest.fail("table never became visible after the invalidation push")


def test_streaming_rpc_through_the_client(con):
    rows = con.execute("SELECT n FROM db.n6k_testing_stream_counter(5) ORDER BY n").fetchall()
    assert [r[0] for r in rows] == [0, 1, 2, 3, 4]
