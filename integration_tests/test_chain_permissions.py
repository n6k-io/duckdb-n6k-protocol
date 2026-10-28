"""n6k_table_permissions over a network→bridge→table chain.

A network (TYPE n6k) client attaches to the test server, whose `bridge_db`
catalog is itself a bridge (TYPE vcatbridge) over a source connection. This
verifies that a bridge's verbs and primary keys survive the network hop: the server
reports them via n6k_table_permissions, ships them in the tables_list response, and
the client surfaces them as n6k_remote rows.

Requires the test server running:
    uv run python -m n6k_server.test_server --port 8099

Then run:
    uv run pytest integration_tests/test_chain_permissions.py -v
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
        pytest.skip("Test server not running on port 8099")


@pytest.fixture()
def conn():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    # The `bridge_db` alias routes the server's session factory to a bridge-backed
    # catalog (see test_server/seeding.seed_bridge).
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS bridge_db (TYPE n6k)")
    yield c


def test_bridge_permissions_over_network(conn):
    conn.execute("SELECT * FROM n6k_invalidate_cache('bridge_db', 'main')").fetchall()
    rows = conn.execute(
        "SELECT name, kind, verbs, primary_key "
        "FROM n6k_table_permissions('bridge_db', schema := 'main') "
        "WHERE name IN ('items', 'readonly_items')"
    ).fetchall()
    by_name = {r[0]: r for r in rows}
    # Bridge readwrite → writes; bridge structure is immutable → no alter;
    # its primary key survives the network→bridge hop.
    items = by_name["items"]
    assert items[1] == "n6k_remote"
    assert {"select", "insert", "update", "delete"} <= set(items[2])
    assert "alter" not in items[2]
    assert items[3] == ["id"]
    # Bridge read-only → no writes; discovery still reports the key.
    readonly = by_name["readonly_items"]
    assert readonly[1] == "n6k_remote"
    assert not {"insert", "update", "delete", "alter"} & set(readonly[2])
    assert readonly[3] == ["id"]
