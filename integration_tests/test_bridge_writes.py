"""Writes through a real client into a bridge-backed catalog, whose far tables come from another extension that
compiles its own copy of crossing.

Run:
    uv run pytest integration_tests/test_bridge_writes.py -v
"""

from collections.abc import Iterator

import duckdb
import pytest

from _targets import server_is_up
from test_connect_url import N6K_URL, _connect

pytestmark = pytest.mark.skipif(not server_is_up(), reason="test server not running on :8099")


@pytest.fixture
def bridge() -> Iterator[duckdb.DuckDBPyConnection]:
    c = _connect()
    # The `bridge_db` alias routes the test server's session to a bridge-backed catalog, seeded per connection.
    c.execute(f"ATTACH '{N6K_URL}' AS bridge_db (TYPE n6k)")
    yield c
    c.close()


def _items(c: duckdb.DuckDBPyConnection) -> list[tuple]:
    return c.execute("SELECT * FROM bridge_db.main.items ORDER BY id").fetchall()


def test_update_insert_and_delete(bridge: duckdb.DuckDBPyConnection) -> None:
    bridge.execute("UPDATE bridge_db.main.items SET name = 'plain' WHERE id = 1")
    bridge.execute("INSERT INTO bridge_db.main.items VALUES (3, 'new')")
    bridge.execute("DELETE FROM bridge_db.main.items WHERE id = 2")
    assert _items(bridge) == [(1, "plain"), (3, "new")]


def test_writes_addressed_by_a_subquery(bridge: duckdb.DuckDBPyConnection) -> None:
    bridge.execute(
        "UPDATE bridge_db.main.items SET name = 'subq' WHERE id IN (SELECT id FROM bridge_db.main.items WHERE id = 2)"
    )
    assert _items(bridge) == [(1, "Widget"), (2, "subq")]
    bridge.execute("DELETE FROM bridge_db.main.items WHERE id IN (SELECT id FROM bridge_db.main.items WHERE id = 1)")
    assert _items(bridge) == [(2, "subq")]


def test_a_read_only_table_refuses_writes(bridge: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(duckdb.PermissionException, match="update"):
        bridge.execute("UPDATE bridge_db.main.readonly_items SET name = 'x' WHERE id = 1")
    assert bridge.execute("SELECT * FROM bridge_db.main.readonly_items").fetchall() == [(1, "Locked")]
