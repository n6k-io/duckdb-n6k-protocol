"""Seeded DuckDB fixtures for the test server.

Every served connection gets its own writable in-memory catalog (`seed_memory`),
so tests can UPDATE/INSERT without seeing each other's writes.
"""

import duckdb

from n6k_server.bridge import Policy, bridge
from n6k_server.test_server.fixtures import fixture_statements

# Clients that ATTACH with this alias get a bridge-backed catalog instead of the
# plain native one — used by the network→bridge→table chain test.
BRIDGE_CATALOG = "bridge_db"
# Clients that ATTACH with this alias get a catalog held in a sqlite file. Its scan cannot be
# serialized, so it pins that the server binds tables where the plan runs.
SQLITE_CATALOG = "sqlite_db"
_BRIDGE_CONN_CONFIG: dict[str, str | bool | int | float | list[str]] = {"allow_unsigned_extensions": "true"}


def seed_memory(con: duckdb.DuckDBPyConnection, catalog: str) -> None:
    con.execute(f"ATTACH ':memory:' AS \"{catalog}\"")
    seed_tables(con, catalog)


def seed_bridge(target: duckdb.DuckDBPyConnection, catalog: str) -> duckdb.DuckDBPyConnection:
    """Returns the source connection — the caller must keep it alive for the session's
    lifetime, since the bridge reads from it on every query.
    """
    source = duckdb.connect(config=_BRIDGE_CONN_CONFIG)
    source.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO items VALUES (1, 'Widget'), (2, 'Gadget')")
    source.execute("CREATE TABLE readonly_items (id INTEGER PRIMARY KEY, name VARCHAR)")
    source.execute("INSERT INTO readonly_items VALUES (1, 'Locked')")

    bridge(
        source,
        target,
        catalog,
        source_catalog="memory",
        policies=[
            Policy("main.items", "primary_key", ["id"]),
            Policy("main.items", "select", "true"),
            Policy("main.items", "insert", "true"),
            Policy("main.items", "update", "true"),
            Policy("main.items", "delete", "true"),
            Policy("main.readonly_items", "select", "true"),
        ],
    )
    return source


def seed_tables(con: duckdb.DuckDBPyConnection, catalog: str) -> None:
    for statement in fixture_statements(catalog):
        con.execute(statement)


def seed_sqlite(con: duckdb.DuckDBPyConnection, catalog: str, path: str) -> None:
    """Attach a sqlite file at `path` as `catalog` and seed one keyed table.

    Raises if the sqlite extension cannot be installed; the client then fails to
    attach and the test that wanted it skips.
    """
    con.execute("INSTALL sqlite; LOAD sqlite")
    con.execute(f"ATTACH '{path}' AS \"{catalog}\" (TYPE sqlite)")
    con.execute(f'CREATE TABLE "{catalog}".main.people(id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)')
    con.execute(f"INSERT INTO \"{catalog}\".main.people VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35)")
