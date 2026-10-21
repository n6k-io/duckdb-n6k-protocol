"""`ATTACH ':memory:' AS workspace (TYPE virtual_catalog_provider)` must behave
identically to plain `:memory:` for native tables.

The virtual_catalog_provider storage extension wraps a native DuckCatalog. A bare
catalog with no provider registered should be transparent:
CREATE / INSERT / SELECT / ALTER / DROP / BEGIN-ROLLBACK / transactions
behave the same as unwrapped DuckDB.
"""

import duckdb

from n6k_server.extension import load_virtual_catalog_provider

CONN_CONFIG: dict[str, str | bool | int | float | list[str]] = {"allow_unsigned_extensions": "true"}


def _attach(conn: duckdb.DuckDBPyConnection, name: str = "workspace") -> None:
    load_virtual_catalog_provider(conn)
    conn.execute(f"ATTACH ':memory:' AS {name} (TYPE virtual_catalog_provider)")


def test_create_insert_select() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE TABLE workspace.main.t(id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO workspace.main.t VALUES (1, 'a'), (2, 'b')")
    rows = conn.execute("SELECT * FROM workspace.main.t ORDER BY id").fetchall()
    assert rows == [(1, "a"), (2, "b")]


def test_alter_add_column() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE TABLE workspace.main.t(id INTEGER)")
    conn.execute("ALTER TABLE workspace.main.t ADD COLUMN score DOUBLE")
    conn.execute("INSERT INTO workspace.main.t VALUES (1, 3.14)")
    rows = conn.execute("SELECT id, score FROM workspace.main.t").fetchall()
    assert rows == [(1, 3.14)]


def test_drop_table() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE TABLE workspace.main.t(id INTEGER)")
    conn.execute("DROP TABLE workspace.main.t")
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables " "WHERE table_catalog='workspace' AND table_schema='main'"
    ).fetchall()
    assert rows == []


def test_create_view() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE TABLE workspace.main.t(id INTEGER)")
    conn.execute("INSERT INTO workspace.main.t VALUES (1), (2), (3)")
    conn.execute("CREATE VIEW workspace.main.v AS SELECT id * 10 AS big FROM workspace.main.t")
    rows = conn.execute("SELECT * FROM workspace.main.v ORDER BY big").fetchall()
    assert rows == [(10,), (20,), (30,)]


def test_transaction_rollback_discards_insert() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE TABLE workspace.main.t(id INTEGER)")
    conn.execute("INSERT INTO workspace.main.t VALUES (1)")
    conn.execute("BEGIN TRANSACTION")
    conn.execute("INSERT INTO workspace.main.t VALUES (2)")
    conn.execute("ROLLBACK")
    rows = conn.execute("SELECT id FROM workspace.main.t ORDER BY id").fetchall()
    assert rows == [(1,)]


def test_transaction_commit_persists_insert() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE TABLE workspace.main.t(id INTEGER)")
    conn.execute("BEGIN TRANSACTION")
    conn.execute("INSERT INTO workspace.main.t VALUES (1), (2)")
    conn.execute("COMMIT")
    rows = conn.execute("SELECT id FROM workspace.main.t ORDER BY id").fetchall()
    assert rows == [(1,), (2,)]


def test_schema_create_and_use() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE SCHEMA workspace.myschema")
    conn.execute("CREATE TABLE workspace.myschema.t(id INTEGER)")
    conn.execute("INSERT INTO workspace.myschema.t VALUES (42)")
    rows = conn.execute("SELECT id FROM workspace.myschema.t").fetchall()
    assert rows == [(42,)]


def test_show_tables() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    conn.execute("CREATE TABLE workspace.main.a(x INTEGER)")
    conn.execute("CREATE TABLE workspace.main.b(y VARCHAR)")
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog='workspace' AND table_schema='main' "
        "ORDER BY table_name"
    ).fetchall()
    assert [r[0] for r in rows] == ["a", "b"]


def test_catalog_type_reported() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    _attach(conn)
    rows = conn.execute("SELECT type FROM duckdb_databases() WHERE database_name='workspace'").fetchall()
    assert rows == [("virtual_catalog_provider",)]
