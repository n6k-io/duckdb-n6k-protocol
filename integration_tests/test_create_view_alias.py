"""CREATE / DROP VIEW over an n6k attach, whose alias may differ from the server catalog.

The view's body ships as a crossing plan (OP_CREATE_VIEW), so the server stores it under
its own catalog name and the client's alias never reaches it.

Run:
    uv run pytest integration_tests/test_create_view_alias.py -v
"""

import os
import uuid

import duckdb
import pytest

from _paths import EXT_PATH
from _targets import FIXTURE_CATALOG


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


def _connect() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


@pytest.fixture(params=["other", FIXTURE_CATALOG])
def attached(server, request):
    alias = request.param
    c = _connect()
    c.execute(f"ATTACH 'n6k://localhost:{server}' AS {alias} (TYPE n6k, CATALOG '{FIXTURE_CATALOG}')")
    view = "v_" + uuid.uuid4().hex[:8]
    yield c, alias, view
    c.close()


def _server_view_sql(conn: duckdb.DuckDBPyConnection, alias: str, view: str) -> list[tuple]:
    return conn.execute(
        f"SELECT * FROM {alias}.query('SELECT sql FROM duckdb_views() WHERE view_name = ''{view}''')"
    ).fetchall()


def _users(conn: duckdb.DuckDBPyConnection, alias: str, columns: str = "id, name") -> list[tuple]:
    return conn.execute(f"SELECT {columns} FROM {alias}.main.users ORDER BY id").fetchall()


def test_create_view(attached):
    conn, alias, view = attached
    conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT id, name FROM {alias}.main.users")
    assert conn.execute(f"SELECT id, name FROM {alias}.main.{view} ORDER BY id").fetchall() == _users(conn, alias)


def test_stored_view_names_no_catalog(attached):
    conn, alias, view = attached
    conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT id FROM {alias}.main.users")
    [(sql,)] = _server_view_sql(conn, alias, view)
    assert f"{alias}." not in sql
    assert f"{FIXTURE_CATALOG}." not in sql


def test_create_or_replace_and_if_not_exists(attached):
    conn, alias, view = attached
    conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT id FROM {alias}.main.users")
    with pytest.raises(duckdb.Error):
        conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT name FROM {alias}.main.users")
    conn.execute(f"CREATE VIEW IF NOT EXISTS {alias}.main.{view} AS SELECT name FROM {alias}.main.users")
    assert conn.execute(f"SELECT * FROM {alias}.main.{view} ORDER BY 1").fetchall() == _users(conn, alias, "id")
    conn.execute(f"CREATE OR REPLACE VIEW {alias}.main.{view} AS SELECT name FROM {alias}.main.users")
    assert conn.execute(f"SELECT * FROM {alias}.main.{view} ORDER BY 1").fetchall() == sorted(
        _users(conn, alias, "name")
    )


def test_drop_view(attached):
    conn, alias, view = attached
    conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT id FROM {alias}.main.users")
    conn.execute(f"DROP VIEW {alias}.main.{view}")
    assert _server_view_sql(conn, alias, view) == []
    with pytest.raises(duckdb.Error):
        conn.execute(f"SELECT * FROM {alias}.main.{view}")
    conn.execute(f"DROP VIEW IF EXISTS {alias}.main.{view}")


def test_view_bodies(attached):
    conn, alias, view = attached
    conn.execute(
        f"CREATE VIEW {alias}.main.{view}(a, b) AS "
        f"SELECT u.id, v.name FROM {alias}.main.users u JOIN {alias}.main.users v ON u.id = v.id WHERE u.age > 26"
    )
    assert [r[0] for r in conn.execute(f"DESCRIBE {alias}.main.{view}").fetchall()] == ["a", "b"]
    assert conn.execute(f"SELECT a, b FROM {alias}.main.{view} ORDER BY a").fetchall() == [
        (1, "Alice"),
        (3, "Charlie"),
    ]


def test_unqualified_body_after_use(attached):
    conn, alias, view = attached
    conn.execute(f"USE {alias}")
    conn.execute(f"CREATE VIEW {view} AS SELECT id FROM users")
    assert conn.execute(f"SELECT * FROM {view} ORDER BY 1").fetchall() == _users(conn, alias, "id")


def test_view_in_a_transaction(attached):
    conn, alias, view = attached
    conn.execute("BEGIN")
    conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT id FROM {alias}.main.users")
    assert conn.execute(f"SELECT count(*) FROM {alias}.main.{view}").fetchone() == (3,)
    conn.execute(f"DROP VIEW {alias}.main.{view}")
    with pytest.raises(duckdb.Error):
        conn.execute(f"SELECT * FROM {alias}.main.{view}")
    conn.execute("COMMIT")
    assert _server_view_sql(conn, alias, view) == []


def test_rolled_back_view_is_gone(attached):
    conn, alias, view = attached
    conn.execute("BEGIN")
    conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT id FROM {alias}.main.users")
    conn.execute("ROLLBACK")
    assert _server_view_sql(conn, alias, view) == []
    with pytest.raises(duckdb.Error):
        conn.execute(f"SELECT * FROM {alias}.main.{view}")


@pytest.mark.parametrize(
    "body",
    ["SELECT 1 AS x", "SELECT u.id FROM {alias}.main.users u JOIN local_t l ON u.id = l.x"],
    ids=["nothing-remote", "local-table"],
)
def test_body_that_does_not_run_on_the_server_is_refused(attached, body):
    conn, alias, view = attached
    conn.execute("CREATE TABLE local_t(x INTEGER)")
    with pytest.raises(duckdb.Error, match="does not run wholly on the source"):
        conn.execute(f"CREATE VIEW {alias}.main.{view} AS {body.format(alias=alias)}")
    assert _server_view_sql(conn, alias, view) == []


def test_body_in_another_attach_is_refused(attached, server):
    conn, alias, view = attached
    conn.execute(f"ATTACH 'n6k://localhost:{server}' AS peer (TYPE n6k, CATALOG 'peer')")
    with pytest.raises(duckdb.Error, match="does not run wholly on the source"):
        conn.execute(f"CREATE VIEW {alias}.main.{view} AS SELECT id FROM peer.main.users")
    assert _server_view_sql(conn, alias, view) == []


def test_schema_without_create_is_refused(server):
    conn = _connect()
    # The `bridge_db` alias gets a bridge-backed catalog, whose schemas grant no create.
    conn.execute(f"ATTACH 'n6k://localhost:{server}' AS bridge_db (TYPE n6k)")
    with pytest.raises(duckdb.Error, match="does not have 'create' permission"):
        conn.execute("CREATE VIEW bridge_db.main.v_refused AS SELECT id FROM bridge_db.main.items")
    conn.close()
