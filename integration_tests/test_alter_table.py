"""Test ALTER TABLE over an attached n6k catalog (network path).

Requires the test server running:
    uv run python -m n6k_server.test_server --port 8099

Run:
    uv run pytest integration_tests/test_alter_table.py -v
"""

import os
import urllib.error
import urllib.request
import uuid

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
def test_env():
    ns = "alter_" + uuid.uuid4().hex[:8]
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db (TYPE n6k)")
    tbl = f'db.main."{ns}"'
    c.execute(f"CREATE TABLE {tbl} AS SELECT * FROM VALUES " f"(1, 'alice', 30), (2, 'bob', 25) t(id, name, age)")
    yield c, ns, tbl
    try:
        c.execute(f"DROP TABLE IF EXISTS {tbl}")
    except Exception:
        pass


def _col_names(conn: duckdb.DuckDBPyConnection, tbl: str) -> list[str]:
    rows = conn.execute(f"DESCRIBE {tbl}").fetchall()
    return [r[0] for r in rows]


def test_add_column(test_env):
    conn, ns, tbl = test_env
    conn.execute(f"ALTER TABLE {tbl} ADD COLUMN score DOUBLE")
    assert "score" in _col_names(conn, tbl)


def test_drop_column(test_env):
    conn, ns, tbl = test_env
    conn.execute(f"ALTER TABLE {tbl} DROP COLUMN age")
    cols = _col_names(conn, tbl)
    assert "age" not in cols
    assert {"id", "name"}.issubset(set(cols))


def test_rename_column(test_env):
    conn, ns, tbl = test_env
    conn.execute(f"ALTER TABLE {tbl} RENAME COLUMN name TO label")
    cols = _col_names(conn, tbl)
    assert "name" not in cols
    assert "label" in cols


def test_add_column_visible_after_select(test_env):
    conn, ns, tbl = test_env
    conn.execute(f"ALTER TABLE {tbl} ADD COLUMN note VARCHAR")
    rows = conn.execute(f"SELECT id, note FROM {tbl} ORDER BY id").fetchall()
    assert len(rows) == 2
    assert rows[0][1] is None


def test_alter_column_type_rejected_at_planning(test_env):
    """Unsupported kind must be rejected by the C++ client binder before any RPC."""
    conn, ns, tbl = test_env
    with pytest.raises(duckdb.BinderException) as ei:
        conn.execute(f"ALTER TABLE {tbl} ALTER COLUMN age TYPE BIGINT")
    msg = str(ei.value).lower()
    assert "not supported" in msg


def test_rename_table_rejected_at_planning(test_env):
    conn, ns, tbl = test_env
    new_ns = f"{ns}_renamed"
    with pytest.raises(duckdb.BinderException) as ei:
        conn.execute(f'ALTER TABLE {tbl} RENAME TO "{new_ns}"')
    msg = str(ei.value).lower()
    assert "not supported" in msg
