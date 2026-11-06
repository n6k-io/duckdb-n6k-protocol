"""CREATE VIEW on a schema whose catalog grants no create.

Run:
    uv run pytest integration_tests/test_create_view_alias.py -v
"""

import os

import duckdb
import pytest

from _paths import EXT_PATH


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


def _connect() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


def test_schema_without_create_is_refused(server):
    conn = _connect()
    # The `bridge_db` alias gets a bridge-backed catalog, whose schemas grant no create.
    conn.execute(f"ATTACH 'n6k://localhost:{server}' AS bridge_db (TYPE n6k)")
    with pytest.raises(duckdb.Error, match="does not have 'create' permission"):
        conn.execute("CREATE VIEW bridge_db.main.v_refused AS SELECT id FROM bridge_db.main.items")
    conn.close()
