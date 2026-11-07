"""OP_CREATE_SCHEMA refuses a malformed request and a catalog that grants no create.

Run:
    uv run pytest integration_tests/test_create_schema_wire.py -v
"""

import duckdb
import pytest

from _paths import EXT_PATH
from _targets import TARGETS, Target, connect

from n6k_protocol.protocol import OP_CREATE_SCHEMA


def _client() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.parametrize(
    "args",
    [
        {"schema": "", "on_conflict": "error"},
        {"schema": "never_created", "on_conflict": "replace"},
        {"schema": "never_created", "on_conflict": ""},
    ],
    ids=["no-schema", "replace", "no-on-conflict"],
)
async def test_malformed_create_schema_is_refused(target: Target, args: dict) -> None:
    async with connect(target) as (client, _hello):
        err = await client.error(OP_CREATE_SCHEMA, **args)
        assert err["exception_type"] == "InvalidInputException"


def test_bridge_catalog_refuses(server: int) -> None:
    conn = _client()
    # The `bridge_db` alias gets a bridge-backed catalog, whose schemas grant no create.
    conn.execute(f"ATTACH 'n6k://localhost:{server}' AS bridge_db (TYPE n6k)")
    with pytest.raises(duckdb.PermissionException, match="does not allow CREATE SCHEMA"):
        conn.execute("CREATE SCHEMA bridge_db.refused")
    conn.close()
