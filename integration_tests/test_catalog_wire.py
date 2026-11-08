"""OP_TABLES_LIST and OP_TABLE_SCHEMA describe what the session's catalog has committed.

Run:
    uv run pytest integration_tests/test_catalog_wire.py -v
"""

import io

import pyarrow.ipc as ipc
import pytest

from _targets import TARGETS, Target, connect

from n6k_protocol.protocol import (
    FT_RESP_CHUNK,
    FT_RESP_SCHEMA,
    OP_TABLE_SCHEMA,
    OP_TABLES_LIST,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_tables_list(target: Target) -> None:
    async with connect(target) as (client, _hello):
        frames = await client.request(OP_TABLES_LIST)
        chunk = [h for h, _ in frames if h["t"] == FT_RESP_CHUNK][0]
        users = [t for t in chunk["tables"] if t["schema"] == "main" and t["name"] == "users"]
        assert users and "select" in users[0]["verbs"]
        assert users[0]["row_ids"] is True
        main = [s for s in chunk["schemas"] if s["name"] == "main"]
        assert main and main[0]["verbs"] == ["create"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_table_schema(target: Target) -> None:
    async with connect(target) as (client, _hello):
        frames = await client.request(OP_TABLE_SCHEMA, schema="main", table="users")
        schema = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        assert "id" in ipc.open_stream(io.BytesIO(schema)).schema.names
