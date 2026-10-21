"""OP_TABLES_LIST / OP_TABLE_SCHEMA inside an open transaction (`txn`).

Run:
    uv run pytest integration_tests/test_txn_catalog_wire.py -v
"""

import io

import pyarrow.ipc as ipc
import pytest

from _targets import TARGETS, Target, connect

from n6k_protocol.protocol import (
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
    OP_TABLE_SCHEMA,
    OP_TABLES_LIST,
    OP_TXN_BEGIN,
    OP_TXN_ROLLBACK,
)


async def _begin(client) -> int:
    frames = await client.request(OP_TXN_BEGIN)
    return [h for h, _ in frames if h["t"] == FT_RESP_END][0]["txn"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_tables_list_in_a_transaction(target: Target) -> None:
    async with connect(target) as (client, _hello):
        txn = await _begin(client)
        frames = await client.request(OP_TABLES_LIST, txn=txn)
        chunk = [h for h, _ in frames if h["t"] == FT_RESP_CHUNK][0]
        users = [t for t in chunk["tables"] if t["schema"] == "main" and t["name"] == "users"]
        assert users and "select" in users[0]["verbs"]
        main = [s for s in chunk["schemas"] if s["name"] == "main"]
        assert main and main[0]["verbs"] == ["create"]
        await client.request(OP_TXN_ROLLBACK, txn=txn)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_table_schema_in_a_transaction(target: Target) -> None:
    async with connect(target) as (client, _hello):
        txn = await _begin(client)
        frames = await client.request(OP_TABLE_SCHEMA, schema="main", table="users", txn=txn)
        schema = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        assert "id" in ipc.open_stream(io.BytesIO(schema)).schema.names
        await client.request(OP_TXN_ROLLBACK, txn=txn)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.parametrize(
    "op, args", [(OP_TABLES_LIST, {}), (OP_TABLE_SCHEMA, {"schema": "main", "table": "users"})], ids=["list", "schema"]
)
async def test_an_unknown_transaction_is_refused(target: Target, op: int, args: dict) -> None:
    async with connect(target) as (client, _hello):
        err = await client.error(op, txn=987654, **args)
        assert err["exception_type"] == "TransactionException"
