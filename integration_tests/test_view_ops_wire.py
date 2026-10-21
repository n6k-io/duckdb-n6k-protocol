"""OP_CREATE_VIEW / OP_DROP_VIEW: a view whose body is a Substrait plan over the served catalog.

Run:
    uv run pytest integration_tests/test_view_ops_wire.py -v
"""

import uuid

import pytest

from _targets import FIXTURE_CATALOG, TARGETS, Target, connect, scan_plan

from n6k_protocol.protocol import (
    FT_RESP_CHUNK,
    FT_RESP_END,
    OP_CREATE_VIEW,
    OP_DROP_VIEW,
    OP_QUERY,
    OP_TABLES_LIST,
    OP_TXN_BEGIN,
    OP_TXN_ROLLBACK,
)


def _view() -> str:
    return "wv_" + uuid.uuid4().hex[:8]


def _create_args(view: str, **overrides) -> dict:
    args = {
        "schema": "main",
        "name": view,
        "on_conflict": "error",
        "names": ["uid", "uname"],
        "plan": scan_plan("main", "users", ["id", "name"]),
    }
    args.update(overrides)
    return args


async def _ok(client, op: int, **args) -> None:
    frames = await client.request(op, **args)
    end = [h for h, _ in frames if h["t"] == FT_RESP_END]
    assert end and end[0].get("ok") is True, frames


async def _listed(client, view: str, **args) -> bool:
    frames = await client.request(OP_TABLES_LIST, schema="main", **args)
    chunk = [h for h, _ in frames if h["t"] == FT_RESP_CHUNK][0]
    return any(t["name"] == view for t in chunk["tables"])


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_create_query_and_drop_a_view(target: Target) -> None:
    view = _view()
    async with connect(target) as (client, _hello):
        await _ok(client, OP_CREATE_VIEW, **_create_args(view))
        rows = await client.table(OP_QUERY, sql=f"SELECT * FROM {FIXTURE_CATALOG}.main.{view} ORDER BY uid")
        assert rows.schema.names == ["uid", "uname"]
        assert rows.column("uname").to_pylist() == ["Alice", "Bob", "Charlie"]
        assert await _listed(client, view)

        await _ok(client, OP_DROP_VIEW, schema="main", name=view, if_exists=False)
        assert not await _listed(client, view)
        await _ok(client, OP_DROP_VIEW, schema="main", name=view, if_exists=True)
        assert await client.error(OP_DROP_VIEW, schema="main", name=view, if_exists=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_on_conflict(target: Target) -> None:
    view = _view()
    async with connect(target) as (client, _hello):
        await _ok(client, OP_CREATE_VIEW, **_create_args(view))
        assert await client.error(OP_CREATE_VIEW, **_create_args(view))
        await _ok(client, OP_CREATE_VIEW, **_create_args(view, on_conflict="ignore", names=["a", "b"]))
        rows = await client.table(OP_QUERY, sql=f"SELECT * FROM {FIXTURE_CATALOG}.main.{view} LIMIT 0")
        assert rows.schema.names == ["uid", "uname"]
        await _ok(client, OP_CREATE_VIEW, **_create_args(view, on_conflict="replace", names=["a", "b"]))
        rows = await client.table(OP_QUERY, sql=f"SELECT * FROM {FIXTURE_CATALOG}.main.{view} LIMIT 0")
        assert rows.schema.names == ["a", "b"]
        await _ok(client, OP_DROP_VIEW, schema="main", name=view, if_exists=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_a_view_created_in_a_transaction_is_listed_only_there(target: Target) -> None:
    view = _view()
    async with connect(target) as (client, _hello):
        frames = await client.request(OP_TXN_BEGIN)
        txn = [h for h, _ in frames if h["t"] == FT_RESP_END][0]["txn"]
        await _ok(client, OP_CREATE_VIEW, txn=txn, **_create_args(view))
        assert await _listed(client, view, txn=txn)
        assert not await _listed(client, view)
        await client.request(OP_TXN_ROLLBACK, txn=txn)
        assert not await _listed(client, view)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.parametrize(
    "overrides",
    [{"plan": "not json"}, {"names": ["only_one"]}, {"name": ""}],
    ids=["bad-plan", "names-mismatch", "no-name"],
)
async def test_malformed_create_view_is_refused(target: Target, overrides: dict) -> None:
    view = _view()
    async with connect(target) as (client, _hello):
        err = await client.error(OP_CREATE_VIEW, **_create_args(view, **overrides))
        assert err["exception_type"] == "InvalidInputException"
        assert not await _listed(client, view)
