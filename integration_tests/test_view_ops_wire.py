"""OP_CREATE_VIEW refuses a malformed request.

Run:
    uv run pytest integration_tests/test_view_ops_wire.py -v
"""

import uuid

import pytest

from _targets import TARGETS, Target, connect, scan_plan

from n6k_protocol.protocol import (
    FT_RESP_CHUNK,
    OP_CREATE_VIEW,
    OP_TABLES_LIST,
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


async def _listed(client, view: str, **args) -> bool:
    frames = await client.request(OP_TABLES_LIST, schema="main", **args)
    chunk = [h for h, _ in frames if h["t"] == FT_RESP_CHUNK][0]
    return any(t["name"] == view for t in chunk["tables"])


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
