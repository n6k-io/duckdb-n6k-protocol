"""OP_READ_PLAN: a hand-written Substrait JSON plan executed over the served catalog.

Run:
    uv run pytest integration_tests/test_read_plan_wire.py -v
"""

import pytest

from _targets import (
    FIXTURE_TABLES,
    TARGETS,
    Target,
    connect,
    substrait_plan as _plan,
    substrait_read,
)

from n6k_protocol.protocol import OP_READ_PLAN


def _read_users(fields: list[int]) -> dict:
    return substrait_read("main", "users", FIXTURE_TABLES["users"], fields)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_read_plan_rejects_an_unknown_table(target: Target) -> None:
    rel = dict(_read_users([0]))
    rel["read"]["namedTable"] = {"names": ["main", "nope"]}
    async with connect(target) as (client, _hello):
        assert await client.error(OP_READ_PLAN, plan=_plan(rel, ["id"], []))
