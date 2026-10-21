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
    substrait_field as _field,
    substrait_nullable as _nullable,
    substrait_plan as _plan,
    substrait_read,
)

from n6k_protocol.protocol import OP_READ_PLAN


def _read_users(fields: list[int]) -> dict:
    return substrait_read("main", "users", FIXTURE_TABLES["users"], fields)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_read_plan_projects_and_filters(target: Target) -> None:
    rel = {
        "filter": {
            "input": _read_users([0, 2]),
            "condition": {
                "scalarFunction": {
                    "functionReference": 1,
                    "outputType": _nullable("bool"),
                    "arguments": [{"value": _field(1)}, {"value": {"literal": {"i32": 28}}}],
                }
            },
        }
    }
    async with connect(target) as (client, _hello):
        rows = await client.table(OP_READ_PLAN, plan=_plan(rel, ["id", "age"], ["gt"]))
        assert rows.schema.names == ["id", "age"]
        assert sorted(rows.column("id").to_pylist()) == [1, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_read_plan_aggregates(target: Target) -> None:
    rel = {
        "aggregate": {
            "input": _read_users([2]),
            "groupings": [{"groupingExpressions": []}],
            "measures": [
                {
                    "measure": {
                        "functionReference": 1,
                        "outputType": _nullable("i64"),
                        "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
                        "invocation": "AGGREGATION_INVOCATION_ALL",
                        "arguments": [{"value": _field(0)}],
                    }
                }
            ],
        }
    }
    async with connect(target) as (client, _hello):
        rows = await client.table(OP_READ_PLAN, plan=_plan(rel, ["total"], ["sum"]))
        assert rows.column("total").to_pylist() == [90]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_read_plan_rejects_an_unknown_table(target: Target) -> None:
    rel = dict(_read_users([0]))
    rel["read"]["namedTable"] = {"names": ["main", "nope"]}
    async with connect(target) as (client, _hello):
        assert await client.error(OP_READ_PLAN, plan=_plan(rel, ["id"], []))
