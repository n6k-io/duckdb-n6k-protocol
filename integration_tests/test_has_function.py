"""OP_HAS_FUNCTION: whether a server has a function taking exactly these argument types.

Run:
    uv run pytest integration_tests/test_has_function.py -v
"""

import pytest

from _targets import TARGETS, Target, connect

from n6k_protocol.protocol import FT_RESP_END, OP_HAS_FUNCTION


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.parametrize(
    "name, args, exists",
    [
        ("upper", ["VARCHAR"], True),
        ("UPPER", ["VARCHAR"], True),
        ("upper", ["INTEGER"], False),
        ("sqrt", ["DOUBLE"], True),
        ("sqrt", ["INTEGER"], False),
        ("abs", ["DECIMAL(10,2)"], True),
        ("abs", ["VARCHAR"], False),
        ("sum", ["INTEGER"], True),
        ("count_star", [], True),
        ("concat", ["VARCHAR", "VARCHAR", "VARCHAR"], True),
        ("list_contains", ["INTEGER[]", "INTEGER"], True),
        ("struct_extract", ["STRUCT(a INTEGER, b VARCHAR)", "VARCHAR"], True),
        ("struct_extract", ["STRUCT(a INTEGER, b VARCHAR)"], False),
        ("struct_pack", ["INTEGER", "VARCHAR"], True),
        ("no_such_function", [], False),
        ("read_csv", ["VARCHAR"], False),
        ("upper", ["NOT_A_TYPE"], False),
    ],
)
async def test_answer(target: Target, name: str, args: list[str], exists: bool) -> None:
    async with connect(target) as (client, _hello):
        frames = await client.request(OP_HAS_FUNCTION, name=name, args=args)
        end = [h for h, _ in frames if h["t"] == FT_RESP_END][0]
        assert end["exists"] is exists


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_an_empty_name_is_refused(target: Target) -> None:
    async with connect(target) as (client, _hello):
        err = await client.error(OP_HAS_FUNCTION, name="", args=[])
        assert err["exception_type"] == "InvalidInputException"
