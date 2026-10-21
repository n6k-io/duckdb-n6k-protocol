"""ERR frames carry a real `exception_type`, from every server.

`exception_type` is a closed set of DuckDB class names; the client reconstructs the
matching exception from it and treats anything unrecognized as `IOException`. That
fallback is **silent**, so a server sending the wrong spelling does not fail — it just
strips the type off every error it ever reports.

That is precisely what the C++ server did: it sent `Exception::ExceptionTypeToString`,
which yields the display string ("Catalog") rather than the class name
("CatalogException"), so every typed error reached clients as an untyped IOException
while the tests that only ran against the Python mount stayed green. The fix put the
correspondence in src/common/n6k_exception_types.cpp, shared by both servers.

Run:
    uv run pytest integration_tests/test_error_typing_parity.py -v
"""

import pytest

from _targets import TARGETS, connect
from n6k_protocol.protocol import EXCEPTION_TYPES, OP_QUERY

# (sql, expected exception_type). Chosen so each lands in a different DuckDB
# ExceptionType, and so the display string differs from the wire value in every case.
CASES = [
    ("SELECT * FROM definitely_not_a_table", "CatalogException"),
    ("SELEC 1", "ParserException"),
    ("SELECT no_such_column FROM db.main.users", "BinderException"),
]


@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.parametrize("sql,expected", CASES, ids=[c[1] for c in CASES])
@pytest.mark.asyncio
async def test_error_type_is_the_wire_class_name(server, target, sql, expected) -> None:
    async with connect(target) as (client, _):
        err = await client.error(OP_QUERY, sql=sql)

    assert err["exception_type"] == expected, (
        f"{target} sent exception_type={err['exception_type']!r}; "
        f"the client only reconstructs class names, so this reaches it as IOException"
    )


@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.asyncio
async def test_error_type_is_always_in_the_closed_set(server, target) -> None:
    """Whatever a server reports, it has to be a value the client can place."""
    async with connect(target) as (client, _):
        for sql, _expected in CASES:
            err = await client.error(OP_QUERY, sql=sql)
            assert err["exception_type"] in EXCEPTION_TYPES, (
                f"{target} sent {err['exception_type']!r}, which is not a protocol "
                f"exception type; the client silently downgrades it to IOException"
            )


@pytest.mark.asyncio
async def test_all_servers_agree_on_error_type(server) -> None:
    """The same bad SQL must be typed identically everywhere."""
    for sql, expected in CASES:
        seen = {}
        for target in TARGETS:
            async with connect(target) as (client, _):
                seen[target.id] = (await client.error(OP_QUERY, sql=sql))["exception_type"]
        assert set(seen.values()) == {expected}, f"servers disagree on {sql!r}: {seen}"
