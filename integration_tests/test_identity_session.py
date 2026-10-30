"""The host builds each socket's session for the identity its FT_HELLO token names.

`/ident` maps `tok-a` to catalog `a` and `tok-b` to `b`; anything else is refused.

Run:
    uv run pytest integration_tests/test_identity_session.py -v
"""

import asyncio
import json
import urllib.parse
import urllib.request
from typing import Any

import pytest
import websockets

from _targets import (
    SERVER_URL,
    Target,
    WireClient,
    hello,
    open_session,
    server_is_up,
    substrait_plan,
    substrait_read,
)

from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_RESP_END,
    OP_QUERY,
    OP_READ_PLAN,
    OP_TXN_BEGIN,
)

pytestmark = pytest.mark.skipif(not server_is_up(), reason="test server not running on :8099")

IDENT = Target(id="ident", mount="/ident")
SESSION_ID = "'00000000-0000-4000-8000-000000000000'::UUID"


def _debug(path: str, params: dict[str, str] | None = None, method: str = "GET") -> dict[str, Any]:
    url = f"{SERVER_URL}/debug/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=5) as response:
        return dict(json.loads(response.read()))


@pytest.mark.asyncio
@pytest.mark.parametrize("token,mine,other", [("tok-a", "a", "b"), ("tok-b", "b", "a")])
async def test_an_identity_can_open_only_its_own_catalog(token: str, mine: str, other: str) -> None:
    async with websockets.connect(IDENT.ws_url(catalog=None)) as ws:
        assert (await hello(ws, token))["t"] == FT_HELLO_ACK
        assert (await open_session(ws, 1, mine))["t"] == FT_OPEN_ACK
        assert (await open_session(ws, 2, other))["t"] == FT_OPEN_ERR
        assert (await WireClient(ws).scan("main", "users")).num_rows == 3


@pytest.mark.asyncio
async def test_an_unknown_identity_builds_nothing() -> None:
    before = (await asyncio.to_thread(_debug, "bridge_builds"))["builds"]
    async with websockets.connect(IDENT.ws_url(catalog=None)) as ws:
        err = await hello(ws, "tok-nobody")
        assert err["t"] == FT_HELLO_ERR
        assert "unknown identity" in err["exception_message"]
    after = (await asyncio.to_thread(_debug, "bridge_builds"))["builds"]
    assert after == before


@pytest.mark.asyncio
async def test_the_startup_memory_catalog_is_not_served() -> None:
    async with websockets.connect(IDENT.ws_url(catalog=None)) as ws:
        assert (await hello(ws, "tok-a"))["t"] == FT_HELLO_ACK
        assert (await open_session(ws, 1, "memory"))["t"] == FT_OPEN_ERR


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        f"SELECT * FROM n6k_serve_send_text({SESSION_ID}, 'x')",
        f"SELECT * FROM n6k_serve_close({SESSION_ID}, 1000, 'x')",
        f"CALL n6k_serve_ws({SESSION_ID}, '{{}}')",
        f"CALL n6k_serve_fd({SESSION_ID})",
        "CALL n6k_serve_socket()",
        "CALL n6k_serve_http('127.0.0.1', 1)",
        "SELECT * FROM n6k_connect(wsFd => 1000000)",
        "SELECT * FROM n6k_list_catalogs(wsFd => 1000000)",
        "CALL n6k_serve_push_invalidate('a', 'main')",
        "SELECT * FROM n6k_serve_stats()",
    ],
)
async def test_a_served_client_cannot_reach_the_serve_functions(sql: str) -> None:
    async with websockets.connect(IDENT.ws_url(catalog=None)) as ws:
        assert (await hello(ws, "tok-a"))["t"] == FT_HELLO_ACK
        assert (await open_session(ws, 1, "a"))["t"] == FT_OPEN_ACK
        client = WireClient(ws)
        frames = await client.request(OP_QUERY, sql=sql)
        errs = [h for h, _ in frames if h["t"] == FT_ERR]
        assert errs, frames
        assert "not callable from a served connection" in errs[0]["exception_message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("pinned", [False, True])
async def test_a_plan_cannot_reach_the_serve_functions(pinned: bool) -> None:
    async with websockets.connect(IDENT.ws_url(catalog=None)) as ws:
        assert (await hello(ws, "tok-a"))["t"] == FT_HELLO_ACK
        assert (await open_session(ws, 1, "a"))["t"] == FT_OPEN_ACK
        sql = f"CREATE OR REPLACE VIEW a.main.send_text_view AS SELECT * FROM n6k_serve_send_text({SESSION_ID}, 'x')"
        assert (await asyncio.to_thread(_debug, "server_exec", {"catalog": "a", "sql": sql}, "POST"))["handlers"] >= 1
        client = WireClient(ws)
        args: dict[str, Any] = {}
        if pinned:
            begin = await client.request(OP_TXN_BEGIN)
            assert begin[-1][0]["t"] == FT_RESP_END, begin
            args["txn"] = begin[-1][0]["txn"]
        plan = substrait_plan(substrait_read("main", "send_text_view", [("sent", "bool")]), ["sent"])
        err = await client.error(OP_READ_PLAN, plan=plan, **args)
        assert "not callable from a served connection" in err["exception_message"]


@pytest.mark.asyncio
async def test_a_rejected_socket_is_invisible_to_server_exec() -> None:
    catalog = "a"
    before = (await asyncio.to_thread(_debug, "server_exec", {"catalog": catalog, "sql": "SELECT 1"}, "POST"))[
        "handlers"
    ]
    async with websockets.connect(IDENT.ws_url(catalog=None)) as ws:
        assert (await hello(ws, "tok-nobody"))["t"] == FT_HELLO_ERR
        during = (await asyncio.to_thread(_debug, "server_exec", {"catalog": catalog, "sql": "SELECT 1"}, "POST"))[
            "handlers"
        ]
    assert during == before
