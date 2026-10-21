"""`JsonRpc` dispatch and `JsonRpcApp` routing, without a socket."""

import asyncio
import json
from typing import Any

import pytest

from n6k_server.jsonrpc import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    JsonRpc,
    JsonRpcApp,
    JsonRpcError,
    _JsonRpcSession,
)


def _api() -> JsonRpc:
    api = JsonRpc()

    @api.method()
    def add(a: int, b: int) -> int:
        return a + b

    @api.method("slow_add")
    async def _slow_add(a: int, b: int) -> int:
        await asyncio.sleep(0)
        return a + b

    @api.method()
    def refuse() -> None:
        raise JsonRpcError(42, "no", {"why": "test"})

    @api.method()
    def crash() -> None:
        raise ValueError("boom")

    return api


def _call(api: JsonRpc, message: dict[str, Any]) -> Any:
    return asyncio.run(api.call(message))


def test_by_name_and_positional_params():
    api = _api()
    assert _call(api, {"jsonrpc": "2.0", "id": 1, "method": "add", "params": {"a": 1, "b": 2}}) == {
        "jsonrpc": "2.0",
        "id": 1,
        "result": 3,
    }
    assert _call(api, {"jsonrpc": "2.0", "id": 2, "method": "slow_add", "params": [2, 3]})["result"] == 5


def test_errors():
    api = _api()

    def code(message: dict[str, Any]) -> int:
        return int(_call(api, {"jsonrpc": "2.0", "id": 1, **message})["error"]["code"])

    assert code({"method": "missing"}) == METHOD_NOT_FOUND
    assert code({"method": "add", "params": {"a": 1}}) == INVALID_PARAMS
    assert code({"method": 7}) == INVALID_REQUEST
    assert code({"method": "crash"}) == INTERNAL_ERROR
    assert _call(api, {"jsonrpc": "2.0", "id": 1, "method": "refuse"})["error"] == {
        "code": 42,
        "message": "no",
        "data": {"why": "test"},
    }


def test_a_notification_gets_no_response():
    calls: list[int] = []
    api = JsonRpc()

    @api.method()
    def note(n: int) -> None:
        calls.append(n)

    assert _call(api, {"jsonrpc": "2.0", "method": "note", "params": [1]}) is None
    assert _call(api, {"jsonrpc": "2.0", "method": "missing"}) is None
    assert calls == [1]


def test_duplicate_names_are_refused():
    api = JsonRpc()
    api.method("x")(lambda: None)
    with pytest.raises(ValueError, match="already registered"):
        api.method("x")(lambda: None)


class _FakeMcp:
    def __init__(self) -> None:
        self.seen: list[str] = []

    def feed(self, text: str, send_text: Any) -> str:
        self.seen.append(json.loads(text).get("method", "<response>"))
        return ""

    async def notify(self, text: str, send_text: Any) -> None:
        await send_text(text)

    async def aclose(self) -> None:
        pass


def test_registered_methods_go_to_jsonrpc_and_everything_else_to_mcp():
    sent: list[str] = []
    mcp = _FakeMcp()

    async def send_text(reply: str) -> None:
        sent.append(reply)

    async def run() -> None:
        session = _JsonRpcSession(_api(), mcp)
        for message in (
            {"jsonrpc": "2.0", "id": 1, "method": "add", "params": [1, 2]},
            {"jsonrpc": "2.0", "id": 2, "method": "subscriptions/listen"},
            {"jsonrpc": "2.0", "id": 3, "method": "made/up"},
            {"jsonrpc": "2.0", "id": 4, "result": {}},
        ):
            await session.handle(json.dumps(message), send_text)

    asyncio.run(run())
    assert [json.loads(s)["result"] for s in sent] == [3]
    assert mcp.seen == ["subscriptions/listen", "made/up", "<response>"]


def test_an_unhashable_method_goes_to_mcp_instead_of_raising():
    mcp = _FakeMcp()

    async def send_text(reply: str) -> None:
        pass

    async def run() -> None:
        session = _JsonRpcSession(_api(), mcp)
        text = json.dumps({"jsonrpc": "2.0", "id": 1, "method": [1]})
        await session.handle(text, send_text)
        assert session.handle_busy(text, send_text) == ""

    asyncio.run(run())
    assert len(mcp.seen) == 2


def _handle(app: JsonRpcApp, text: str) -> list[Any]:
    sent: list[str] = []

    async def send_text(reply: str) -> None:
        sent.append(reply)

    async def run() -> None:
        session = app.open()
        await session.handle(text, send_text)
        await session.aclose()

    asyncio.run(run())
    return [json.loads(s) for s in sent]


def test_the_app_answers_parse_errors_and_batches():
    app = JsonRpcApp(jsonrpc=_api())
    assert _handle(app, "{not json")[0]["error"]["code"] == PARSE_ERROR
    assert _handle(app, "[]")[0]["error"]["code"] == INVALID_REQUEST
    assert _handle(app, json.dumps({"jsonrpc": "2.0", "id": 9, "method": "add", "params": [1, 1]})) == [
        {"jsonrpc": "2.0", "id": 9, "result": 2}
    ]


def test_capabilities():
    assert list(JsonRpcApp(jsonrpc=JsonRpc()).capabilities) == ["jsonrpc"]
    with pytest.raises(ValueError):
        JsonRpcApp()
