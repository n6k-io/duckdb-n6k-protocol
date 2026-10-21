"""JSON-RPC 2.0 and MCP over the n6k socket's text frames.

    api = JsonRpc()

    @api.method()
    def add_block(after: str, kind: str) -> str: ...

    register(app, "/db/{page}", connect=..., authenticate=..., jsonrpc=api, mcp=mcp_server)

A text frame is one JSON-RPC message. A message whose method is registered on `jsonrpc` goes there;
everything else, responses included, goes to `mcp` when given. Batches are not supported.
"""

from __future__ import annotations

import inspect
import json
import logging
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, Sequence, TypeVar

from n6k_protocol.protocol import CAP_JSONRPC, CAP_MCP
from n6k_server.pump import SendText, TextSession

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from n6k_server.mcp import McpSession

logger = logging.getLogger("n6k_server.jsonrpc")

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
SERVER_BUSY = -32000

_F = TypeVar("_F", bound=Callable[..., Any])


class JsonRpcError(Exception):
    """Raise from a method to answer with this JSON-RPC error instead of INTERNAL_ERROR."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


def _error(req_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": error}


def _dumps(response: dict[str, Any]) -> str:
    try:
        return json.dumps(response)
    except (TypeError, ValueError):
        logger.exception("jsonrpc result for id %r is not JSON-serializable", response.get("id"))
        return json.dumps(_error(response.get("id"), INTERNAL_ERROR, "result is not JSON-serializable"))


class JsonRpc:
    """Methods served by `register(..., jsonrpc=)`, added with `@api.method()` or `@api.method("name")`.

    Methods may be sync or async and take params by name or position. A `JsonRpcError` answers with
    that error; any other exception, or a result `json.dumps` cannot encode, answers INTERNAL_ERROR
    and is logged. Unknown methods answer METHOD_NOT_FOUND, params that do not bind INVALID_PARAMS,
    invalid JSON PARSE_ERROR, a malformed request or a batch INVALID_REQUEST. Notifications get no
    reply.
    """

    def __init__(self) -> None:
        self._methods: dict[str, Callable[..., Any]] = {}

    def method(self, name: Optional[str] = None) -> Callable[[_F], _F]:
        def decorate(fn: _F) -> _F:
            method = name or fn.__name__
            if method in self._methods:
                raise ValueError(f"method {method!r} is already registered")
            self._methods[method] = fn
            return fn

        return decorate

    def __contains__(self, method: object) -> bool:
        return isinstance(method, str) and method in self._methods

    async def call(self, message: Mapping[str, Any]) -> Optional[dict[str, Any]]:
        req_id = message.get("id")
        notification = "id" not in message
        method = message.get("method")
        params = message.get("params", {})
        if message.get("jsonrpc") != "2.0" or not isinstance(method, str) or not isinstance(params, (dict, list)):
            return None if notification else _error(req_id, INVALID_REQUEST, "invalid request")
        fn = self._methods.get(method)
        if fn is None:
            return None if notification else _error(req_id, METHOD_NOT_FOUND, f"method not found: {method}")
        args, kwargs = (params, {}) if isinstance(params, list) else ([], params)
        try:
            inspect.signature(fn).bind(*args, **kwargs)
        except TypeError as exc:
            return None if notification else _error(req_id, INVALID_PARAMS, str(exc))
        try:
            result = fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except JsonRpcError as exc:
            return None if notification else _error(req_id, exc.code, exc.message, exc.data)
        except Exception:  # noqa: BLE001
            logger.exception("jsonrpc method %r failed", method)
            return None if notification else _error(req_id, INTERNAL_ERROR, "internal error")
        return None if notification else {"jsonrpc": "2.0", "id": req_id, "result": result}


class JsonRpcApp:
    def __init__(
        self,
        jsonrpc: Optional[JsonRpc] = None,
        mcp: Optional["MCPServer"] = None,
        max_mcp_inflight: Optional[int] = None,
    ) -> None:
        if jsonrpc is None and mcp is None:
            raise ValueError("pass jsonrpc, mcp, or both")
        self._rpc = jsonrpc
        self._mcp = mcp
        self._max_mcp_inflight = max_mcp_inflight

    @property
    def capabilities(self) -> Sequence[str]:
        return (CAP_JSONRPC, CAP_MCP) if self._mcp is not None else (CAP_JSONRPC,)

    def open(self) -> TextSession:
        mcp_session: Optional[McpSession] = None
        if self._mcp is not None:
            from n6k_server.mcp import McpSession

            mcp_session = McpSession(self._mcp, self._max_mcp_inflight)
        return _JsonRpcSession(self._rpc, mcp_session)


class _JsonRpcSession:
    def __init__(self, rpc: Optional[JsonRpc], mcp: Optional["McpSession"]) -> None:
        self._rpc = rpc
        self._mcp = mcp

    def _parse(self, text: str, send_text: SendText) -> dict[str, Any] | str:
        try:
            message = json.loads(text)
        except ValueError:
            return json.dumps(_error(None, PARSE_ERROR, "parse error"))
        if not isinstance(message, dict):
            return json.dumps(_error(None, INVALID_REQUEST, "batches are not supported"))
        if self._mcp is not None and (self._rpc is None or message.get("method") not in self._rpc):
            return self._mcp.feed(text, send_text)
        return message

    async def handle(self, text: str, send_text: SendText) -> None:
        message = self._parse(text, send_text)
        if isinstance(message, str):
            if message:
                await send_text(message)
            return
        if self._rpc is not None:
            response = await self._rpc.call(message)
        elif "id" in message:
            response = _error(message.get("id"), METHOD_NOT_FOUND, f"method not found: {message.get('method')}")
        else:
            response = None
        if response is not None:
            await send_text(_dumps(response))

    def handle_busy(self, text: str, send_text: SendText) -> Optional[str]:
        message = self._parse(text, send_text)
        if isinstance(message, str):
            return message
        if "id" not in message:
            return ""
        return json.dumps(_error(message.get("id"), SERVER_BUSY, "server busy"))

    async def notify(self, text: str, send_text: SendText) -> None:
        if self._mcp is not None:
            await self._mcp.notify(text, send_text)
        else:
            await send_text(text)

    async def aclose(self) -> None:
        if self._mcp is not None:
            await self._mcp.aclose()
