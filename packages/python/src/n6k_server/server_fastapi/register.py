"""Mount the n6k WebSocket endpoint on a FastAPI app.

    register(app, "/db/{tenant}", connect=open_db, authenticate=authorize)

Mounts `<prefix>/ws`. Per WebSocket:

1. `connect(ws, **path_params)` returns an empty DuckDB connection with the n6k
   extensions loaded. It may be sync, async, or an async generator (yield the
   connection; code after the `yield` runs once the socket drops).
2. The extension reads the client's FT_HELLO and hands its token to
   `authenticate(ws, con, token, **path_params)`, which verifies it, attaches onto
   `con` what that identity may see, and returns the catalogs to serve. It may be
   sync or async, and may take as long as it needs: the client gets FT_HELLO_PENDING
   (carrying `hello_pending_message`) until it returns. An entry may be a
   `PendingCatalog` instead of a name: its `attach` runs while serving, and clients
   opening it wait (FT_OPEN_PENDING) until it returns.
3. `n6k_serve_fd` serves exactly those catalogs. If given, `on_serve(ws, handle, **path_params)`
   then gets a `LiveServe` that adds and removes catalogs while the socket is served.
4. If given, `resolve(handle, name)` runs when a client opens a catalog that is not served, while that
   client waits (FT_OPEN_PENDING); `handle` is the connection's `LiveServe`. The open gets OPEN_ACK if
   `resolve` served the catalog by the time it returns (e.g. ATTACH plus `handle.add_catalog`), or is
   forwarded to the upstream serving it, else OPEN_ERR. Concurrent opens of one name share one call;
   every OPEN of a name not served calls `resolve` again.
   `authenticate` (or `handle.add_catalog`) may list a `LazyCatalog(name)`: listed to clients at
   once, served only once `resolve` serves it.
5. Binary frames are n6k. Text frames, once HELLO_ACK is sent, go to
   `on_text(ws, text, send_text, **path_params)` if given, each on its own task; `send_text`
   replies, and raises if the client's FT_HELLO offered neither `jsonrpc` nor `mcp`, or once the
   connection has ended.
6. Instead of `on_text`, `jsonrpc=` (an `n6k_server.jsonrpc.JsonRpc`) and/or `mcp=` (an MCP SDK `MCPServer`)
   serve JSON-RPC methods and MCP tools on the text frames. HELLO_ACK grants the client each of
   `jsonrpc` / `mcp` that it offered and the app serves; `mcp=` serves both. `n6k_server.context.current()` inside a method or
   tool is the connection's path params and the identity `authenticate` returned in `Authorized`.

`authenticate` refuses by raising `WsReject(code, reason)`: the reason reaches the
client as FT_HELLO_ERR and nothing is served. Any other exception refuses the same way, with
reason "internal error" and close code 1011. An empty list logs the client in with no catalogs
(text only, or catalogs added later through `on_serve` or `resolve`). `connect` may also raise
`WsReject`, before any frame is read.
"""

import inspect
import logging
import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Optional, Union

import duckdb
from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect

from n6k_server.pump import Authenticate, OnServe, OnText, Resolve, TextApp, WsReject, serve_and_close_connection
from n6k_server.jsonrpc import JsonRpc, JsonRpcApp

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

logger = logging.getLogger("n6k_server.server_fastapi.register")

Connect = Callable[
    ...,
    Union[
        duckdb.DuckDBPyConnection,
        Awaitable[duckdb.DuckDBPyConnection],
        AsyncIterator[duckdb.DuckDBPyConnection],
    ],
]


def register(
    app: FastAPI,
    prefix: str,
    *,
    connect: Connect,
    authenticate: Authenticate,
    ping_interval_ms: Optional[int] = None,
    hello_pending_message: str = "authenticating",
    on_serve: Optional[OnServe] = None,
    resolve: Optional[Resolve] = None,
    on_text: Optional[OnText] = None,
    jsonrpc: Optional[JsonRpc] = None,
    mcp: Optional["MCPServer"] = None,
    max_text_inflight: Optional[int] = None,
    max_mcp_inflight: Optional[int] = 8,
) -> APIRouter:
    """Mount `<prefix>/ws` on `app` and return the router.

    `prefix` follows FastAPI's convention: empty for root, else starts with `/` and
    does not end with `/`. Path params it captures are passed to `connect` and
    `authenticate` as keyword arguments. `ping_interval_ms`: None leaves the keepalive to
    `$N6K_PING_INTERVAL` (seconds), 0 disables it. `max_text_inflight`: None sets no limit on text
    handlers running at once. `max_mcp_inflight`: MCP requests running at once per connection; past
    it a request is answered "server busy". None sets no limit.
    """
    if ping_interval_ms is not None and ping_interval_ms < 0:
        raise ValueError(f"ping_interval_ms must be None, 0 (disabled) or positive, got {ping_interval_ms}")
    if prefix and not prefix.startswith("/"):
        raise ValueError(f"prefix must be empty or start with '/', got {prefix!r}")
    if prefix.endswith("/"):
        raise ValueError(f"prefix must not end with '/' (FastAPI convention), got {prefix!r}")
    if max_text_inflight is not None and max_text_inflight <= 0:
        raise ValueError(f"max_text_inflight must be None (no limit) or positive, got {max_text_inflight}")
    _text_app(on_text, jsonrpc, mcp, max_mcp_inflight)

    router = APIRouter(prefix=prefix)

    @router.websocket("/ws")
    async def _ws(ws: WebSocket) -> None:
        await ws.accept()
        started = time.perf_counter()
        params = {str(k): str(v) for k, v in ws.path_params.items()}
        logger.info("ws accept path=%s params=%s", ws.url.path, params)
        try:
            await serve_ws(
                ws,
                connect,
                authenticate,
                params,
                ping_interval_ms=ping_interval_ms,
                hello_pending_message=hello_pending_message,
                on_serve=on_serve,
                resolve=resolve,
                on_text=on_text,
                jsonrpc=jsonrpc,
                mcp=mcp,
                max_text_inflight=max_text_inflight,
                max_mcp_inflight=max_mcp_inflight,
            )
        except WsReject as reject:
            await _close(ws, reject.code, reject.reason)
        except WebSocketDisconnect:
            pass
        except Exception as exc:  # noqa: BLE001 - surface the reason rather than hang the client
            logger.exception("ws failed path=%s", ws.url.path)
            await _close(ws, 1011, str(exc))
        finally:
            logger.info("ws close path=%s duration_ms=%.1f", ws.url.path, (time.perf_counter() - started) * 1000)

    app.include_router(router)
    return router


async def serve_ws(
    ws: WebSocket,
    connect: Connect,
    authenticate: Authenticate,
    params: dict[str, str],
    *,
    ping_interval_ms: Optional[int] = None,
    hello_pending_message: str = "authenticating",
    on_serve: Optional[OnServe] = None,
    resolve: Optional[Resolve] = None,
    on_text: Optional[OnText] = None,
    jsonrpc: Optional[JsonRpc] = None,
    mcp: Optional["MCPServer"] = None,
    max_text_inflight: Optional[int] = None,
    max_mcp_inflight: Optional[int] = 8,
) -> None:
    """Open the connection `connect` describes, authenticate and serve it, and let it clean up."""
    text_app = _text_app(on_text, jsonrpc, mcp, max_mcp_inflight)
    opened: Any = connect(ws, **params)

    if inspect.isasyncgen(opened):
        con = await opened.__anext__()
        try:
            await serve_and_close_connection(
                ws,
                con,
                authenticate,
                params,
                ping_interval_ms=ping_interval_ms,
                hello_pending_message=hello_pending_message,
                on_serve=on_serve,
                resolve=resolve,
                on_text=on_text,
                text_app=text_app,
                max_text_inflight=max_text_inflight,
            )
        finally:
            # Resumes the generator past its `yield` so its `finally` runs. It must not
            # yield again (`aclose` raises RuntimeError) — a second connection has nothing to
            # be served on.
            await opened.aclose()
        return

    con = await opened if inspect.isawaitable(opened) else opened
    await serve_and_close_connection(
        ws,
        con,
        authenticate,
        params,
        ping_interval_ms=ping_interval_ms,
        hello_pending_message=hello_pending_message,
        on_serve=on_serve,
        resolve=resolve,
        on_text=on_text,
        text_app=text_app,
        max_text_inflight=max_text_inflight,
    )


def _text_app(
    on_text: Optional[OnText],
    jsonrpc: Optional[JsonRpc],
    mcp: Optional["MCPServer"],
    max_mcp_inflight: Optional[int],
) -> Optional[TextApp]:
    if max_mcp_inflight is not None and max_mcp_inflight <= 0:
        raise ValueError(f"max_mcp_inflight must be None (no limit) or positive, got {max_mcp_inflight}")
    if jsonrpc is None and mcp is None:
        return None
    if on_text is not None:
        raise ValueError("pass on_text or jsonrpc/mcp, not both")
    return JsonRpcApp(jsonrpc, mcp, max_mcp_inflight)


async def _close(ws: WebSocket, code: int, reason: str) -> None:
    """Close with a reason, truncated to the 123 bytes RFC 6455 allows."""
    try:
        await ws.close(code=code, reason=reason[:123])
    except RuntimeError:
        pass  # already closed
