"""One MCP session per connection, run by the official SDK (`pip install n6k-duckdb[mcp]`).

```python
from mcp.server.mcpserver import MCPServer

server = MCPServer("pages")

@server.tool()
def add_block(after: str, kind: str, content: str) -> str:
    page = n6k_server.context.current().params["page"]
    ...

register(app, "/page/{page}", connect=..., authenticate=..., mcp=server)
```
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import Optional, Union

try:
    import anyio
    import mcp_types as types
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
    from mcp.server.mcpserver import MCPServer
    from mcp.shared.message import SessionMessage
except ImportError as exc:  # pragma: no cover
    raise ImportError("n6k_server.mcp needs the MCP SDK: pip install 'n6k-duckdb[mcp]'") from exc

from n6k_server.jsonrpc import INVALID_REQUEST, SERVER_BUSY, _error
from n6k_server.pump import SendText, TextBacklogFull

logger = logging.getLogger("n6k_server.mcp")

_MAX_HELD = 1024


class McpSession:
    def __init__(self, server: MCPServer, max_inflight: Optional[int] = None) -> None:
        self._server = server
        self._max_inflight = max_inflight
        self._inflight: set[Union[int, str]] = set()
        self._inbound: Optional[MemoryObjectSendStream[SessionMessage | Exception]] = None
        self._tasks: list[asyncio.Task[None]] = []
        self._handshaking = False
        self._flushing = False
        self._held: list[str] = []

    def feed(self, text: str, send_text: SendText) -> str:
        """The reply to send now: a busy error past `max_inflight`, else ""."""
        # Synchronous: frames are fed in arrival order, and MCP needs that order.
        if self._inbound is None:
            self._start(send_text)
        assert self._inbound is not None
        try:
            message = types.jsonrpc_message_adapter.validate_json(text, by_name=False)
        except ValueError:
            # The SDK's dispatcher drops an invalid message unanswered.
            raw = json.loads(text)
            if "id" not in raw:
                return ""
            return json.dumps(_error(raw["id"], INVALID_REQUEST, "invalid request"))
        if isinstance(message, types.JSONRPCRequest):
            if self._max_inflight is not None and len(self._inflight) >= self._max_inflight:
                return json.dumps(_error(message.id, SERVER_BUSY, "server busy"))
            self._inflight.add(message.id)
        elif isinstance(message, types.JSONRPCNotification) and message.method == "notifications/cancelled":
            # A cancelled request gets no response, so its slot is freed here.
            self._inflight.discard((message.params or {}).get("requestId"))
        method = getattr(message, "method", None)
        if method == "initialize":
            self._handshaking = True
        self._inbound.send_nowait(SessionMessage(message))
        if method == "notifications/initialized" and self._handshaking and not self._flushing:
            if self._held:
                self._flushing = True
                self._tasks.append(asyncio.create_task(self._flush(send_text)))
            else:
                self._handshaking = False
        return ""

    async def _flush(self, send_text: SendText) -> None:
        # Stays "handshaking" until drained, so a notify issued meanwhile queues behind the held ones.
        try:
            while self._held:
                await send_text(self._held.pop(0))
        finally:
            self._handshaking = False
            self._flushing = False

    async def notify(self, text: str, send_text: SendText) -> None:
        if self._handshaking:
            if len(self._held) >= _MAX_HELD:
                raise TextBacklogFull(f"more than {_MAX_HELD} notifications held for the mcp handshake")
            self._held.append(text)
        else:
            await send_text(text)

    def _start(self, send_text: SendText) -> None:
        inbound, server_read = anyio.create_memory_object_stream[SessionMessage | Exception](math.inf)
        server_write, outbound = anyio.create_memory_object_stream[SessionMessage](math.inf)
        self._inbound = inbound
        low = self._server._lowlevel_server
        run = low.run(server_read, server_write, low.create_initialization_options())
        self._tasks = [asyncio.create_task(run), asyncio.create_task(self._forward(outbound, send_text))]
        for task in self._tasks:
            task.add_done_callback(_log_failure)

    async def _forward(self, outbound: MemoryObjectReceiveStream[SessionMessage], send_text: SendText) -> None:
        async with outbound:
            async for session_message in outbound:
                message = session_message.message
                if isinstance(message, (types.JSONRPCResponse, types.JSONRPCError)) and message.id is not None:
                    self._inflight.discard(message.id)
                await send_text(message.model_dump_json(by_alias=True, exclude_unset=True))

    async def aclose(self) -> None:
        if self._inbound is not None:
            await self._inbound.aclose()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)


def _log_failure(task: "asyncio.Task[None]") -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.error("mcp session task failed", exc_info=task.exception())
