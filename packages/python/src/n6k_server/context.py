"""The connection a handler is serving: an RPC method, an MCP tool, `on_text`, or `resolve`."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Optional


@dataclass(frozen=True)
class Connection:
    """`params` are the route's path params; `identity` is what `authenticate` returned in
    `Authorized(..., identity=...)` (None for a plain catalog list)."""

    params: Mapping[str, str] = field(default_factory=dict)
    identity: Any = None
    _notify: Optional[Callable[[str, Any], Awaitable[bool]]] = field(default=None, repr=False, compare=False)

    async def notify(self, method: str, params: Any = None) -> bool:
        """Send the client a JSON-RPC notification. Keep the `Connection` to push after the handler
        returns. False, with nothing sent, when the client's FT_HELLO offered neither jsonrpc nor mcp,
        the route has no text handler, or the connection has ended. To an MCP client mid-handshake it is
        held and sent once the handshake ends, and dropped if the connection ends first; True either way,
        unless more than `mcp._MAX_HELD` are held: then the socket closes (1008) and it returns False.
        A failed WebSocket send raises.
        """
        if self._notify is None:
            return False
        return await self._notify(method, params)


_CURRENT: ContextVar[Connection] = ContextVar("n6k_connection")


def current() -> Connection:
    """Raises `RuntimeError` outside a connection's handler."""
    try:
        return _CURRENT.get()
    except LookupError:
        raise RuntimeError("not running inside an n6k connection's handler") from None
