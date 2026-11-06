"""Serve a WebSocket by handing it to the C++ reactor and shuttling bytes.

This module is the whole of Python's involvement in a served connection. It never decodes an
n6k frame, never inspects an op, and cannot tell a SCAN from a PING: the protocol, the handshake
included, lives in `src/n6k_server`. It reads only the stream framing envelope, the extension's
CLOSE frame, and text frames, which belong to the app.

The mechanism is a `socketpair()`. Python keeps one end and passes the *number*
of the other to `n6k_serve_create_session(<fd>)`, then serves the returned id with
`CALL n6k_serve_fd(<id>, ...)`, which `dup()`s the fd and then reads and writes it like any socket.

The host's hooks reach the extension as scalar UDFs it calls back (see `_Callbacks`). Text the app
sends goes back through the extension (`n6k_serve_send_text`), so it is ordered after HELLO_ACK and
with the binary frames.

In-process rather than a spawned `duckdb`, because the reactor's workers open
connections on *this* `DatabaseInstance`. That is what lets a table backed by
`virtual_catalog.provider` — whose rows come from Python callbacks marshalled to this
event loop — be served at all. A subprocess would have its own database and no
interpreter to call back into. `duckdb` releases the GIL for the whole of
`execute()`, so the blocking `CALL` parks a thread without starving the loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import socket
import threading
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)

import duckdb
import msgpack
from duckdb.func import FunctionNullHandling

from n6k_protocol.protocol import APP_HELLO_CAPS, STREAM_FRAME_CLOSE, STREAM_FRAME_JSONRPC, STREAM_FRAME_N6K
from n6k_server.context import _CURRENT, Connection
from n6k_server.stream import FrameTooLarge, read_frame, write_frame

logger = logging.getLogger("n6k_server.pump")

# Bounds a corrupt length prefix from the extension; far above any real frame.
MAX_FRAME_BYTES = 1 << 30
# How long to wait for the serving thread to unwind after the socket drops.
SERVE_JOIN_TIMEOUT_S = 5.0
DRAIN_TIMEOUT_S = 5.0
# `LiveServe.paused`: how long cancelled requests get to unwind, and how often the drain is polled.
PAUSE_CANCEL_TIMEOUT_S = 5.0
PAUSE_POLL_S = 0.02

_T = TypeVar("_T")

_UUID = duckdb.sqltype("UUID")
_VARCHAR = duckdb.sqltype("VARCHAR")
_VARCHAR_LIST = duckdb.sqltype("VARCHAR[]")


class WsReject(Exception):
    """Raise from `connect` or `authenticate` to refuse a connection.

    From `authenticate` the reason reaches the client as FT_HELLO_ERR and the code
    closes the WebSocket. From `connect` (before any frame) the code and reason close it.
    """

    def __init__(self, code: int = 4400, reason: str = "") -> None:
        super().__init__(reason or f"ws reject {code}")
        self.code = code
        self.reason = reason


class WebSocketLike(Protocol):
    """The transport surface the pump needs, so this module imports no framework."""

    async def receive(self) -> Mapping[str, Any]: ...

    async def send_bytes(self, data: bytes) -> None: ...

    async def send_text(self, data: str) -> None: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class UpstreamSocket(Protocol):
    """An open client websocket to another n6k server, in the `websockets` client shape: `recv()` gives
    bytes for a binary message and str for a text one, and raises once the socket has closed."""

    async def recv(self) -> Union[bytes, str]: ...

    async def send(self, message: bytes) -> None: ...

    async def close(self) -> None: ...


class UpstreamError(Exception):
    """`connect_upstream` could not finish the upstream's HELLO or CATALOG_LIST."""


OnUpstreamText = Callable[[str], Optional[Awaitable[None]]]


class Upstream:
    """One connection to another n6k server, from `LiveServe.connect_upstream`. `catalogs` is what it
    served when the HELLO finished; `add_upstream` serves them on this connection.

    Its binary frames are n6k and pass between the extension and the websocket unmodified. Its text
    frames go to `on_text`, never to the client. `wait_closed()` returns why it ended: the extension's
    reason when the extension ended it (ping timeout, `remove_upstream`, ...) while it was added and
    this `LiveServe` was open, else the websocket's or a generic one.
    """

    def __init__(self, ws: UpstreamSocket, live: "LiveServe", on_text: Optional[OnUpstreamText]) -> None:
        self.catalogs: list[str] = []
        self.capabilities: list[str] = []
        self.reason: Optional[str] = None
        self._ws = ws
        self._live = live
        self._on_text = on_text
        self._host_sock, self._ext_sock = socket.socketpair()
        self._writer: Optional[asyncio.StreamWriter] = None
        self._pump: Optional[asyncio.Task[None]] = None
        self._ended = asyncio.Event()
        self._added = False
        self._hello_running = False

    @property
    def fd(self) -> int:
        return self._ext_sock.fileno()

    @property
    def closed(self) -> bool:
        return self._ended.is_set()

    async def wait_closed(self) -> str:
        await self._ended.wait()
        assert self.reason is not None
        return self.reason

    async def aclose(self) -> None:
        """Close the upstream and wait for its pump to end; a served upstream's sessions fail as
        closed."""
        if self._writer is not None and not self._writer.is_closing():
            self._writer.close()
        if self._pump is None:
            self._host_sock.close()
            self._release()
            return
        await asyncio.gather(self._pump, return_exceptions=True)

    async def _start(self) -> None:
        reader, writer = await asyncio.open_connection(sock=self._host_sock)
        self._writer = writer
        self._pump = asyncio.create_task(self._run(reader, writer))

    async def _run(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        from_ws = asyncio.create_task(self._ws_to_sock(writer))
        from_ext = asyncio.create_task(self._sock_to_ws(reader))
        try:
            done, _ = await asyncio.wait({from_ws, from_ext}, return_when=asyncio.FIRST_COMPLETED)
            first = from_ext if from_ext in done else from_ws
            self.reason = first.result()
            if first is from_ext and self._added and not self._live.closed:
                await self._read_extension_reason()
        except BaseException as exc:
            self.reason = self.reason or f"upstream pump failed: {exc or type(exc).__name__}"
            raise
        finally:
            for task in (from_ws, from_ext):
                task.cancel()
            await asyncio.gather(from_ws, from_ext, return_exceptions=True)
            if not writer.is_closing():
                writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ended.set()
            self._release()

    async def _read_extension_reason(self) -> None:
        try:
            reason = await self._live._upstream_closed_reason(self.fd)
        except Exception:  # noqa: BLE001
            logger.debug("reading the upstream's close reason failed", exc_info=True)
            return
        if reason:
            self.reason = reason

    async def _ws_to_sock(self, writer: asyncio.StreamWriter) -> str:
        while True:
            try:
                message = await self._ws.recv()
            except Exception as exc:  # noqa: BLE001
                return f"upstream websocket closed: {exc or type(exc).__name__}"
            if isinstance(message, str):
                await self._text(message)
                continue
            try:
                write_frame(writer, STREAM_FRAME_N6K, message)
                await writer.drain()
            except Exception as exc:  # noqa: BLE001
                return f"the extension closed the upstream: {exc or type(exc).__name__}"

    async def _sock_to_ws(self, reader: asyncio.StreamReader) -> str:
        while True:
            try:
                frame = await read_frame(reader, MAX_FRAME_BYTES)
            except ConnectionError:
                return "the extension closed the upstream"
            except FrameTooLarge as exc:
                return f"{exc} (from the extension)"
            if frame is None:
                return "the extension closed the upstream"
            frame_type, data = frame
            if frame_type != STREAM_FRAME_N6K:
                logger.error("dropping stream frame type 0x%02x from the extension for an upstream", frame_type)
                continue
            try:
                await self._ws.send(data)
            except Exception as exc:  # noqa: BLE001
                return f"upstream websocket closed: {exc or type(exc).__name__}"

    async def _text(self, text: str) -> None:
        if self._on_text is None:
            logger.debug("upstream text frame with no on_text: %.200s", text)
            return
        try:
            result = self._on_text(text)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("upstream on_text failed")

    def _hello_finished(self) -> None:
        self._hello_running = False
        self._release()

    def _release(self) -> None:
        # The extension reads the fd by number while the HELLO runs, so it must not be freed for reuse.
        if not self._hello_running and (self._pump is None or self._ended.is_set()):
            self._ext_sock.close()


@dataclass(frozen=True)
class PendingCatalog:
    """A catalog served before it exists. A client's OPEN for `name` gets FT_OPEN_PENDING (carrying
    `message`) until `attach` returns, then OPEN_ACK; if `attach` raises, OPEN_ERR with its text.

    `attach` runs concurrently with serving, on its own cursor of the connection, and must ATTACH
    `name` on it. `timeout` (seconds) fails the open if `attach` has not returned by then; a thread
    `attach` is blocked in keeps running regardless.
    """

    name: str
    attach: Callable[[duckdb.DuckDBPyConnection], Awaitable[None]]
    message: str = ""
    timeout: Optional[float] = None


@dataclass(frozen=True)
class LazyCatalog:
    """A catalog listed before anything serves it, so clients can find it. Opening it calls `resolve`
    like any unknown name (so `register` needs `resolve=`); once something serves it, it is listed
    once. While nothing does (a failed `resolve`, or the upstream serving it gone) it stays listed and
    the next open calls `resolve` again. `remove_catalog` stops listing it.

    Against upstreams that report the same name, it is a claim of `rank`: the lowest rank wins (then
    the earliest added), so an upstream of lower rank serves it and one of higher rank never does.
    `rank=None` loses to every upstream. A local catalog (`add_catalog`) always wins."""

    name: str
    rank: Optional[int] = None


ServedCatalog = Union[str, PendingCatalog, LazyCatalog]


@dataclass(frozen=True)
class Authorized:
    """What `authenticate` may return instead of a bare catalog list, to hand the connection's
    `identity` to its text handlers (`n6k_server.context.current().identity`)."""

    catalogs: Sequence[ServedCatalog]
    identity: Any = None


Authenticate = Callable[
    ..., Union[Sequence[ServedCatalog], Authorized, Awaitable[Union[Sequence[ServedCatalog], Authorized]]]
]


class LiveServe:
    """The catalogs of one connection while it serves; every change reaches the client as
    CATALOGS_CHANGED. Calls after the connection closed raise `RuntimeError`. `connection` is the
    connection `current()` returns in its text handlers. Every call waits for the serve to register
    (after HELLO_ACK is queued) and raises if it ends first; see `AwaitServe` in serve_reactor.cpp.
    """

    def __init__(
        self,
        cur: duckdb.DuckDBPyConnection,
        session_id: uuid.UUID,
        unsettled: "_Unsettled",
        connection: Connection,
    ) -> None:
        self.connection = connection
        self._cur = cur
        self._session_id = session_id
        self._unsettled = unsettled
        self._lock = asyncio.Lock()
        self._closed = False
        self._calls: set[asyncio.Future[Any]] = set()
        self._upstreams: set[Upstream] = set()

    async def add_catalog(self, catalog: ServedCatalog) -> None:
        """Serve `catalog` from now on. A name must already be attached; a `PendingCatalog` is
        attached on this handle's cursor first, and is not served if that fails or times out; a
        `LazyCatalog` is only listed (it needs `resolve`)."""
        async with self._lock:
            self._check_open()
            if isinstance(catalog, LazyCatalog):
                rank = catalog.rank
                await self._call(
                    lambda cur, session_id, name: _add_lazy_catalog(cur, session_id, name, rank), catalog.name
                )
                return
            if isinstance(catalog, PendingCatalog):
                await asyncio.wait_for(catalog.attach(self._cur), catalog.timeout)
                self._check_open()
                name = catalog.name
            else:
                name = catalog
            await self._call(_add_catalog, name)

    async def remove_catalog(self, name: str) -> None:
        """Stop serving `name`: its sessions close and their in-flight requests fail. Raises if `name`
        is not served, pending or lazy here."""
        async with self._lock, self._unsettled.lock:
            self._check_open()
            await self._call(_remove_catalog, name)
            self._unsettled.names.discard(name)

    async def connect_upstream(
        self, ws: UpstreamSocket, token: str = "", on_text: Optional[OnUpstreamText] = None
    ) -> Upstream:
        """HELLO another n6k server over `ws`, an open websocket to it, with `token`, and read its
        catalogs. Waits through its HELLO_PENDING with no overall deadline, but the first reply and each
        HELLO_PENDING must arrive in time (see `HelloOnFd`). Raises `UpstreamError` if
        `n6k_upstream_hello` fails, including on those timeouts; cancelling closes `ws`. `on_text(text)` (sync or async) gets the
        upstream's text frames for as long as it is open."""
        self._check_open()
        upstream = Upstream(ws, self, on_text)
        await upstream._start()
        self._upstreams.add(upstream)
        upstream._hello_running = True
        try:
            catalogs, capabilities = await self._call(
                lambda cur, _session_id, _name: _upstream_hello(cur, upstream.fd, token), "", upstream._hello_finished
            )
        except asyncio.CancelledError:
            await upstream.aclose()
            raise
        except Exception as exc:
            await upstream.aclose()
            raise UpstreamError(str(exc)) from exc
        upstream.catalogs = catalogs
        upstream.capabilities = capabilities
        return upstream

    async def add_upstream(self, upstream: Upstream, rank: int = 0) -> list[str]:
        """Forward to `upstream` each of its catalogs it wins, now and as it reports more. A local
        catalog always wins a name; against `LazyCatalog`s and other upstreams the lowest rank wins
        (then the earliest added), re-decided on every change. A name that changes hands fails its
        sessions on the loser. Returns the names it reports but does not win."""
        self._check_open()
        if upstream.closed:
            raise RuntimeError(f"the upstream is closed: {upstream.reason}")
        catalogs = list(upstream.catalogs)
        skipped = await self._call(
            lambda cur, session_id, _name: _add_upstream(cur, session_id, upstream.fd, catalogs, rank), ""
        )
        upstream._added = True
        return skipped

    async def set_upstream_rank(self, upstream: Upstream, rank: int) -> list[str]:
        """Change an added upstream's rank and re-decide every name it reports. Returns the names it
        reports but does not win."""
        self._check_open()
        return await self._call(
            lambda cur, session_id, _name: _set_upstream_rank(cur, session_id, upstream.fd, rank), ""
        )

    async def remove_upstream(self, upstream: Upstream) -> None:
        """Stop forwarding to `upstream` and close it; its sessions fail with "removed by host"."""
        self._check_open()
        await self._call(lambda cur, session_id, _name: _remove_upstream(cur, session_id, upstream.fd), "")
        await upstream.wait_closed()

    async def _upstream_closed_reason(self, up_fd: int) -> Optional[str]:
        return await self._call(lambda cur, session_id, _name: _upstream_status(cur, session_id, up_fd), "")

    @contextlib.asynccontextmanager
    async def paused(self, catalog: str, timeout: float = 5.0) -> AsyncIterator[None]:
        """Hold new requests for `catalog` while the body runs, e.g. to DETACH and re-attach it under
        the same name; on exit they run in arrival order against whatever `catalog` is then. Sessions
        stay open throughout. Raises if `catalog` is not served or pending here, or is already paused.

        Entering waits for requests already running against `catalog` to finish, and cancels those
        still running after `timeout` seconds; raises `TimeoutError` if any are still running
        `PAUSE_CANCEL_TIMEOUT_S` after that. A statement in a client's open transaction runs on that
        transaction's own connection, which may keep seeing the old catalog until it ends.
        """
        self._check_open()
        await self._call(_pause, catalog)
        try:
            await self._drain(catalog, timeout)
            yield
        finally:
            if not self._closed:
                await self._call(_resume, catalog)

    async def _drain(self, catalog: str, timeout: float) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        cancelled = False
        while True:
            if await self._call(_drained, catalog):
                return
            if loop.time() >= deadline:
                if cancelled:
                    raise TimeoutError(f"requests on {catalog!r} did not stop after being cancelled")
                await self._call(_cancel_inflight, catalog)
                cancelled = True
                deadline = loop.time() + PAUSE_CANCEL_TIMEOUT_S
            await asyncio.sleep(PAUSE_POLL_S)

    @property
    def closed(self) -> bool:
        return self._closed

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("the connection is no longer served")

    def _close(self) -> None:
        self._closed = True

    async def _call(
        self,
        fn: Callable[[duckdb.DuckDBPyConnection, uuid.UUID, str], _T],
        name: str,
        on_done: Optional[Callable[[], None]] = None,
    ) -> _T:
        # Its own cursor: `paused` polls while another call, or an `attach` on `_cur`, may be running.
        cur = self._cur.cursor()

        def run() -> _T:
            try:
                return fn(cur, self._session_id, name)
            finally:
                cur.close()

        # Shielded so a cancelled caller leaves the call tracked until its thread returns.
        call = asyncio.get_running_loop().run_in_executor(None, run)
        self._calls.add(call)
        call.add_done_callback(self._forget)
        if on_done is not None:
            call.add_done_callback(lambda _: on_done())
        return await asyncio.shield(call)

    async def _close_upstreams(self, timeout: float) -> None:
        closing = [asyncio.create_task(up.aclose()) for up in self._upstreams]
        if closing:
            await asyncio.wait(closing, timeout=timeout)

    def _forget(self, call: "asyncio.Future[Any]") -> None:
        self._calls.discard(call)
        if not call.cancelled():
            call.exception()

    async def _join(self, timeout: float) -> "Optional[asyncio.Future[Any]]":
        """None once every call has returned, else a future for when they will. Until then the cursor
        is in use, so it cannot be closed."""
        if not self._calls:
            return None
        calls = asyncio.gather(*self._calls, return_exceptions=True)
        try:
            await asyncio.wait_for(asyncio.shield(calls), timeout)
        except asyncio.TimeoutError:
            return calls
        return None


OnServe = Callable[..., Optional[Awaitable[None]]]
Resolve = Callable[[LiveServe, str], Optional[Awaitable[None]]]
OnText = Callable[..., Optional[Awaitable[None]]]
OnClose = Callable[[duckdb.DuckDBPyConnection], None]
SendText = Callable[[str], Awaitable[None]]


class TextBacklogFull(Exception):
    """Raise from `TextSession.notify` to close the socket with 1008: the client is not draining."""


class TextSession(Protocol):
    """One connection's handler of text frames. `handle` runs on its own task per frame, in arrival
    order up to its first await; `aclose` runs once when the connection ends."""

    async def handle(self, text: str, send_text: SendText) -> None: ...

    def handle_busy(self, text: str, send_text: SendText) -> Optional[str]:
        """Called instead of `handle` while `max_text_inflight` text tasks run: the reply to send
        ("" for none), or None to close the socket."""
        ...

    async def notify(self, text: str, send_text: SendText) -> None:
        """Send the server-initiated `text` now, or once the session can take it."""
        ...

    async def aclose(self) -> None: ...


class TextApp(Protocol):
    """Opens a `TextSession` per authenticated connection. `capabilities` are the APP_HELLO_CAPS it
    serves; the extension grants them in HELLO_ACK to clients that offer them."""

    @property
    def capabilities(self) -> Sequence[str]: ...

    def open(self) -> TextSession: ...


class _OnTextApp:
    capabilities: Sequence[str] = APP_HELLO_CAPS

    def __init__(self, on_text: OnText, ws: WebSocketLike, params: dict[str, str]) -> None:
        self._on_text = on_text
        self._ws = ws
        self._params = params

    def open(self) -> TextSession:
        return self

    async def handle(self, text: str, send_text: SendText) -> None:
        result = self._on_text(self._ws, text, send_text, **self._params)
        if inspect.isawaitable(result):
            await result

    def handle_busy(self, text: str, send_text: SendText) -> Optional[str]:
        return None

    async def notify(self, text: str, send_text: SendText) -> None:
        await send_text(text)

    async def aclose(self) -> None:
        pass


def _method_not_found(text: str) -> Optional[str]:
    try:
        message = json.loads(text)
    except ValueError:
        return None
    if not isinstance(message, dict) or "method" not in message or "id" not in message:
        return None
    return json.dumps({"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32601, "message": "Method not found"}})


class _TextChannel:
    """A client whose FT_HELLO offered no APP_HELLO_CAPS name cannot read text frames. With an app that
    declares capabilities, only the names HELLO_ACK granted count."""

    def __init__(
        self, ws: WebSocketLike, app: Optional[TextApp], send_lock: asyncio.Lock, max_inflight: Optional[int] = None
    ) -> None:
        self._ws = ws
        self._app = app
        self._send_lock = send_lock
        self.max_inflight = max_inflight
        self._session: Optional[TextSession] = None
        self._connection = Connection()
        self._accepts_text = False
        self._authorized = False
        self._closed = False
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def capabilities(self) -> Sequence[str]:
        return self._app.capabilities if self._app is not None else ()

    @property
    def connection(self) -> Connection:
        return self._connection

    def authorize(self, params: Mapping[str, str], identity: Any, hello_capabilities: Sequence[str]) -> None:
        self._connection = Connection(params, identity, self.notify)
        self._authorized = True
        granted = self.capabilities
        self._accepts_text = any(
            cap in APP_HELLO_CAPS and (not granted or cap in granted) for cap in hello_capabilities
        )
        if self._app is not None:
            self._session = self._app.open()

    def receive(self, text: str) -> bool:
        if not self._accepts_text:
            logger.debug("dropping a text frame (client offered no text capability)")
            return True
        if self._session is None:
            reply = _method_not_found(text) if self._authorized else None
            if reply is None:
                logger.debug("dropping a text frame (no text session)")
                return True
            self._spawn(self._send_quietly(reply))
            return True
        # Runs on the socket's reader task, so every task started from here inherits it.
        _CURRENT.set(self._connection)
        if self.max_inflight is None or len(self._tasks) < self.max_inflight:
            self._spawn(self._run(self._session, text))
            return True
        reply = self._session.handle_busy(text, self.send_text)
        if reply is None:
            return False
        if reply:
            self._spawn(self._send_quietly(reply))
        return True

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def notify(self, method: str, params: Any = None) -> bool:
        if self._closed or not self._accepts_text or self._session is None:
            return False
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        try:
            await self._session.notify(json.dumps(message), self.send_text)
        except TextBacklogFull as exc:
            logger.info("closing: %s", exc)
            async with self._send_lock:
                with contextlib.suppress(Exception):
                    await self._ws.close(code=1008, reason=str(exc))
            return False
        return True

    async def _send_quietly(self, text: str) -> None:
        try:
            await self.send_text(text)
        except Exception:  # noqa: BLE001
            logger.debug("dropping a reply to a closing socket", exc_info=True)

    async def send_text(self, text: str) -> None:
        if self._closed:
            raise RuntimeError("the connection is no longer served")
        if not self._accepts_text:
            raise RuntimeError("the client was not granted jsonrpc or mcp in HELLO_ACK")
        async with self._send_lock:
            await self._ws.send_text(text)

    async def _run(self, session: TextSession, text: str) -> None:
        try:
            await session.handle(text, self.send_text)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("text handler failed")

    def close(self) -> list["asyncio.Task[None]"]:
        self._closed = True
        return list(self._tasks)

    async def aclose_session(self) -> None:
        if self._session is None:
            return
        try:
            await asyncio.wait_for(self._session.aclose(), SERVE_JOIN_TIMEOUT_S)
        except Exception:  # noqa: BLE001 - teardown must not raise over the real error
            logger.exception("closing the text session failed")


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_serve_sql(
    session_id: uuid.UUID,
    catalogs: list[str],
    ping_interval_ms: Optional[int] = None,
    *,
    authenticate: Optional[str] = None,
    on_text: Optional[str] = None,
    resolve: Optional[str] = None,
    hello_pending_message: Optional[str] = None,
    workers: Optional[int] = None,
) -> str:
    """`CALL n6k_serve_fd(...)` serving `catalogs`, with the names of the UDFs the extension calls back
    (`authenticate`'s result replaces `catalogs` when it carries a `"catalogs"` key)."""
    args = [f"'{session_id}'::UUID", *(_quote_literal(name) for name in catalogs)]
    if ping_interval_ms is not None:
        args.append(f"ping_interval_ms => {ping_interval_ms}")
    if workers is not None:
        args.append(f"workers => {workers}")
    for param, value in (
        ("authenticate", authenticate),
        ("on_text", on_text),
        ("resolve", resolve),
        ("hello_pending_message", hello_pending_message),
    ):
        if value is not None:
            args.append(f"{param} => {_quote_literal(value)}")
    return f"CALL n6k_serve_fd({', '.join(args)})"


def _split_served(
    served: Sequence[ServedCatalog], resolves: bool
) -> tuple[list[str], list[PendingCatalog], dict[str, Optional[int]]]:
    ready: list[str] = []
    pending: list[PendingCatalog] = []
    lazy: dict[str, Optional[int]] = {}
    seen: set[str] = set()
    for item in served:
        if not isinstance(item, (str, PendingCatalog, LazyCatalog)):
            raise TypeError(f"authenticate returned {item!r}; expected str, PendingCatalog or LazyCatalog")
        name = item if isinstance(item, str) else item.name
        if name in seen:
            raise ValueError(f"authenticate returned catalog {name!r} more than once")
        seen.add(name)
        if isinstance(item, str):
            ready.append(item)
        elif isinstance(item, PendingCatalog):
            pending.append(item)
        elif not resolves:
            raise TypeError(f"authenticate returned {item!r}, but register was given no resolve")
        else:
            lazy[item.name] = item.rank
    return ready, pending, lazy


def _create_session(cur: duckdb.DuckDBPyConnection, fd: int) -> uuid.UUID:
    row = cur.execute("SELECT session_id FROM n6k_serve_create_session(?)", [fd]).fetchone()
    assert row is not None
    session_id: uuid.UUID = row[0]
    return session_id


def _mark_ready(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_open_ready(?, ?)", [session_id, name]).fetchall()


def _mark_failed(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str, message: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_open_failed(?, ?, ?)", [session_id, name, message]).fetchall()


def _add_catalog(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_add_catalog(?, ?)", [session_id, name]).fetchall()


def _add_lazy_catalog(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str, rank: Optional[int]) -> None:
    cur.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, ?, rank => ?)", [session_id, name, rank]).fetchall()


def _send_text(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, text: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_send_text(?, ?)", [session_id, text]).fetchall()


def _close(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, code: int, reason: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_close(?, ?, ?)", [session_id, code, reason]).fetchall()


def _upstream_hello(cur: duckdb.DuckDBPyConnection, fd: int, token: str) -> tuple[list[str], list[str]]:
    row = cur.execute("SELECT catalogs, capabilities FROM n6k_upstream_hello(?, ?)", [fd, token]).fetchone()
    assert row is not None
    return [str(c) for c in row[0]], [str(c) for c in row[1]]


def _add_upstream(
    cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, up_fd: int, catalogs: list[str], rank: int
) -> list[str]:
    row = cur.execute(
        "SELECT skipped FROM n6k_serve_add_upstream(?, ?, ?, rank => ?)", [session_id, up_fd, catalogs, rank]
    ).fetchone()
    assert row is not None
    return [str(c) for c in row[0]]


def _set_upstream_rank(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, up_fd: int, rank: int) -> list[str]:
    row = cur.execute("SELECT skipped FROM n6k_serve_set_upstream_rank(?, ?, ?)", [session_id, up_fd, rank]).fetchone()
    assert row is not None
    return [str(c) for c in row[0]]


def _remove_upstream(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, up_fd: int) -> None:
    cur.execute("SELECT * FROM n6k_serve_remove_upstream(?, ?)", [session_id, up_fd]).fetchall()


def _upstream_status(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, up_fd: int) -> Optional[str]:
    row = cur.execute("SELECT open, reason FROM n6k_serve_upstream_status(?, ?)", [session_id, up_fd]).fetchone()
    assert row is not None
    return None if row[0] else str(row[1])


def _remove_catalog(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_remove_catalog(?, ?)", [session_id, name]).fetchall()


def _pause(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_pause(?, ?)", [session_id, name]).fetchall()


def _resume(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_resume(?, ?)", [session_id, name]).fetchall()


def _drained(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> bool:
    row = cur.execute("SELECT holding, running FROM n6k_serve_inflight(?, ?)", [session_id, name]).fetchone()
    assert row is not None
    return bool(row[0]) and int(row[1]) == 0


def _cancel_inflight(cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_cancel_inflight(?, ?)", [session_id, name]).fetchall()


class _Unsettled:
    """The pending catalogs not yet marked ready or failed. Settling one and `remove_catalog` of it
    hold `lock`, so a catalog removed while its `attach` ran is never settled."""

    def __init__(self, names: Sequence[str]) -> None:
        self.names = set(names)
        self.lock = asyncio.Lock()


async def _settle_pending(
    pending: PendingCatalog, cur: duckdb.DuckDBPyConnection, session_id: uuid.UUID, unsettled: _Unsettled
) -> None:
    failure: Optional[str] = None
    try:
        await asyncio.wait_for(pending.attach(cur), pending.timeout)
    except asyncio.TimeoutError:
        logger.info("pending catalog %r timed out after %ss", pending.name, pending.timeout)
        failure = f"did not become ready within {pending.timeout}s"
    except Exception as exc:  # noqa: BLE001
        logger.info("pending catalog %r failed: %s", pending.name, exc)
        failure = str(exc) or type(exc).__name__
    async with unsettled.lock:
        if pending.name not in unsettled.names:
            logger.info("pending catalog %r was removed before it settled", pending.name)
            return
        if failure is None:
            try:
                await asyncio.to_thread(_mark_ready, cur, session_id, pending.name)
            except Exception as exc:  # noqa: BLE001 - e.g. `attach` returned without attaching it
                logger.info("pending catalog %r failed: %s", pending.name, exc)
                failure = str(exc) or type(exc).__name__
        if failure is not None:
            await asyncio.to_thread(_mark_failed, cur, session_id, pending.name, failure)
        unsettled.names.discard(pending.name)


async def _run_on_serve(on_serve: OnServe, ws: Any, handle: LiveServe, params: dict[str, str]) -> None:
    try:
        result = on_serve(ws, handle, **params)
        if inspect.isawaitable(result):
            await result
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - a failing hook must not end the connection
        logger.exception("on_serve failed")


async def _authenticate(
    authenticate: Authenticate, ws: Any, con: duckdb.DuckDBPyConnection, token: str, params: dict[str, str]
) -> Authorized:
    result = authenticate(ws, con, token, **params)
    if inspect.isawaitable(result):
        result = await result
    if isinstance(result, Authorized):
        return result
    return Authorized(list(result))


class _ServedText:
    """Sends a `_TextChannel`'s text through the extension."""

    def __init__(self, handle: LiveServe) -> None:
        self._handle = handle

    async def send_text(self, data: str) -> None:
        await self._handle._call(lambda cur, session_id, _name: _send_text(cur, session_id, data), "")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._handle._call(lambda cur, session_id, _name: _close(cur, session_id, code, reason[:123]), "")


class _Callbacks:
    """The UDFs `n6k_serve_fd` calls back, each run on this loop. They run on the extension's threads, so
    each blocks only that thread: `authenticate` on a thread of its own, `on_text` on the reactor's text
    worker (it only schedules delivery, so never blocks), and `resolve` serially on the reactor's single
    resolver thread (`ResolverLoop`), so a slow resolve delays the connection's others."""

    def __init__(
        self,
        ws: Any,
        con: duckdb.DuckDBPyConnection,
        authenticate: Authenticate,
        params: dict[str, str],
        handle: LiveServe,
        text: "_TextChannel",
        on_serve: Optional[OnServe],
        resolve: Optional[Resolve],
    ) -> None:
        self._loop = asyncio.get_running_loop()
        self._ws = ws
        self._con = con
        self._authenticate = authenticate
        self._params = params
        self._handle = handle
        self._text = text
        self._on_serve = on_serve
        self._resolve = resolve
        self._closing = False
        self._resolving: set[Future[Optional[str]]] = set()
        self.settlers: list[asyncio.Task[None]] = []
        self.cursors: list[duckdb.DuckDBPyConnection] = []
        # create_function registers into the DATABASE catalog; the suffix keeps connections apart.
        suffix = uuid.uuid4().hex
        self.authenticate_udf = f"__n6k_authenticate_{suffix}"
        self.on_text_udf = f"__n6k_on_text_{suffix}" if text.capabilities else None
        self.resolve_udf = f"__n6k_resolve_{suffix}" if resolve is not None else None

    def create_functions(self) -> None:
        special = FunctionNullHandling.SPECIAL
        self._con.create_function(
            self.authenticate_udf,
            self._authenticate_udf,
            [_UUID, _VARCHAR, _VARCHAR_LIST, _VARCHAR],
            _VARCHAR,
            null_handling=special,
            side_effects=True,
        )
        if self.on_text_udf is not None:
            self._con.create_function(
                self.on_text_udf,
                self._on_text_udf,
                [_UUID, _VARCHAR],
                _VARCHAR,
                null_handling=special,
                side_effects=True,
            )
        if self.resolve_udf is not None:
            self._con.create_function(
                self.resolve_udf,
                self._resolve_udf,
                [_UUID, _VARCHAR],
                _VARCHAR,
                null_handling=special,
                side_effects=True,
            )

    def close(self) -> list["asyncio.Task[None]"]:
        self._closing = True
        for future in list(self._resolving):
            future.cancel()
        return self.settlers

    def _authenticate_udf(
        self, session_id: uuid.UUID, token: str, capabilities: list[str], request: Optional[str]
    ) -> str:
        return asyncio.run_coroutine_threadsafe(self._admit(token, list(capabilities or ())), self._loop).result()

    async def _admit(self, token: str, capabilities: list[str]) -> str:
        """Returns the JSON `ParseAuthResult` (src/n6k_server/host_callbacks.cpp) parses."""
        try:
            authorized = await _authenticate(self._authenticate, self._ws, self._con, token, self._params)
            catalogs, pending, lazy = _split_served(authorized.catalogs, self._resolve is not None)
        except WsReject as reject:
            return json.dumps({"reject": reject.reason or "unauthorized", "code": reject.code})
        except Exception:  # noqa: BLE001 - any failure refuses the client rather than serving it
            logger.exception("authenticate failed")
            return json.dumps({"reject": "internal error", "code": 1011})
        if self._closing:
            return json.dumps({"reject": "the connection closed", "code": 1000})
        self._text.authorize(self._params, authorized.identity, capabilities)
        handle = self._handle
        handle.connection = self._text.connection
        handle._unsettled.names.update(p.name for p in pending)
        for p in pending:
            cur = handle._cur.cursor()
            self.cursors.append(cur)
            self.settlers.append(asyncio.create_task(_settle_pending(p, cur, handle._session_id, handle._unsettled)))
        if self._on_serve is not None:
            self.settlers.append(asyncio.create_task(_run_on_serve(self._on_serve, self._ws, handle, self._params)))
        return json.dumps(
            {
                "catalogs": catalogs,
                "pending": {p.name: p.message for p in pending},
                "lazy": lazy,
                "app_capabilities": list(self._text.capabilities),
            }
        )

    def _on_text_udf(self, session_id: uuid.UUID, text: str) -> None:
        self._loop.call_soon_threadsafe(self._deliver, text)

    def _deliver(self, text: str) -> None:
        if self._closing or self._text.receive(text):
            return
        logger.info("closing: more than %d text messages in flight", self._text.max_inflight)
        self.settlers.append(asyncio.create_task(self._close_busy()))

    async def _close_busy(self) -> None:
        with contextlib.suppress(Exception):
            await _ServedText(self._handle).close(1008, "too many text messages in flight")

    def _resolve_udf(self, session_id: uuid.UUID, name: str) -> Optional[str]:
        if self._closing:
            return "the connection closed"
        future = asyncio.run_coroutine_threadsafe(self._run_resolve(name), self._loop)
        self._resolving.add(future)
        try:
            return future.result()
        except BaseException:  # noqa: BLE001 - cancelled at teardown
            return "the connection closed"
        finally:
            self._resolving.discard(future)

    async def _run_resolve(self, name: str) -> Optional[str]:
        assert self._resolve is not None
        _CURRENT.set(self._handle.connection)
        try:
            result = self._resolve(self._handle, name)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.info("resolving catalog %r failed: %s", name, exc)
            return str(exc) or type(exc).__name__
        return None


def _start_directions(
    ws: WebSocketLike,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    send_lock: asyncio.Lock,
) -> tuple["asyncio.Task[None]", "asyncio.Task[None]"]:
    async def ws_to_sock() -> None:
        while True:
            try:
                message = await ws.receive()
            except Exception:
                # A receive after the client closed raises. That is an ordinary end of stream, not a
                # failure.
                return
            if message.get("type") == "websocket.disconnect":
                return
            data = message.get("bytes")
            frame_type = STREAM_FRAME_N6K
            if data is None:
                data = (message.get("text") or "").encode()
                frame_type = STREAM_FRAME_JSONRPC
            write_frame(writer, frame_type, data)
            await writer.drain()

    async def sock_to_ws() -> None:
        while True:
            try:
                frame = await read_frame(reader, MAX_FRAME_BYTES)
            except FrameTooLarge as exc:
                logger.error("closing: %s (from the extension)", exc)
                return
            if frame is None:
                return  # the extension closed
            frame_type, data = frame
            if frame_type not in (STREAM_FRAME_N6K, STREAM_FRAME_JSONRPC, STREAM_FRAME_CLOSE):
                logger.error("closing: stream frame type 0x%02x from the extension", frame_type)
                return
            if frame_type == STREAM_FRAME_CLOSE:
                close = msgpack.unpackb(data)
                async with send_lock:
                    with contextlib.suppress(Exception):
                        await ws.close(code=int(close["code"]), reason=str(close["reason"]))
                return
            async with send_lock:
                if frame_type == STREAM_FRAME_JSONRPC:
                    await ws.send_text(data.decode())
                else:
                    await ws.send_bytes(data)

    return asyncio.create_task(ws_to_sock()), asyncio.create_task(sock_to_ws())


async def _stage_won(stage: "asyncio.Future[Any]", *directions: "asyncio.Task[None]") -> bool:
    done, _ = await asyncio.wait({stage, *directions}, return_when=asyncio.FIRST_COMPLETED)
    return stage in done


async def _exited_within(task: "Optional[asyncio.Future[Any]]", timeout: float) -> bool:
    if task is None:
        return True
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except asyncio.TimeoutError:
        return False
    except Exception:  # noqa: BLE001 - teardown must not raise over the real error
        pass
    return True


def _close_con(con: duckdb.DuckDBPyConnection, on_close: Optional[OnClose]) -> None:
    if on_close is not None:
        try:
            on_close(con)
        except Exception:  # noqa: BLE001 - the connection is closed regardless
            logger.exception("on_close failed")
    con.close()


def _close_cursors(cursors: Sequence[duckdb.DuckDBPyConnection]) -> None:
    for cur in cursors:
        try:
            cur.close()
        except Exception:  # noqa: BLE001 - closed with `con` regardless
            pass


def _close_all(
    cursors: Sequence[duckdb.DuckDBPyConnection], con: duckdb.DuckDBPyConnection, on_close: Optional[OnClose]
) -> None:
    _close_cursors(cursors)
    _close_con(con, on_close)


def _on_own_thread(fn: Callable[..., Any], *args: Any) -> "asyncio.Future[Any]":
    """`fn(*args)` on a thread of its own, as a future on this loop.

    Not the default executor: a serving CALL holds its thread for the connection's life, so a pool
    sized for short calls would cap the connections at its size and starve every other `to_thread`.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[Any] = loop.create_future()

    def settle(result: Any, error: Optional[BaseException]) -> None:
        if future.cancelled():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(result)

    def run() -> None:
        try:
            result, error = fn(*args), None
        except BaseException as exc:  # noqa: BLE001 - handed to the awaiting side as-is
            result, error = None, exc
        try:
            loop.call_soon_threadsafe(settle, result, error)
        except RuntimeError:
            pass  # the loop already closed: nobody is left to hear the outcome

    threading.Thread(target=run, name="n6k-serve").start()
    return future


def _log_late_failure(task: "asyncio.Future[Any]") -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.info("stage failed after its socket closed: %s", task.exception())


async def serve_and_close_connection(
    ws: WebSocketLike,
    con: duckdb.DuckDBPyConnection,
    authenticate: Authenticate,
    params: dict[str, str],
    *,
    ping_interval_ms: Optional[int] = None,
    hello_pending_message: str = "authenticating",
    on_serve: Optional[OnServe] = None,
    on_text: Optional[OnText] = None,
    text_app: Optional[TextApp] = None,
    resolve: Optional[Resolve] = None,
    max_text_inflight: Optional[int] = None,
    workers: Optional[int] = None,
    on_close: Optional[OnClose] = None,
) -> None:
    """Handshake, authenticate, then serve `ws` from `con` until either side drops; then close `con`.

    `con` arrives empty. The extension reads the client's FT_HELLO and calls back, and only then
    does `authenticate(ws, con, token, **params)` attach what that identity may see and return the
    catalogs to serve: names already attached, `PendingCatalog`s attached while serving, and, with
    `resolve`, `LazyCatalog`s. While it runs past the extension's `hello_pending_first_ms`, the client gets
    FT_HELLO_PENDING carrying `hello_pending_message`. A `WsReject` or any other exception sends
    FT_HELLO_ERR and nothing is served; an empty list logs the client in with no catalogs. Returning
    `Authorized(catalogs, identity)` hands `identity` to text handlers.

    Ownership passes here: the serving CALL runs on a cursor of `con` for the connection's life.

    Once `authenticate` returns, `on_serve(ws, handle, **params)` (sync or async) gets a `LiveServe` for
    adding and removing catalogs; it is cancelled when the connection ends. It starts before the
    extension validates that result and sends HELLO_ACK, so it also runs on a connection the extension
    then rejects.

    With `resolve`, a client's OPEN for a catalog that is not served waits (FT_OPEN_PENDING) while
    `resolve(handle, name)` (sync or async) runs on that same `LiveServe`, once per name however many
    OPENs wait on it. When it returns, the OPENs get OPEN_ACK if it served the catalog (e.g. ATTACH plus
    `handle.add_catalog`), are forwarded if an upstream then serves it, and get OPEN_ERR "not found"
    otherwise; if it raises, OPEN_ERR with its message.
    Nothing is remembered, so a later OPEN of the name calls it again. It is cancelled when the
    connection ends.

    Binary frames are n6k; text frames go to `on_text(ws, text, send_text, **params)` (sync or
    async), or to a session `text_app` opens, each on its own task, once HELLO_ACK is sent. Earlier
    text is dropped; with no handler, a JSON-RPC request gets -32601 and other text is dropped.
    `send_text` raises when the client's FT_HELLO offered neither jsonrpc nor mcp (or, for a
    `text_app`, HELLO_ACK granted neither), or once the connection has ended. With `max_text_inflight`,
    more handlers than that running at once closes the socket (1008) unless the session's `handle_busy`
    answers it; None (the default) sets no limit. Handlers are cancelled when the
    connection ends. `n6k_server.context.current()` inside a handler is the connection's params and
    identity.

    `workers`: `n6k_serve_fd`'s `workers` option; None keeps its default.

    `con` is closed off the event loop and only after every query on it returns; this returns without
    waiting for them, so a query that never returns leaves `con` open. `on_close(con)` runs on that
    thread after the cursors close and before `con.close()`, with nothing else using `con`; if it raises, the error is logged and `con` is
    closed anyway.
    """
    if on_text is not None and text_app is not None:
        raise ValueError("pass on_text or text_app, not both")
    if max_text_inflight is not None and max_text_inflight <= 0:
        raise ValueError(f"max_text_inflight must be None (no limit) or positive, got {max_text_inflight}")
    if on_text is not None:
        text_app = _OnTextApp(on_text, ws, params)
    host_sock, ext_sock = socket.socketpair()
    stage: Optional[asyncio.Future[Any]] = None
    directions: tuple[asyncio.Task[None], ...] = ()
    writer: Optional[asyncio.StreamWriter] = None
    transport_owns_host_sock = False
    send_lock = asyncio.Lock()
    # Taken before the CALL: a cursor of a connection that is executing waits for it. `con` itself stays
    # free for `authenticate`.
    serve_cur = con.cursor()
    session_id = _create_session(serve_cur, ext_sock.fileno())
    handle = LiveServe(con.cursor(), session_id, _Unsettled(()), Connection())
    cursors = [serve_cur]
    text = _TextChannel(_ServedText(handle), text_app, asyncio.Lock(), max_text_inflight)  # type: ignore[arg-type]
    callbacks = _Callbacks(ws, con, authenticate, params, handle, text, on_serve, resolve)
    try:
        reader, writer = await asyncio.open_connection(sock=host_sock)
        transport_owns_host_sock = True
        directions = _start_directions(ws, reader, writer, send_lock)
        callbacks.create_functions()
        sql = build_serve_sql(
            session_id,
            [],
            ping_interval_ms,
            authenticate=callbacks.authenticate_udf,
            on_text=callbacks.on_text_udf,
            resolve=callbacks.resolve_udf,
            hello_pending_message=hello_pending_message,
            workers=workers,
        )
        stage = _on_own_thread(serve_cur.execute, sql)
        await _stage_won(stage, *directions)
        if stage.done():
            stage.result()
    finally:
        # Synchronous, so a cancellation that aborts every await below still EOFs the running stage.
        if stage is not None and not stage.done():
            try:
                host_sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
        handle._close()
        settlers = [*callbacks.close(), *text.close()]
        cursors.extend(callbacks.cursors)
        if stage is not None and stage.done() and len(directions) == 2:
            try:
                await asyncio.wait_for(asyncio.shield(directions[1]), timeout=DRAIN_TIMEOUT_S)
            except Exception:  # noqa: BLE001 - a drain that cannot finish is abandoned
                pass
        for task in (*directions, *settlers):
            task.cancel()
        await asyncio.gather(*directions, return_exceptions=True)
        if settlers:
            await asyncio.wait(settlers, timeout=SERVE_JOIN_TIMEOUT_S)
        await text.aclose_session()
        # Closing our end EOFs the extension's, which ends whatever stage is still reading it.
        # `close()` only *starts* that, so the wait is required.
        if writer is not None:
            if not writer.is_closing():
                writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001 - a broken transport is already closed enough
                pass
        if not transport_owns_host_sock:
            host_sock.close()
        exited = await _exited_within(stage, SERVE_JOIN_TIMEOUT_S)
        await handle._close_upstreams(SERVE_JOIN_TIMEOUT_S)
        ext_sock.close()
        calls = await handle._join(SERVE_JOIN_TIMEOUT_S)
        try:
            await ws.close()
        except Exception:  # noqa: BLE001 - already closed by the peer, by the extension, or mid-close
            pass
        late_cursors = [handle._cur] if calls is not None else []
        if calls is None:
            cursors.append(handle._cur)
        # A thread that outlived the timeout still owns `con`; closing it underneath would race.
        busy: list[asyncio.Future[Any]] = []
        if not exited and stage is not None:
            stage.add_done_callback(_log_late_failure)
            busy.append(stage)
        if calls is not None:
            busy.append(calls)
        # Off the loop: a query still running on `con` may be a provider call waiting on this loop.
        loop = asyncio.get_running_loop()
        if busy:

            def close_late(_: "asyncio.Future[Any]") -> None:
                loop.run_in_executor(None, _close_all, [*cursors, *late_cursors], con, on_close)

            asyncio.gather(*busy, return_exceptions=True).add_done_callback(close_late)
        else:
            await loop.run_in_executor(None, _close_all, cursors, con, on_close)
