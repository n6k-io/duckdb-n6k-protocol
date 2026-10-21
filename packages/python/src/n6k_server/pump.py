"""Serve a WebSocket by handing it to the C++ reactor and shuttling bytes.

This module is the whole of Python's involvement in a served connection. It never decodes an
n6k frame, never inspects an op, and cannot tell a SCAN from a PING: the protocol lives in
`src/n6k_server`. It reads only the stream framing envelope, the extension's host events (JSON,
`_Resolver.receive`), and text frames, which belong to the app.

The mechanism is a `socketpair()`. Python keeps one end and passes the *number*
of the other to SQL — `CALL n6k_serve_fd(<fd>, 'db')` — which the extension
`dup()`s and then reads and writes like any socket.

The handshake replies are the exception: once `n6k_handshake` has read the token, the extension
builds HELLO_PENDING, HELLO_ERR and HELLO_ACK (`n6k_hello_pending` / `n6k_reject` /
`n6k_hello_ack`) and Python sends them itself, so it knows HELLO_ACK has reached the socket before
any text it sends. When `n6k_handshake` rejects the HELLO it sends its own HELLO_ERR.

In-process rather than a spawned `duckdb`, because the reactor's workers open
connections on *this* `DatabaseInstance`. That is what lets a table backed by
`n6k_server.provider` — whose rows come from Python callbacks marshalled to this
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
import struct
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

from n6k_protocol.protocol import APP_HELLO_CAPS, OPEN_PENDING_INTERVAL_MS, STREAM_FRAME_HOST, STREAM_FRAME_N6K
from n6k_server.context import _CURRENT, Connection
from n6k_server.provider import release_providers

logger = logging.getLogger("n6k_server.pump")

STREAM_HEADER = struct.Struct(">IB")
# Bounds a corrupt length prefix from the extension; far above any real frame.
MAX_FRAME_BYTES = 1 << 30
# How long to wait for the serving thread to unwind after the socket drops.
SERVE_JOIN_TIMEOUT_S = 5.0
DRAIN_TIMEOUT_S = 5.0
# `LiveServe.paused`: how long cancelled requests get to unwind, and how often the drain is polled.
PAUSE_CANCEL_TIMEOUT_S = 5.0
PAUSE_POLL_S = 0.02

_T = TypeVar("_T")


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
                writer.write(STREAM_HEADER.pack(len(message), STREAM_FRAME_N6K) + message)
                await writer.drain()
            except Exception as exc:  # noqa: BLE001
                return f"the extension closed the upstream: {exc or type(exc).__name__}"

    async def _sock_to_ws(self, reader: asyncio.StreamReader) -> str:
        while True:
            try:
                header = await reader.readexactly(STREAM_HEADER.size)
            except (asyncio.IncompleteReadError, ConnectionError):
                return "the extension closed the upstream"
            n, frame_type = STREAM_HEADER.unpack(header)
            if n > MAX_FRAME_BYTES:
                return f"frame of {n} bytes from the extension exceeds {MAX_FRAME_BYTES}"
            try:
                data = await reader.readexactly(n)
            except (asyncio.IncompleteReadError, ConnectionError):
                return "the extension closed the upstream"
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
    connection `current()` returns in its text handlers; HELLO_ACK has been sent, so it may notify.
    """

    def __init__(
        self, cur: duckdb.DuckDBPyConnection, fd: int, unsettled: "_Unsettled", connection: Connection
    ) -> None:
        self.connection = connection
        self._cur = cur
        self._fd = fd
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
                await self._call(lambda cur, fd, name: _add_lazy_catalog(cur, fd, name, rank), catalog.name)
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
                lambda cur, _fd, _name: _upstream_hello(cur, upstream.fd, token), "", upstream._hello_finished
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
        skipped = await self._call(lambda cur, fd, _name: _add_upstream(cur, fd, upstream.fd, catalogs, rank), "")
        upstream._added = True
        return skipped

    async def set_upstream_rank(self, upstream: Upstream, rank: int) -> list[str]:
        """Change an added upstream's rank and re-decide every name it reports. Returns the names it
        reports but does not win."""
        self._check_open()
        return await self._call(lambda cur, fd, _name: _set_upstream_rank(cur, fd, upstream.fd, rank), "")

    async def remove_upstream(self, upstream: Upstream) -> None:
        """Stop forwarding to `upstream` and close it; its sessions fail with "removed by host"."""
        self._check_open()
        await self._call(lambda cur, fd, _name: _remove_upstream(cur, fd, upstream.fd), "")
        await upstream.wait_closed()

    async def _upstream_closed_reason(self, up_fd: int) -> Optional[str]:
        return await self._call(lambda cur, fd, _name: _upstream_status(cur, fd, up_fd), "")

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
        fn: Callable[[duckdb.DuckDBPyConnection, int, str], _T],
        name: str,
        on_done: Optional[Callable[[], None]] = None,
    ) -> _T:
        # Its own cursor: `paused` polls while another call, or an `attach` on `_cur`, may be running.
        cur = self._cur.cursor()

        def run() -> _T:
            try:
                return fn(cur, self._fd, name)
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
        is in use and the fd may still be looked up, so neither can be closed."""
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
    fd: int,
    catalogs: list[str],
    ping_interval_ms: Optional[int] = None,
    pending: Optional[Mapping[str, str]] = None,
    capabilities: Sequence[str] = (),
    app_capabilities: Sequence[str] = (),
    hello_acked: bool = False,
    resolve: bool = False,
    lazy: Optional[Mapping[str, Optional[int]]] = None,
) -> str:
    """`CALL n6k_serve_fd(...)` serving exactly `catalogs`, plus `pending` (name -> status message).

    `capabilities` is the client's FT_HELLO list as `n6k_handshake` returned it; the extension
    negotiates it. `app_capabilities` are the APP_HELLO_CAPS this socket's app serves.
    `hello_acked` means the host already sent HELLO_ACK (`n6k_hello_ack`). `resolve` parks an OPEN for
    an unknown catalog and asks the host to resolve it (STREAM_FRAME_HOST); `lazy` (name -> rank or
    None) names are listed before that (they need `resolve`)."""
    args = [str(fd), *(_quote_literal(name) for name in catalogs)]
    if ping_interval_ms is not None:
        args.append(f"ping_interval_ms => {ping_interval_ms}")
    if capabilities:
        args.append(f"capabilities => [{', '.join(_quote_literal(c) for c in capabilities)}]")
    if app_capabilities:
        args.append(f"app_capabilities => [{', '.join(_quote_literal(c) for c in app_capabilities)}]")
    if hello_acked:
        args.append("hello_acked => true")
    if resolve:
        args.append("resolve => true")
    if lazy:
        entries = ", ".join(
            f"{_quote_literal(name)}: {'NULL' if rank is None else int(rank)}" for name, rank in lazy.items()
        )
        args.append(f"lazy => MAP {{{entries}}}")
    if pending:
        entries = ", ".join(f"{_quote_literal(name)}: {_quote_literal(msg)}" for name, msg in pending.items())
        args.append(f"pending => MAP {{{entries}}}")
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


def _mark_ready(cur: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_open_ready(?, ?)", [fd, name]).fetchall()


def _mark_failed(cur: duckdb.DuckDBPyConnection, fd: int, name: str, message: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_open_failed(?, ?, ?)", [fd, name, message]).fetchall()


def _add_catalog(cur: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_add_catalog(?, ?)", [fd, name]).fetchall()


def _add_lazy_catalog(cur: duckdb.DuckDBPyConnection, fd: int, name: str, rank: Optional[int]) -> None:
    cur.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, ?, rank => ?)", [fd, name, rank]).fetchall()


def _resolved(cur: duckdb.DuckDBPyConnection, fd: int, name: str, error: Optional[str]) -> None:
    cur.execute("SELECT * FROM n6k_serve_resolved(?, ?, ?)", [fd, name, error]).fetchall()


def _upstream_hello(cur: duckdb.DuckDBPyConnection, fd: int, token: str) -> tuple[list[str], list[str]]:
    row = cur.execute("SELECT catalogs, capabilities FROM n6k_upstream_hello(?, ?)", [fd, token]).fetchone()
    assert row is not None
    return [str(c) for c in row[0]], [str(c) for c in row[1]]


def _add_upstream(cur: duckdb.DuckDBPyConnection, fd: int, up_fd: int, catalogs: list[str], rank: int) -> list[str]:
    row = cur.execute(
        "SELECT skipped FROM n6k_serve_add_upstream(?, ?, ?, rank => ?)", [fd, up_fd, catalogs, rank]
    ).fetchone()
    assert row is not None
    return [str(c) for c in row[0]]


def _set_upstream_rank(cur: duckdb.DuckDBPyConnection, fd: int, up_fd: int, rank: int) -> list[str]:
    row = cur.execute("SELECT skipped FROM n6k_serve_set_upstream_rank(?, ?, ?)", [fd, up_fd, rank]).fetchone()
    assert row is not None
    return [str(c) for c in row[0]]


def _remove_upstream(cur: duckdb.DuckDBPyConnection, fd: int, up_fd: int) -> None:
    cur.execute("SELECT * FROM n6k_serve_remove_upstream(?, ?)", [fd, up_fd]).fetchall()


def _upstream_status(cur: duckdb.DuckDBPyConnection, fd: int, up_fd: int) -> Optional[str]:
    row = cur.execute("SELECT open, reason FROM n6k_serve_upstream_status(?, ?)", [fd, up_fd]).fetchone()
    assert row is not None
    return None if row[0] else str(row[1])


def _remove_catalog(cur: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_remove_catalog(?, ?)", [fd, name]).fetchall()


def _pause(cur: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_pause(?, ?)", [fd, name]).fetchall()


def _resume(cur: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_resume(?, ?)", [fd, name]).fetchall()


def _drained(cur: duckdb.DuckDBPyConnection, fd: int, name: str) -> bool:
    row = cur.execute("SELECT holding, running FROM n6k_serve_inflight(?, ?)", [fd, name]).fetchone()
    assert row is not None
    return bool(row[0]) and int(row[1]) == 0


def _cancel_inflight(cur: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    cur.execute("SELECT * FROM n6k_serve_cancel_inflight(?, ?)", [fd, name]).fetchall()


class _Unsettled:
    """The pending catalogs not yet marked ready or failed. Settling one and `remove_catalog` of it
    hold `lock`, so a catalog removed while its `attach` ran is never settled."""

    def __init__(self, names: Sequence[str]) -> None:
        self.names = set(names)
        self.lock = asyncio.Lock()


async def _settle_pending(
    pending: PendingCatalog, cur: duckdb.DuckDBPyConnection, fd: int, unsettled: _Unsettled
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
                await asyncio.to_thread(_mark_ready, cur, fd, pending.name)
            except Exception as exc:  # noqa: BLE001 - e.g. `attach` returned without attaching it
                logger.info("pending catalog %r failed: %s", pending.name, exc)
                failure = str(exc) or type(exc).__name__
        if failure is not None:
            await asyncio.to_thread(_mark_failed, cur, fd, pending.name, failure)
        unsettled.names.discard(pending.name)


class _Resolver:
    """Runs `resolve(live, name)` for each catalog the extension asks about, then settles it with
    `n6k_serve_resolved`: NULL if `resolve` returned (the extension ACKs if the catalog is served by
    then, or forwards it to the upstream serving it, else OPEN_ERRs "not found"), its message if it
    raised. The extension asks once per name until it is settled."""

    def __init__(self, resolve: Optional[Resolve]) -> None:
        self._resolve = resolve
        self._handle: Optional[LiveServe] = None
        self._tasks: set[asyncio.Task[None]] = set()

    def bind(self, handle: LiveServe) -> None:
        self._handle = handle

    def receive(self, payload: bytes) -> None:
        try:
            name = json.loads(payload)["resolve"]
        except (ValueError, KeyError, TypeError):
            logger.error("dropping a malformed host event from the extension: %r", payload[:200])
            return
        if self._resolve is None or self._handle is None:
            logger.error("dropping a resolve request for %r: no resolve hook is serving", name)
            return
        task = asyncio.create_task(self._run(self._resolve, self._handle, str(name)))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run(self, resolve: Resolve, handle: LiveServe, name: str) -> None:
        _CURRENT.set(handle.connection)
        error: Optional[str] = None
        try:
            result = resolve(handle, name)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.info("resolving catalog %r failed: %s", name, exc)
            error = str(exc) or type(exc).__name__
        if handle.closed:
            return
        try:
            await handle._call(lambda cur, fd, n: _resolved(cur, fd, n, error), name)
        except Exception:  # noqa: BLE001
            logger.exception("settling the resolve of catalog %r failed", name)

    def close(self) -> list["asyncio.Task[None]"]:
        return list(self._tasks)


async def _run_on_serve(on_serve: OnServe, ws: Any, handle: LiveServe, params: dict[str, str]) -> None:
    try:
        result = on_serve(ws, handle, **params)
        if inspect.isawaitable(result):
            await result
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - a failing hook must not end the connection
        logger.exception("on_serve failed")


def _handshake(con: duckdb.DuckDBPyConnection, fd: int) -> tuple[str, list[str]]:
    row = con.execute("SELECT token, capabilities FROM n6k_handshake(?)", [fd]).fetchone()
    assert row is not None
    return str(row[0]), [str(c) for c in row[1]]


def _frame(cur: duckdb.DuckDBPyConnection, sql: str, params: Sequence[Any] = ()) -> bytes:
    row = cur.execute(sql, list(params)).fetchone()
    assert row is not None
    return bytes(row[0])


def _reject(con: duckdb.DuckDBPyConnection, reason: str) -> bytes:
    return _frame(con, "SELECT frame FROM n6k_reject(?)", [reason])


def _hello_pending(cur: duckdb.DuckDBPyConnection, message: str) -> bytes:
    return _frame(cur, "SELECT frame FROM n6k_hello_pending(?)", [message])


def _hello_ack(con: duckdb.DuckDBPyConnection, capabilities: Sequence[str], app_capabilities: Sequence[str]) -> bytes:
    args = []
    if capabilities:
        args.append(f"capabilities => [{', '.join(_quote_literal(c) for c in capabilities)}]")
    if app_capabilities:
        args.append(f"app_capabilities => [{', '.join(_quote_literal(c) for c in app_capabilities)}]")
    return _frame(con, f"SELECT frame FROM n6k_hello_ack({', '.join(args)})")


async def _send_frame(ws: WebSocketLike, send_lock: asyncio.Lock, data: bytes) -> bool:
    try:
        async with send_lock:
            await ws.send_bytes(data)
    except Exception:  # noqa: BLE001 - the client is gone; the directions see it too
        return False
    return True


# Well inside the client's HANDSHAKE_TIMEOUT_MS wait for a first reply, and long enough that a fast
# authenticate answers with HELLO_ACK alone.
HELLO_PENDING_FIRST_S = 1.0


async def _beat_hello_pending(
    cur: duckdb.DuckDBPyConnection, ws: WebSocketLike, send_lock: asyncio.Lock, message: str, stop: asyncio.Event
) -> None:
    """Stopped by the event, not by cancellation, so the caller can await the in-flight send before
    HELLO_ACK or HELLO_ERR."""
    delay = HELLO_PENDING_FIRST_S
    while True:
        try:
            await asyncio.wait_for(stop.wait(), delay)
            return
        except asyncio.TimeoutError:
            pass
        await _send_frame(ws, send_lock, await asyncio.to_thread(_hello_pending, cur, message))
        delay = OPEN_PENDING_INTERVAL_MS / 1000


async def _authenticate(
    authenticate: Authenticate, ws: Any, con: duckdb.DuckDBPyConnection, token: str, params: dict[str, str]
) -> Authorized:
    result = authenticate(ws, con, token, **params)
    if inspect.isawaitable(result):
        result = await result
    if isinstance(result, Authorized):
        return result
    return Authorized(list(result))


def _start_directions(
    ws: WebSocketLike,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    text: _TextChannel,
    send_lock: asyncio.Lock,
    resolver: _Resolver,
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
            if data is None:
                if text.receive(message.get("text") or ""):
                    continue
                logger.info("closing: more than %d text messages in flight", text.max_inflight)
                async with send_lock:
                    with contextlib.suppress(Exception):
                        await ws.close(code=1008, reason="too many text messages in flight")
                return
            writer.write(STREAM_HEADER.pack(len(data), STREAM_FRAME_N6K) + data)
            await writer.drain()

    async def sock_to_ws() -> None:
        while True:
            try:
                header = await reader.readexactly(STREAM_HEADER.size)
            except asyncio.IncompleteReadError:
                return  # the extension closed
            n, frame_type = STREAM_HEADER.unpack(header)
            if frame_type not in (STREAM_FRAME_N6K, STREAM_FRAME_HOST):
                logger.error("closing: stream frame type 0x%02x from the extension, expected n6k", frame_type)
                return
            if n > MAX_FRAME_BYTES:
                logger.error("closing: frame of %d bytes from the extension exceeds %d", n, MAX_FRAME_BYTES)
                return
            data = await reader.readexactly(n)
            if frame_type == STREAM_FRAME_HOST:
                resolver.receive(data)
                continue
            async with send_lock:
                await ws.send_bytes(data)

    return asyncio.create_task(ws_to_sock()), asyncio.create_task(sock_to_ws())


async def _stage_won(stage: "asyncio.Task[Any]", *directions: "asyncio.Task[None]") -> bool:
    done, _ = await asyncio.wait({stage, *directions}, return_when=asyncio.FIRST_COMPLETED)
    return stage in done


async def _exited_within(task: "Optional[asyncio.Task[Any]]", timeout: float) -> bool:
    if task is None:
        return True
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except asyncio.TimeoutError:
        return False
    except Exception:  # noqa: BLE001 - teardown must not raise over the real error
        pass
    return True


def _close_con(con: duckdb.DuckDBPyConnection) -> None:
    try:
        release_providers(con)
    except Exception:  # noqa: BLE001 - the connection is closed regardless
        logger.exception("releasing providers failed")
    con.close()


def _close_cursors(cursors: Sequence[duckdb.DuckDBPyConnection]) -> None:
    for cur in cursors:
        try:
            cur.close()
        except Exception:  # noqa: BLE001 - closed with `con` regardless
            pass


def _close_all(cursors: Sequence[duckdb.DuckDBPyConnection], con: duckdb.DuckDBPyConnection) -> None:
    _close_cursors(cursors)
    _close_con(con)


def _log_late_failure(task: "asyncio.Task[Any]") -> None:
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
) -> None:
    """Handshake, authenticate, then serve `ws` from `con` until either side drops; then close `con`.

    `con` arrives empty. `n6k_handshake` reads the client's FT_HELLO token on it, and only then
    does `authenticate(ws, con, token, **params)` attach what that identity may see and return the
    catalogs to serve: names already attached, `PendingCatalog`s attached while serving, and, with
    `resolve`, `LazyCatalog`s. While it runs past `HELLO_PENDING_FIRST_S`, the client gets
    FT_HELLO_PENDING carrying `hello_pending_message`. A `WsReject` or any other exception sends
    FT_HELLO_ERR (built by `n6k_reject`) and nothing is served; an empty list logs the client in with
    no catalogs. Returning `Authorized(catalogs, identity)` hands `identity` to text handlers.

    Ownership passes here: the serving CALL holds `con` for the connection's life. A caller that
    needs to reach the data meanwhile must take a `con.cursor()` inside `authenticate`.

    Once serving starts, `on_serve(ws, handle, **params)` (sync or async) gets a `LiveServe` for
    adding and removing catalogs; it is cancelled when the connection ends.

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

    `con` is closed off the event loop and only after every query on it returns; this returns without
    waiting for them, so a query that never returns leaves `con` open.
    """
    if on_text is not None and text_app is not None:
        raise ValueError("pass on_text or text_app, not both")
    if max_text_inflight is not None and max_text_inflight <= 0:
        raise ValueError(f"max_text_inflight must be None (no limit) or positive, got {max_text_inflight}")
    if on_text is not None:
        text_app = _OnTextApp(on_text, ws, params)
    host_sock, ext_sock = socket.socketpair()
    fd = ext_sock.fileno()
    stage: Optional[asyncio.Task[Any]] = None
    directions: tuple[asyncio.Task[None], ...] = ()
    settlers: list[asyncio.Task[None]] = []
    cursors: list[duckdb.DuckDBPyConnection] = []
    handle: Optional[LiveServe] = None
    writer: Optional[asyncio.StreamWriter] = None
    transport_owns_host_sock = False
    send_lock = asyncio.Lock()
    text = _TextChannel(ws, text_app, send_lock, max_text_inflight)
    resolver = _Resolver(resolve)
    close_code = 1000
    close_reason = ""
    try:
        reader, writer = await asyncio.open_connection(sock=host_sock)
        transport_owns_host_sock = True
        directions = _start_directions(ws, reader, writer, text, send_lock, resolver)

        stage = asyncio.create_task(asyncio.to_thread(_handshake, con, fd))
        if not await _stage_won(stage, *directions):
            return
        try:
            token, hello_capabilities = stage.result()
        except Exception as exc:  # noqa: BLE001
            logger.info("handshake failed: %s", exc)
            return

        # Its own cursor: `authenticate` may be blocked in a query on `con`.
        beat_cur = con.cursor()
        cursors.append(beat_cur)
        beat_stop = asyncio.Event()
        beat = asyncio.create_task(_beat_hello_pending(beat_cur, ws, send_lock, hello_pending_message, beat_stop))
        stage = asyncio.create_task(_authenticate(authenticate, ws, con, token, params))
        won = await _stage_won(stage, *directions)
        beat_stop.set()
        await asyncio.gather(beat, return_exceptions=True)
        if not won:
            return
        reason: Optional[str] = None
        catalogs: list[str] = []
        pending: list[PendingCatalog] = []
        lazy: dict[str, Optional[int]] = {}
        try:
            authorized = stage.result()
            catalogs, pending, lazy = _split_served(authorized.catalogs, resolve is not None)
        except WsReject as reject:
            reason = reject.reason or "unauthorized"
            close_code = reject.code
        except Exception:  # noqa: BLE001 - any failure refuses the client rather than serving it
            logger.exception("authenticate failed")
            reason = "internal error"
            close_code = 1011
        if reason is not None:
            close_reason = reason
            stage = asyncio.create_task(asyncio.to_thread(_reject, con, reason))
            if await _stage_won(stage, *directions):
                await _send_frame(ws, send_lock, stage.result())
                # Nothing serves the fd, so there is no output to drain.
                stage = None
            return
        stage = asyncio.create_task(asyncio.to_thread(_hello_ack, con, hello_capabilities, text.capabilities))
        if not await _stage_won(stage, *directions):
            return
        acked = await _send_frame(ws, send_lock, stage.result())
        stage = None
        if not acked:
            return
        text.authorize(params, authorized.identity, hello_capabilities)

        pending_cursors = [(p, con.cursor()) for p in pending]
        cursors.extend(cur for _, cur in pending_cursors)
        sql = build_serve_sql(
            fd,
            catalogs,
            ping_interval_ms,
            {p.name: p.message for p in pending},
            hello_capabilities,
            text.capabilities,
            hello_acked=True,
            resolve=resolve is not None,
            lazy=lazy,
        )
        # The CALL's thread starts at the next await, so cursors taken before then precede it.
        stage = asyncio.create_task(asyncio.to_thread(con.execute, sql))
        unsettled = _Unsettled([p.name for p in pending])
        settlers = [asyncio.create_task(_settle_pending(p, cur, fd, unsettled)) for p, cur in pending_cursors]
        if on_serve is not None or resolve is not None:
            handle = LiveServe(con.cursor(), fd, unsettled, text.connection)
            resolver.bind(handle)
        if on_serve is not None and handle is not None:
            settlers.append(asyncio.create_task(_run_on_serve(on_serve, ws, handle, params)))
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
        if handle is not None:
            handle._close()
        settlers.extend(text.close())
        settlers.extend(resolver.close())
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
        if handle is not None:
            await handle._close_upstreams(SERVE_JOIN_TIMEOUT_S)
        # A LiveServe call still running looks its reactor up by fd number; closing `ext_sock` would
        # free that number for another connection's socketpair.
        calls = await handle._join(SERVE_JOIN_TIMEOUT_S) if handle is not None else None
        if calls is None:
            ext_sock.close()
        else:
            calls.add_done_callback(lambda _: ext_sock.close())
        try:
            await ws.close(code=close_code, reason=close_reason[:123])
        except Exception:  # noqa: BLE001 - already closed by the peer, or mid-close
            pass
        late_cursors = [handle._cur] if handle is not None and calls is not None else []
        if handle is not None and calls is None:
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
                loop.run_in_executor(None, _close_all, [*cursors, *late_cursors], con)

            asyncio.gather(*busy, return_exceptions=True).add_done_callback(close_late)
        else:
            await loop.run_in_executor(None, _close_all, cursors, con)
