"""`register()` mounts `<prefix>/ws`, reads the client's FT_HELLO before the host builds
anything, and refuses through FT_HELLO_ERR."""

import asyncio
import concurrent.futures
import contextlib
import json
import socket
import struct
import threading
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any, Callable, Optional, Sequence

import duckdb
import pyarrow as pa
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_HELLO_PENDING,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_OPEN_PENDING,
    FT_PING,
    FT_PUSH,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    N6K_PROTOCOL_VERSION,
    OP_CATALOG_LIST,
    OP_CATALOGS_CHANGED,
    OP_QUERY,
)
from n6k_server import pump
from virtual_catalog.filters import Filters
from n6k_server.extension import load_n6k_server
from virtual_catalog.extension import load_vcatprovider
from virtual_catalog.provider import Provider, _catalogs, register_provider, release_providers
from n6k_server import context
from n6k_server.jsonrpc import JsonRpc, JsonRpcApp
from n6k_server.pump import Authorized, LazyCatalog, PendingCatalog, WsReject, build_serve_sql
from n6k_server.server_fastapi.register import register


def _never_connect(ws: Any, **_: Any) -> Any:
    raise AssertionError("connect should not run during registration")


def _never_authenticate(ws: Any, con: Any, token: str, **_: Any) -> list[str]:
    raise AssertionError("authenticate should not run during registration")


def _paths(app: FastAPI) -> list[str]:
    return [r.path for r in app.routes if hasattr(r, "path")]


def _register(app: FastAPI, prefix: str) -> None:
    register(app, prefix=prefix, connect=_never_connect, authenticate=_never_authenticate)


def test_prefix_must_not_end_with_slash():
    with pytest.raises(ValueError):
        _register(FastAPI(), "/foo/")


def test_non_empty_prefix_must_start_with_slash():
    with pytest.raises(ValueError):
        _register(FastAPI(), "foo")


def test_empty_prefix_mounts_at_root():
    app = FastAPI()
    _register(app, "")
    assert "/ws" in _paths(app)


def test_prefix_with_path_param():
    app = FastAPI()
    _register(app, "/mydb/{catalog}")
    assert "/mydb/{catalog}/ws" in _paths(app)


def test_connect_and_authenticate_are_required():
    with pytest.raises(TypeError):
        register(FastAPI(), prefix="/nope", connect=_never_connect)
    with pytest.raises(TypeError):
        register(FastAPI(), prefix="/nope", authenticate=_never_authenticate)


def test_a_negative_ping_interval_is_refused():
    with pytest.raises(ValueError, match="ping_interval_ms"):
        register(FastAPI(), "", connect=_never_connect, authenticate=_never_authenticate, ping_interval_ms=-1)


def test_a_non_positive_max_text_inflight_is_refused():
    with pytest.raises(ValueError, match="max_text_inflight"):
        register(FastAPI(), "", connect=_never_connect, authenticate=_never_authenticate, max_text_inflight=0)


def test_a_non_positive_max_mcp_inflight_is_refused():
    with pytest.raises(ValueError, match="max_mcp_inflight"):
        register(FastAPI(), "", connect=_never_connect, authenticate=_never_authenticate, max_mcp_inflight=0)


def test_a_non_positive_workers_is_refused():
    with pytest.raises(ValueError, match="workers"):
        register(FastAPI(), "", connect=_never_connect, authenticate=_never_authenticate, workers=0)


def test_a_non_positive_max_connections_is_refused():
    with pytest.raises(ValueError, match="max_connections"):
        register(FastAPI(), "", connect=_never_connect, authenticate=_never_authenticate, max_connections=0)


def test_two_registrations_are_independent():
    app = FastAPI()
    _register(app, "/a")
    _register(app, "/b")
    paths = _paths(app)
    assert "/a/ws" in paths
    assert "/b/ws" in paths


class _Host:
    def __init__(
        self,
        authenticate: Callable[..., Any],
        on_serve: Optional[Callable[..., Any]] = None,
        on_text: Optional[Callable[..., Any]] = None,
        resolve: Optional[Callable[..., Any]] = None,
        **text_apps: Any,
    ) -> None:
        self.events: list[str] = []
        self.cons: list[duckdb.DuckDBPyConnection] = []
        self._authenticate = authenticate
        self._on_serve = on_serve
        self._on_text = on_text
        self._resolve = resolve
        self._text_apps = text_apps

    async def connect(self, ws: Any) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        load_n6k_server(con)
        self.cons.append(con)
        try:
            yield con
        finally:
            self.events.append("teardown")

    def authenticate(self, ws: Any, con: duckdb.DuckDBPyConnection, token: str) -> Any:
        self.events.append(f"authenticate:{token}")
        return self._authenticate(con, token)

    def app(self) -> FastAPI:
        app = FastAPI()
        register(
            app,
            prefix="",
            connect=self.connect,
            authenticate=self.authenticate,
            on_serve=self._on_serve,
            on_text=self._on_text,
            resolve=self._resolve,
            **self._text_apps,
        )
        return app

    def wait_for(self, event: str) -> None:
        deadline = time.monotonic() + 10
        while event not in self.events:
            assert time.monotonic() < deadline, self.events
            time.sleep(0.02)


def _attach_db(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
    con.execute("ATTACH ':memory:' AS db")
    return ["db"]


def _recv(ws: Any) -> dict[str, Any]:
    header, _ = unpack_frame(ws.receive_bytes())
    return dict(header)


def _expect_disconnect(ws: Any) -> int:
    with pytest.raises(WebSocketDisconnect) as excinfo:
        while True:
            ws.receive_bytes()
    return excinfo.value.code


def test_authenticate_gets_the_hello_token_and_connects_con():
    host = _Host(_attach_db)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok-1", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ACK
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "db"}))
        assert _recv(ws)["t"] == FT_OPEN_ACK
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok-1", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ERR
        _expect_disconnect(ws)
        host.wait_for("teardown")
    assert "authenticate:tok-1" in host.events
    assert len(host.cons) == 1


def test_ws_reject_sends_its_reason_and_close_code():
    def reject(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        raise WsReject(4403, "not you")

    host = _Host(reject)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
        err = _recv(ws)
        assert err["t"] == FT_HELLO_ERR
        assert "not you" in err["exception_message"]
        assert _expect_disconnect(ws) == 4403


def test_any_other_exception_is_a_hello_err():
    def boom(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        raise RuntimeError("secret detail")

    host = _Host(boom)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
        err = _recv(ws)
        assert err["t"] == FT_HELLO_ERR
        assert err["exception_message"].endswith("internal error")
        assert "secret detail" not in err["exception_message"]


def test_an_empty_catalog_list_logs_in_with_nothing_served():
    host = _Host(lambda con, token: [])
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ACK
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "memory"}))
        frame = _recv_skipping_pings(ws)
        assert frame["t"] == FT_OPEN_ERR
        assert "not served" in frame["exception_message"]
        _end_serve(ws, host)


def test_a_text_only_login_answers_jsonrpc_and_can_gain_a_catalog_later():
    api = JsonRpc()

    @api.method()
    def ping() -> str:
        return "pong"

    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        cur.execute("ATTACH ':memory:' AS extra")

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        while "add" not in host.events:
            await asyncio.sleep(0.01)
        await handle.add_catalog(PendingCatalog("extra", attach))

    host = _Host(lambda con, token: [], on_serve, jsonrpc=api)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(
            pack_frame(
                {"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": ["jsonrpc"]}
            )
        )
        assert _recv(ws)["t"] == FT_HELLO_ACK
        _call(ws, 1, "ping")
        assert json.loads(_recv_text(ws))["result"] == "pong"
        host.events.append("add")
        assert _recv_catalogs_changed(ws) == ["extra"]
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "extra"}))
        assert _recv_skipping_pings(ws)["t"] == FT_OPEN_ACK
        _end_serve(ws, host)


def test_a_failed_handshake_never_reaches_authenticate():
    host = _Host(_attach_db)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_PING, "id": 1}))
        err = _recv(ws)
        assert err["t"] == FT_HELLO_ERR
        assert "first frame must be FT_HELLO" in err["exception_message"]
    host.wait_for("teardown")
    assert not any(e.startswith("authenticate") for e in host.events)


def test_a_drop_mid_authenticate_closes_con_only_after_authenticate_returns():
    async def slow(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        await asyncio.sleep(0.5)
        host.events.append("authenticate-done")
        return _attach_db(con, token)

    host = _Host(slow)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
        host.wait_for("authenticate:x")
        ws.close()
        host.wait_for("teardown")
    assert host.events.index("authenticate-done") < host.events.index("teardown")
    with pytest.raises(duckdb.ConnectionException):
        host.cons[0].execute("SELECT 1")


def test_an_authenticate_outliving_the_join_timeout_still_closes_con(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pump, "SERVE_JOIN_TIMEOUT_S", 0.05)

    async def slow(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        await asyncio.sleep(0.5)
        host.events.append("authenticate-done")
        return _attach_db(con, token)

    host = _Host(slow)
    with TestClient(host.app()) as client:
        with client.websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
            host.wait_for("authenticate:x")
            ws.close()
            host.wait_for("teardown")
        assert "authenticate-done" not in host.events
        host.wait_for("authenticate-done")
        deadline = time.monotonic() + 10
        while True:
            try:
                host.cons[0].execute("SELECT 1")
            except duckdb.ConnectionException:
                break
            assert time.monotonic() < deadline
            time.sleep(0.02)


def test_a_serve_cancelled_mid_call_still_ends_the_call():
    # TestClient's exit cancels the app task while CALL n6k_serve_fd is running; anyio re-cancels
    # every await in the pump's teardown, so the host's own con.close() is all that is left.
    closed = threading.Event()

    async def connect(ws: Any) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        load_n6k_server(con)
        try:
            yield con
        finally:
            con.close()
            closed.set()

    def serve() -> None:
        app = FastAPI()
        register(app, prefix="", connect=connect, authenticate=lambda ws, con, token: _attach_db(con, token))
        with pytest.raises(concurrent.futures.CancelledError):
            with TestClient(app).websocket_connect("/ws") as ws:
                ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
                assert _recv(ws)["t"] == FT_HELLO_ACK

    threading.Thread(target=serve, daemon=True).start()
    assert closed.wait(15)


class _SharedHost(_Host):
    def __init__(self) -> None:
        super().__init__(self._attach_shared)
        self.db = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        load_n6k_server(self.db)

    @staticmethod
    def _attach_shared(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        con.execute("ATTACH IF NOT EXISTS ':memory:' AS db")
        return ["db"]

    async def connect(self, ws: Any) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        con = self.db.cursor()
        self.cons.append(con)
        try:
            yield con
        finally:
            self.events.append("teardown")


def test_one_database_serves_several_sockets():
    host = _SharedHost()
    with TestClient(host.app()) as client:
        with client.websocket_connect("/ws") as first, client.websocket_connect("/ws") as second:
            for ws in (first, second):
                ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
                assert _recv(ws)["t"] == FT_HELLO_ACK
                ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "db"}))
                assert _recv(ws)["t"] == FT_OPEN_ACK
            first.close()
            second.close()
            host.wait_for("teardown")
        with client.websocket_connect("/ws") as third:
            third.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(third)["t"] == FT_HELLO_ACK
            third.close()


class _EmptyProvider(Provider):
    async def list_tables(self) -> list[str]:
        return []

    async def schema(self, name: str) -> pa.Schema:
        raise KeyError(name)

    async def scan(self, name: str, columns: Optional[list[str]], filters: Filters) -> pa.Table:
        raise KeyError(name)


def _attach_provider(con: duckdb.DuckDBPyConnection, providers: list[Provider]) -> list[str]:
    load_vcatprovider(con)
    con.execute("ATTACH ':memory:' AS db (TYPE vcatprovider)")
    provider = _EmptyProvider()
    register_provider(con, catalog="db", schema_name="main", provider=provider)
    providers.append(provider)
    return ["db"]


def test_closing_a_socket_releases_the_providers_it_registered():
    providers: list[Provider] = []

    async def with_provider(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        return _attach_provider(con, providers)

    host = _Host(with_provider, on_close=release_providers)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ACK
        assert host.cons[0] in _catalogs
        ws.close()
        host.wait_for("teardown")
    assert host.cons[0] not in _catalogs
    assert providers[0]._binding is None


def test_a_socket_closed_after_the_join_timeout_still_releases_its_providers(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(pump, "SERVE_JOIN_TIMEOUT_S", 0.05)
    providers: list[Provider] = []

    async def slow(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        await asyncio.sleep(0.5)
        result = _attach_provider(con, providers)
        host.events.append("authenticate-done")
        return result

    host = _Host(slow, on_close=release_providers)
    with TestClient(host.app()) as client:
        with client.websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
            host.wait_for("authenticate:x")
            ws.close()
            host.wait_for("teardown")
        host.wait_for("authenticate-done")
        deadline = time.monotonic() + 10
        while host.cons[0] in _catalogs:
            assert time.monotonic() < deadline
            time.sleep(0.02)
    assert providers[0]._binding is None


class _SlowListProvider(_EmptyProvider):
    def __init__(self) -> None:
        self.entered = threading.Event()

    async def list_tables(self) -> list[str]:
        self.entered.set()
        await asyncio.sleep(0.5)
        return []


def test_closing_a_socket_while_a_provider_query_waits_on_the_loop_does_not_deadlock():
    provider = _SlowListProvider()

    def with_provider(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        load_vcatprovider(con)
        con.execute("ATTACH ':memory:' AS db (TYPE vcatprovider)")
        register_provider(con, catalog="db", schema_name="main", provider=provider)
        return ["db"]

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        cur = host.cons[0].cursor()
        asyncio.ensure_future(
            asyncio.to_thread(lambda: cur.execute("SELECT schema_name FROM duckdb_schemas()").fetchall())
        )

    host = _Host(with_provider, on_serve, on_close=release_providers)

    def run() -> None:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(ws)["t"] == FT_HELLO_ACK
            assert provider.entered.wait(10)
            ws.close()
            host.wait_for("teardown")

    # A deadlocked loop also hangs TestClient's exit, so only a thread outside it can time out.
    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    runner.join(timeout=20)
    assert not runner.is_alive()
    assert host.cons[0] not in _catalogs


def _recv_skipping_pings(ws: Any) -> dict[str, Any]:
    while True:
        frame = _recv(ws)
        if frame["t"] != FT_PING:
            return frame


def _hello_and_open(ws: Any, catalog: str, capabilities: Sequence[str] = ()) -> None:
    hello: dict[str, Any] = {"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}
    if capabilities:
        hello["capabilities"] = list(capabilities)
    ws.send_bytes(pack_frame(hello))
    assert _recv(ws)["t"] == FT_HELLO_ACK
    ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": catalog}))


def _end_serve(ws: Any, host: _Host) -> None:
    ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
    _expect_disconnect(ws)
    host.wait_for("teardown")


def _pending(attach: Callable[[duckdb.DuckDBPyConnection], Any], name: str = "late") -> Callable[..., Any]:
    return lambda con, token: [PendingCatalog(name, attach, message="warming up")]


def test_a_pending_catalog_answers_open_pending_until_attach_returns_then_open_ack():
    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        await asyncio.sleep(0.3)
        cur.execute("ATTACH ':memory:' AS late")

    host = _Host(_pending(attach))
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "late")
        pending = _recv_skipping_pings(ws)
        assert pending == {"t": FT_OPEN_PENDING, "ns": 1, "message": "warming up"}
        while (frame := _recv_skipping_pings(ws))["t"] == FT_OPEN_PENDING:
            pass
        assert frame["t"] == FT_OPEN_ACK
        assert frame["ns"] == 1
        _end_serve(ws, host)


def test_a_failing_attach_answers_open_err_with_its_message():
    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        raise RuntimeError("worker never came up")

    host = _Host(_pending(attach))
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "late")
        # The failure's CATALOGS_CHANGED push races the OPEN, so it may land anywhere here.
        while (frame := _recv_skipping_pings(ws))["t"] in (FT_OPEN_PENDING, FT_PUSH):
            pass
        assert frame["t"] == FT_OPEN_ERR
        assert "worker never came up" in frame["exception_message"]
        # A later OPEN of the failed catalog is refused at once.
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 2, "catalog": "late"}))
        while (frame := _recv_skipping_pings(ws))["t"] == FT_PUSH:
            pass
        assert frame["t"] == FT_OPEN_ERR
        assert "worker never came up" in frame["exception_message"]
        _end_serve(ws, host)


def test_an_attach_that_returns_without_attaching_answers_open_err():
    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        return None

    host = _Host(_pending(attach))
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "late")
        while (frame := _recv_skipping_pings(ws))["t"] in (FT_OPEN_PENDING, FT_PUSH):
            pass
        assert frame["t"] == FT_OPEN_ERR
        assert "not attached" in frame["exception_message"]
        _end_serve(ws, host)


def test_a_pending_catalog_removed_while_attaching_is_never_settled(caplog: pytest.LogCaptureFixture):
    attaching = threading.Event()
    removed = threading.Event()

    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        attaching.set()
        while not removed.is_set():
            await asyncio.sleep(0.01)
        cur.execute("ATTACH ':memory:' AS late")

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        while not attaching.is_set():
            await asyncio.sleep(0.01)
        await handle.remove_catalog("late")
        removed.set()

    host = _Host(_pending(attach), on_serve)
    with caplog.at_level("INFO", logger="n6k_server.pump"):
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(ws)["t"] == FT_HELLO_ACK
            assert _recv_catalogs_changed(ws) == []
            deadline = time.monotonic() + 5
            while "was removed before it settled" not in caplog.text:
                assert time.monotonic() < deadline, caplog.text
                time.sleep(0.02)
            _end_serve(ws, host)
    assert "Traceback" not in caplog.text


def test_an_attach_past_its_timeout_answers_open_err():
    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        await asyncio.sleep(3600)

    host = _Host(lambda con, token: [PendingCatalog("late", attach, timeout=0.2)])
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "late")
        while (frame := _recv_skipping_pings(ws))["t"] == FT_OPEN_PENDING:
            pass
        assert frame["t"] == FT_OPEN_ERR
        assert "did not become ready within 0.2s" in frame["exception_message"]
        _end_serve(ws, host)


def test_a_slow_authenticate_answers_hello_pending_until_hello_ack():
    async def authenticate(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        await asyncio.sleep(1.5)
        con.execute("ATTACH ':memory:' AS db")
        return ["db"]

    host = _Host(authenticate)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws) == {"t": FT_HELLO_PENDING, "message": "authenticating"}
        while (frame := _recv(ws))["t"] == FT_HELLO_PENDING:
            pass
        assert frame["t"] == FT_HELLO_ACK
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "db"}))
        assert _recv_skipping_pings(ws)["t"] == FT_OPEN_ACK
        _end_serve(ws, host)


def test_a_fast_authenticate_sends_no_hello_pending():
    host = _Host(_attach_db)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ACK
        _end_serve(ws, host)


def test_a_catalog_returned_twice_is_a_rejection():
    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        return None

    def authenticate(con: duckdb.DuckDBPyConnection, token: str) -> list[Any]:
        con.execute("ATTACH ':memory:' AS db")
        return ["db", PendingCatalog("db", attach)]

    host = _Host(authenticate)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ERR


_SESSION_ID = uuid.UUID("00000000-0000-4000-8000-000000000007")


def test_build_serve_sql_renders_the_callbacks():
    sql = build_serve_sql(_SESSION_ID, ["db"], authenticate="auth", resolve="res", hello_pending_message="it's warming")
    assert sql == (
        f"CALL n6k_serve_fd('{_SESSION_ID}'::UUID, 'db', authenticate => 'auth', resolve => 'res', "
        "hello_pending_message => 'it''s warming')"
    )


def test_build_serve_sql_renders_workers():
    assert build_serve_sql(_SESSION_ID, ["db"], workers=2) == (
        f"CALL n6k_serve_fd('{_SESSION_ID}'::UUID, 'db', workers => 2)"
    )


def test_a_connection_past_max_connections_is_closed_try_again_later():
    host = _Host(_attach_db)
    app = FastAPI()
    register(app, "", connect=host.connect, authenticate=host.authenticate, max_connections=1)
    hello = pack_frame({"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION})
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as first:
            first.send_bytes(hello)
            assert _recv(first)["t"] == FT_HELLO_ACK
            with client.websocket_connect("/ws") as second:
                assert _expect_disconnect(second) == 1013
            first.close()
            host.wait_for("teardown")
        # The slot frees just after the host's teardown, so the first retry may still be refused.
        host.events.clear()
        deadline = time.monotonic() + 10
        while True:
            with client.websocket_connect("/ws") as third:
                third.send_bytes(hello)
                try:
                    assert _recv(third)["t"] == FT_HELLO_ACK
                except WebSocketDisconnect as refused:
                    assert refused.code == 1013 and time.monotonic() < deadline
                    continue
                third.close()
                host.wait_for("teardown")
                break


def _recv_catalogs_changed(ws: Any) -> list[str]:
    while True:
        frame = _recv_skipping_pings(ws)
        if frame["t"] == FT_PUSH and frame.get("op") == OP_CATALOGS_CHANGED:
            return list(frame["catalogs"])


def test_on_serve_adds_a_catalog_the_client_can_open():
    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        cur.execute("ATTACH ':memory:' AS extra")

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        await handle.add_catalog(PendingCatalog("extra", attach))

    host = _Host(_attach_db, on_serve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ACK
        assert _recv_catalogs_changed(ws) == ["db", "extra"]
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "extra"}))
        assert _recv_skipping_pings(ws)["t"] == FT_OPEN_ACK
        _end_serve(ws, host)


def _recv_open_reply(ws: Any) -> tuple[dict[str, Any], int]:
    """The OPEN_ACK or OPEN_ERR, and how many OPEN_PENDINGs came first."""
    pendings = 0
    while True:
        frame = _recv_skipping_pings(ws)
        if frame["t"] == FT_OPEN_PENDING:
            pendings += 1
        elif frame["t"] in (FT_OPEN_ACK, FT_OPEN_ERR):
            return frame, pendings


def test_resolve_attaches_an_unknown_catalog_lazily():
    names: list[str] = []

    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        cur.execute("ATTACH ':memory:' AS lazy")

    async def resolve(handle: pump.LiveServe, name: str) -> None:
        names.append(name)
        await handle.add_catalog(PendingCatalog(name, attach))

    host = _Host(_attach_db, resolve=resolve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "lazy")
        frame, _ = _recv_open_reply(ws)
        assert frame["t"] == FT_OPEN_ACK
        assert names == ["lazy"]
        _end_serve(ws, host)


def test_resolve_that_serves_nothing_is_not_found():
    def resolve(handle: pump.LiveServe, name: str) -> None:
        pass

    host = _Host(_attach_db, resolve=resolve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "nope")
        frame, _ = _recv_open_reply(ws)
        assert frame["t"] == FT_OPEN_ERR
        assert "not found" in frame["exception_message"]
        _end_serve(ws, host)


def test_resolve_that_raises_fails_the_open_with_its_message():
    async def resolve(handle: pump.LiveServe, name: str) -> None:
        raise RuntimeError("postgres is down")

    host = _Host(_attach_db, resolve=resolve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "pg")
        frame, _ = _recv_open_reply(ws)
        assert frame["t"] == FT_OPEN_ERR
        assert "postgres is down" in frame["exception_message"]
        _end_serve(ws, host)


def test_a_slow_resolve_keeps_the_open_pending():
    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        cur.execute("ATTACH ':memory:' AS slow")

    async def resolve(handle: pump.LiveServe, name: str) -> None:
        await asyncio.sleep(2.5)
        await handle.add_catalog(PendingCatalog(name, attach))

    host = _Host(_attach_db, resolve=resolve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "slow")
        frame, pendings = _recv_open_reply(ws)
        assert frame["t"] == FT_OPEN_ACK
        assert pendings >= 2
        _end_serve(ws, host)


def test_resolve_runs_with_the_connection_as_current():
    seen: list[Any] = []

    def authenticate(con: duckdb.DuckDBPyConnection, token: str) -> Authorized:
        return Authorized([], identity="user-1")

    def resolve(handle: pump.LiveServe, name: str) -> None:
        seen.append(context.current().identity)

    host = _Host(authenticate, resolve=resolve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "x")
        _recv_open_reply(ws)
        assert seen == ["user-1"]
        _end_serve(ws, host)


def test_on_serve_removes_a_catalog_and_the_handle_dies_with_the_connection():
    handles: list[pump.LiveServe] = []

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        handles.append(handle)
        await handle.remove_catalog("db")

    host = _Host(_attach_db, on_serve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ACK
        assert _recv_catalogs_changed(ws) == []
        ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "db"}))
        frame = _recv_skipping_pings(ws)
        assert frame["t"] == FT_OPEN_ERR
        assert "not served" in frame["exception_message"]
        _end_serve(ws, host)
    with pytest.raises(RuntimeError, match="no longer served"):
        asyncio.run(handles[0].add_catalog("db"))


def test_paused_swaps_a_catalog_under_an_open_session():
    swap = threading.Event()
    cursors: list[duckdb.DuckDBPyConnection] = []

    def authenticate(con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        cursors.append(con.cursor())
        return _attach_db(con, token)

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        while not swap.is_set():
            await asyncio.sleep(0.01)
        async with handle.paused("db"):
            cursors[0].execute("DETACH db")
            cursors[0].execute("ATTACH ':memory:' AS db")
            cursors[0].execute("CREATE TABLE db.main.fresh AS SELECT 1 AS v")
        host.events.append("swapped")

    def query(ws: Any, req_id: int) -> dict[str, Any]:
        ws.send_bytes(pack_frame({"t": FT_REQ, "id": req_id, "ns": 1, "op": OP_QUERY, "sql": "FROM db.main.fresh"}))
        while True:
            frame = _recv_skipping_pings(ws)
            if frame["t"] in (FT_RESP_END, FT_ERR) and frame.get("id") == req_id:
                return frame

    host = _Host(authenticate, on_serve)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open(ws, "db")
        assert query(ws, 1)["t"] == FT_ERR
        swap.set()
        host.wait_for("swapped")
        assert query(ws, 2)["t"] == FT_RESP_END
        _end_serve(ws, host)


def _query(ws: Any, req_id: int, sql: str = "SELECT 1") -> dict[str, Any]:
    ws.send_bytes(pack_frame({"t": FT_REQ, "id": req_id, "ns": 1, "op": OP_QUERY, "sql": sql}))
    while True:
        frame = _recv_skipping_pings(ws)
        if frame["t"] in (FT_RESP_END, FT_ERR) and frame.get("id") == req_id:
            return frame


def _recv_text(ws: Any) -> str:
    while True:
        message = ws.receive()
        if message.get("text") is not None:
            return str(message["text"])


def _hello_and_open_ack(ws: Any, capabilities: Sequence[str] = ("jsonrpc",)) -> None:
    _hello_and_open(ws, "db", capabilities)
    assert _recv_skipping_pings(ws)["t"] == FT_OPEN_ACK


def test_a_text_frame_does_not_end_the_session():
    host = _Host(_attach_db)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        ws.send_text("hello")
        assert _query(ws, 1)["t"] == FT_RESP_END
        _end_serve(ws, host)


def test_a_jsonrpc_request_without_a_text_app_gets_method_not_found():
    host = _Host(_attach_db)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        ws.send_text(json.dumps({"jsonrpc": "2.0", "method": "missing"}))
        ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 7, "method": "missing"}))
        assert json.loads(_recv_text(ws)) == {
            "jsonrpc": "2.0",
            "id": 7,
            "error": {"code": -32601, "message": "Method not found"},
        }
        assert _query(ws, 1)["t"] == FT_RESP_END
        _end_serve(ws, host)


def test_text_after_hello_reaches_on_text_and_its_reply_arrives_while_queries_work():
    async def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
        await send_text(f"echo:{text}")

    host = _Host(_attach_db, on_text=on_text)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        ws.send_text("ping")
        ws.send_bytes(pack_frame({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 1"}))
        texts: list[str] = []
        ended = False
        while not (texts and ended):
            message = ws.receive()
            if message.get("text") is not None:
                texts.append(message["text"])
                continue
            frame = dict(unpack_frame(message["bytes"])[0])
            ended = ended or (frame["t"] == FT_RESP_END and frame.get("id") == 1)
        assert texts == ["echo:ping"]
        assert _query(ws, 2)["t"] == FT_RESP_END
        _end_serve(ws, host)


def test_text_before_hello_is_dropped():
    def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
        host.events.append(f"text:{text}")

    host = _Host(_attach_db, on_text=on_text)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_text("early")
        _hello_and_open_ack(ws)
        ws.send_text("late")
        host.wait_for("text:late")
        assert "text:early" not in host.events
        _end_serve(ws, host)


class _TextWs:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)


def test_send_text_refuses_a_client_that_did_not_offer_jsonrpc_or_mcp():
    async def run() -> None:
        async def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
            await send_text("reply")

        ws = _TextWs()
        channel = pump._TextChannel(ws, pump._OnTextApp(on_text, ws, {}), asyncio.Lock())  # type: ignore[arg-type]
        channel.authorize({}, None, ["crossing"])
        with pytest.raises(RuntimeError, match="not granted jsonrpc or mcp"):
            await channel.send_text("unprompted")
        assert not await channel.notify("early")
        assert channel.receive("hi")
        await asyncio.gather(*channel._tasks)
        assert ws.sent == []

    asyncio.run(run())


def test_text_from_a_client_that_offered_no_text_capability_never_reaches_on_text():
    async def run() -> list[str]:
        seen: list[str] = []

        def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
            seen.append(text)

        ws = _TextWs()
        channel = pump._TextChannel(ws, pump._OnTextApp(on_text, ws, {}), asyncio.Lock())  # type: ignore[arg-type]
        channel.authorize({}, None, ["crossing"])
        assert channel.receive("hi")
        await asyncio.gather(*channel._tasks)
        return seen

    assert asyncio.run(run()) == []


def test_send_text_refuses_a_client_offering_only_a_capability_the_app_does_not_serve():
    async def run() -> None:
        ws = _TextWs()
        channel = pump._TextChannel(ws, JsonRpcApp(jsonrpc=JsonRpc()), asyncio.Lock())  # type: ignore[arg-type]
        channel.authorize({}, None, ["crossing", "mcp"])
        with pytest.raises(RuntimeError, match="not granted jsonrpc or mcp"):
            await channel.send_text("unprompted")
        assert not await channel.notify("early")
        assert ws.sent == []

    asyncio.run(run())


def test_held_mcp_notifications_go_out_before_one_sent_while_they_flush():
    from n6k_server.mcp import McpSession

    async def run() -> list[str]:
        sent: list[str] = []

        async def send_text(text: str) -> None:
            await asyncio.sleep(0)
            sent.append(text)

        session = McpSession(_mcp_server())
        session.feed(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "t", "version": "0"},
                    },
                }
            ),
            send_text,
        )
        await session.notify("A", send_text)
        await session.notify("B", send_text)
        session.feed(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}), send_text)
        await session.notify("C", send_text)
        while session._handshaking:
            await asyncio.sleep(0)
        await session.notify("D", send_text)
        await session.aclose()
        return [text for text in sent if text in {"A", "B", "C", "D"}]

    assert asyncio.run(run()) == ["A", "B", "C", "D"]


_INITIALIZE = json.dumps(
    {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}},
    }
)


class _ClosingTextWs(_TextWs):
    def __init__(self) -> None:
        super().__init__()
        self.closed: Optional[int] = None

    async def close(self, code: int, reason: str = "") -> None:
        self.closed = code


def test_notifications_held_past_the_cap_for_an_unfinished_mcp_handshake_close_the_socket():
    from n6k_server import mcp

    async def run() -> tuple[int, Optional[int]]:
        ws = _ClosingTextWs()
        channel = pump._TextChannel(ws, JsonRpcApp(mcp=_mcp_server()), asyncio.Lock())  # type: ignore[arg-type]
        channel.authorize({}, None, ["mcp"])
        assert channel.receive(_INITIALIZE)
        await asyncio.gather(*channel._tasks)
        sent = 0
        while await channel.notify("page/refresh"):
            sent += 1
        await channel.aclose_session()
        return sent, ws.closed

    assert asyncio.run(run()) == (mcp._MAX_HELD, 1008)


def _ping(req_id: int) -> str:
    return json.dumps({"jsonrpc": "2.0", "id": req_id, "method": "ping"})


def test_mcp_requests_past_max_inflight_answer_busy_until_one_is_answered_or_cancelled():
    from n6k_server.mcp import McpSession

    async def run() -> None:
        sent: list[dict[str, Any]] = []

        async def send_text(text: str) -> None:
            sent.append(json.loads(text))

        session = McpSession(_mcp_server(), max_inflight=1)
        assert session.feed(_ping(1), send_text) == ""
        assert json.loads(session.feed(_ping(2), send_text)) == {
            "jsonrpc": "2.0",
            "id": 2,
            "error": {"code": -32000, "message": "server busy"},
        }
        while not any(m.get("id") == 1 for m in sent):
            await asyncio.sleep(0)
        assert session.feed(_ping(3), send_text) == ""
        session.feed(
            json.dumps({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 3}}), send_text
        )
        assert session.feed(_ping(4), send_text) == ""
        await session.aclose()

    asyncio.run(run())


def test_an_invalid_mcp_request_answers_invalid_request_and_an_invalid_notification_nothing():
    from n6k_server.mcp import McpSession

    async def send_text(text: str) -> None:
        pass

    async def run() -> None:
        session = McpSession(_mcp_server())
        assert json.loads(session.feed(json.dumps({"jsonrpc": "2.0", "id": 7, "method": 5}), send_text)) == {
            "jsonrpc": "2.0",
            "id": 7,
            "error": {"code": -32600, "message": "invalid request"},
        }
        assert session.feed(json.dumps({"jsonrpc": "1.0", "method": "x"}), send_text) == ""
        await session.aclose()

    asyncio.run(run())


def test_send_text_reaches_a_client_that_offered_jsonrpc_before_it_sends_text_and_refuses_after_close():
    async def run() -> list[str]:
        ws = _TextWs()
        channel = pump._TextChannel(ws, pump._OnTextApp(lambda *_: None, ws, {}), asyncio.Lock())  # type: ignore[arg-type]
        channel.authorize({}, None, ["crossing", "jsonrpc"])
        await channel.send_text("first")
        assert await channel.notify("ping", {"n": 1})
        channel.close()
        with pytest.raises(RuntimeError, match="no longer served"):
            await channel.send_text("too late")
        assert not await channel.notify("late")
        return ws.sent

    assert asyncio.run(run()) == ["first", '{"jsonrpc": "2.0", "method": "ping", "params": {"n": 1}}']


def test_a_slow_on_text_does_not_block_queries_and_is_cancelled_at_close():
    async def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
        host.events.append("started")
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            host.events.append("cancelled")
            raise

    host = _Host(_attach_db, on_text=on_text)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        ws.send_text("slow")
        host.wait_for("started")
        assert _query(ws, 1)["t"] == FT_RESP_END
        _end_serve(ws, host)
    assert "cancelled" in host.events


def test_a_failing_on_text_leaves_the_session_up():
    async def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
        if text == "boom":
            raise ValueError("boom")
        await send_text(f"echo:{text}")

    host = _Host(_attach_db, on_text=on_text)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        ws.send_text("boom")
        ws.send_text("ok")
        assert _recv_text(ws) == "echo:ok"
        assert _query(ws, 1)["t"] == FT_RESP_END
        _end_serve(ws, host)


def test_too_many_text_messages_in_flight_closes_the_socket():
    async def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
        host.events.append(f"text:{text}")
        await asyncio.sleep(60)

    host = _Host(_attach_db, on_text=on_text, max_text_inflight=2)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        ws.send_text("1")
        ws.send_text("2")
        host.wait_for("text:2")
        ws.send_text("3")
        assert _expect_disconnect(ws) == 1008
        host.wait_for("teardown")
    assert "text:3" not in host.events


def test_without_max_text_inflight_text_handlers_are_not_limited():
    async def on_text(ws: Any, text: str, send_text: pump.SendText) -> None:
        host.events.append(f"text:{text}")
        await asyncio.sleep(60)

    host = _Host(_attach_db, on_text=on_text)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        for i in range(40):
            ws.send_text(str(i))
        host.wait_for("text:39")
        assert _query(ws, 1)["t"] == FT_RESP_END
        _end_serve(ws, host)


def _hello_with_caps(ws: Any) -> list[str]:
    ws.send_bytes(
        pack_frame(
            {
                "t": FT_HELLO,
                "token": "tok",
                "protocol_version": N6K_PROTOCOL_VERSION,
                "capabilities": ["crossing", "jsonrpc", "mcp"],
            }
        )
    )
    ack = _recv(ws)
    assert ack["t"] == FT_HELLO_ACK
    return list(ack["capabilities"])


def _mcp_server() -> Any:
    from mcp.server.mcpserver import MCPServer

    server = MCPServer("pages")

    @server.tool()
    def whoami(greeting: str) -> str:
        """Greet the connection's identity."""
        return f"{greeting} {context.current().identity}"

    return server


@pytest.mark.parametrize(
    "apps, granted",
    [
        ({}, ["crossing"]),
        ({"jsonrpc": JsonRpc()}, ["crossing", "jsonrpc"]),
        ({"mcp": "server"}, ["crossing", "jsonrpc", "mcp"]),
        ({"on_text": lambda *a: None}, ["crossing", "jsonrpc", "mcp"]),
    ],
)
def test_hello_ack_grants_jsonrpc_and_mcp_only_when_served(apps: dict[str, Any], granted: list[str]):
    if apps.get("mcp") == "server":
        apps = {"mcp": _mcp_server()}
    host = _Host(_attach_db, **apps)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        assert _hello_with_caps(ws) == granted
        _end_serve(ws, host)


def test_a_jsonrpc_call_sees_the_identity_while_queries_work():
    api = JsonRpc()

    @api.method()
    async def whoami(greeting: str) -> str:
        return f"{greeting} {context.current().identity}"

    host = _Host(lambda con, token: Authorized(_attach_db(con, token), identity="sean"), jsonrpc=api)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "whoami", "params": {"greeting": "hi"}}))
        assert json.loads(_recv_text(ws)) == {"jsonrpc": "2.0", "id": 1, "result": "hi sean"}
        ws.send_text(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "nope"}))
        assert json.loads(_recv_text(ws))["error"]["code"] == -32601
        assert _query(ws, 1)["t"] == FT_RESP_END
        _end_serve(ws, host)


def test_an_mcp_tool_call_sees_the_identity():
    host = _Host(lambda con, token: Authorized(_attach_db(con, token), identity="sean"), mcp=_mcp_server())

    def request(req_id: int, method: str, params: dict[str, Any]) -> dict[str, Any]:
        ws.send_text(json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}))
        reply = json.loads(_recv_text(ws))
        assert reply["id"] == req_id, reply
        return dict(reply["result"])

    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        init = request(
            1,
            "initialize",
            {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}},
        )
        assert init["serverInfo"]["name"] == "pages"
        ws.send_text(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        tools = request(2, "tools/list", {})["tools"]
        assert [t["name"] for t in tools] == ["whoami"]
        assert tools[0]["inputSchema"]["properties"]["greeting"]["type"] == "string"
        result = request(3, "tools/call", {"name": "whoami", "arguments": {"greeting": "hi"}})
        assert result["content"][0]["text"] == "hi sean"
        assert _query(ws, 1)["t"] == FT_RESP_END
        _end_serve(ws, host)


def test_on_text_with_jsonrpc_is_refused():
    with pytest.raises(ValueError, match="not both"):
        register(
            FastAPI(),
            "",
            connect=_never_connect,
            authenticate=_never_authenticate,
            on_text=lambda *a: None,
            jsonrpc=JsonRpc(),
        )


def _call(ws: Any, req_id: int, method: str, params: Any = None) -> None:
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        message["params"] = params
    ws.send_text(json.dumps(message))


def test_a_result_that_is_not_json_answers_internal_error():
    api = JsonRpc()

    @api.method()
    def when() -> object:
        return object()

    host = _Host(_attach_db, jsonrpc=api)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        _call(ws, 1, "when")
        assert json.loads(_recv_text(ws))["error"]["code"] == -32603
        _end_serve(ws, host)


def test_over_the_inflight_cap_a_call_answers_busy_and_the_socket_stays():
    release = asyncio.Event()
    api = JsonRpc()

    @api.method()
    async def wait() -> str:
        host.events.append("waiting")
        while not release.is_set():
            await asyncio.sleep(0.01)
        return "done"

    @api.method()
    async def go() -> None:
        release.set()

    host = _Host(_attach_db, jsonrpc=api, max_text_inflight=2)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        _call(ws, 1, "wait")
        _call(ws, 2, "wait")
        while host.events.count("waiting") < 2:
            time.sleep(0.01)
        _call(ws, 3, "go")
        assert json.loads(_recv_text(ws)) == {
            "jsonrpc": "2.0",
            "id": 3,
            "error": {"code": -32000, "message": "server busy"},
        }
        assert _query(ws, 1)["t"] == FT_RESP_END
        release.set()
        assert sorted(json.loads(_recv_text(ws))["id"] for _ in range(2)) == [1, 2]
        _end_serve(ws, host)


def test_notify_reaches_a_jsonrpc_client_from_a_kept_connection():
    kept: list[context.Connection] = []
    api = JsonRpc()

    @api.method()
    def watch() -> str:
        kept.append(context.current())
        return "watching"

    @api.method()
    async def poke() -> bool:
        return await kept[0].notify("page/refresh", {"page": "p1"})

    host = _Host(_attach_db, jsonrpc=api)
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        _call(ws, 1, "watch")
        assert json.loads(_recv_text(ws))["result"] == "watching"
        _call(ws, 2, "poke")
        assert json.loads(_recv_text(ws)) == {"jsonrpc": "2.0", "method": "page/refresh", "params": {"page": "p1"}}
        assert json.loads(_recv_text(ws)) == {"jsonrpc": "2.0", "id": 2, "result": True}
        _end_serve(ws, host)


def test_on_serve_notifies_after_hello_ack_before_the_client_sends_text():
    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        host.events.append(f"notified:{await handle.connection.notify('welcome', {'n': 1})}")

    host = _Host(lambda con, token: [], on_serve, jsonrpc=JsonRpc())
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(
            pack_frame(
                {"t": FT_HELLO, "token": "x", "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": ["jsonrpc"]}
            )
        )
        first = ws.receive()
        assert dict(unpack_frame(first["bytes"])[0])["t"] == FT_HELLO_ACK
        assert json.loads(_recv_text(ws)) == {"jsonrpc": "2.0", "method": "welcome", "params": {"n": 1}}
        host.wait_for("notified:True")
        _end_serve(ws, host)


def test_notify_to_an_mcp_client_waits_for_its_handshake():
    api = JsonRpc()

    @api.method()
    async def poke() -> bool:
        return await context.current().notify("page/refresh")

    host = _Host(_attach_db, jsonrpc=api, mcp=_mcp_server())
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        _hello_and_open_ack(ws)
        _call(
            ws,
            1,
            "initialize",
            {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}},
        )
        assert json.loads(_recv_text(ws))["id"] == 1
        _call(ws, 2, "poke")
        assert json.loads(_recv_text(ws)) == {"jsonrpc": "2.0", "id": 2, "result": True}
        ws.send_text(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        assert json.loads(_recv_text(ws)) == {"jsonrpc": "2.0", "method": "page/refresh"}
        _end_serve(ws, host)


class _UpstreamDb:
    """A second database served by `n6k_serve_fd`, reached through `link`: the socket a websocket to
    another n6k server would be."""

    def __init__(self, catalog: str = "remote") -> None:
        self.con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        load_n6k_server(self.con)
        self.con.execute(f"ATTACH ':memory:' AS {catalog}")
        self.link, self._served = socket.socketpair()
        row = self.con.execute("SELECT session_id FROM n6k_serve_create_session(?)", [self._served.fileno()]).fetchone()
        assert row is not None
        session_id = row[0]

        def serve() -> None:
            try:
                self.con.execute(f"CALL n6k_serve_fd('{session_id}'::UUID, '{catalog}')").fetchall()
            except Exception:  # noqa: BLE001 - a killed upstream ends its serve with an error
                pass

        self._worker = threading.Thread(target=serve, daemon=True)
        self._worker.start()

    def kill(self) -> None:
        self._served.shutdown(socket.SHUT_RDWR)

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self._served.shutdown(socket.SHUT_RDWR)
        self._worker.join(15)
        self._served.close()
        self.link.close()
        self.con.close()


class _FramedSocket:
    """An `UpstreamSocket` over a stream-framed socket. `texts` are handed out first, as text frames
    the upstream sent."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, texts: Sequence[str]) -> None:
        self._reader = reader
        self._writer = writer
        self._texts = list(texts)

    @classmethod
    async def open(cls, sock: socket.socket, texts: Sequence[str] = ()) -> "_FramedSocket":
        reader, writer = await asyncio.open_connection(sock=sock)
        return cls(reader, writer, texts)

    async def recv(self) -> Any:
        if self._texts:
            return self._texts.pop(0)
        n, frame_type = struct.unpack(">IB", await self._reader.readexactly(5))
        data = await self._reader.readexactly(n)
        return data if frame_type == 0x02 else data.decode()

    async def send(self, message: bytes) -> None:
        self._writer.write(struct.pack(">IB", len(message), 0x02) + message)
        await self._writer.drain()

    async def close(self) -> None:
        self._writer.close()


def _open_reply(ws: Any, ns: int = 1) -> dict[str, Any]:
    while True:
        frame = _recv_skipping_pings(ws)
        if frame["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and frame.get("ns") == ns:
            return frame


def test_resolve_connects_an_upstream_lazily_and_its_text_goes_to_on_text():
    upstream_db = _UpstreamDb()
    texts: list[str] = []

    async def resolve(handle: pump.LiveServe, name: str) -> None:
        up = await handle.connect_upstream(
            await _FramedSocket.open(upstream_db.link, ["run log line"]), on_text=texts.append
        )
        assert up.catalogs == ["remote"]
        assert await handle.add_upstream(up) == []

    host = _Host(_attach_db, resolve=resolve)
    try:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            _hello_and_open(ws, "remote")
            assert (reply := _open_reply(ws))["t"] == FT_OPEN_ACK, reply
            ws.send_bytes(pack_frame({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 42 AS n"}))
            while (frame := _recv_skipping_pings(ws))["t"] != FT_RESP_END:
                assert frame["t"] != FT_ERR, frame
            assert texts == ["run log line"]
            _end_serve(ws, host)
    finally:
        upstream_db.close()


def test_an_on_text_that_raises_leaves_the_upstream_up():
    upstream_db = _UpstreamDb()

    def on_text(text: str) -> None:
        raise RuntimeError("log sink down")

    async def resolve(handle: pump.LiveServe, name: str) -> None:
        up = await handle.connect_upstream(await _FramedSocket.open(upstream_db.link, ["x"]), on_text=on_text)
        await handle.add_upstream(up)

    host = _Host(_attach_db, resolve=resolve)
    try:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            _hello_and_open(ws, "remote")
            assert (reply := _open_reply(ws))["t"] == FT_OPEN_ACK, reply
            _end_serve(ws, host)
    finally:
        upstream_db.close()


def test_remove_upstream_and_a_dead_upstream_report_why_they_closed():
    removed_db = _UpstreamDb("removed")
    killed_db = _UpstreamDb("killed")
    reasons: dict[str, str] = {}
    done = threading.Event()

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        removed = await handle.connect_upstream(await _FramedSocket.open(removed_db.link))
        killed = await handle.connect_upstream(await _FramedSocket.open(killed_db.link))
        await handle.add_upstream(removed)
        await handle.add_upstream(killed)
        await handle.remove_upstream(removed)
        reasons["removed"] = await removed.wait_closed()
        killed_db.kill()
        reasons["killed"] = await killed.wait_closed()
        done.set()

    host = _Host(_attach_db, on_serve)
    try:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(ws)["t"] == FT_HELLO_ACK
            assert done.wait(20)
            assert reasons["removed"] == "removed by host"
            assert "closed" in reasons["killed"]
            _end_serve(ws, host)
    finally:
        removed_db.close()
        killed_db.close()


def test_connect_upstream_raises_upstream_error_on_hello_err():
    link, theirs = socket.socketpair()
    errors: list[BaseException] = []

    def refuse() -> None:
        header = theirs.recv(5)
        n = struct.unpack(">IB", header)[0]
        theirs.recv(n)
        frame = pack_frame({"t": FT_HELLO_ERR, "exception_type": "InvalidInputException", "exception_message": "no"})
        theirs.sendall(struct.pack(">IB", len(frame), 0x02) + frame)

    refuser = threading.Thread(target=refuse, daemon=True)
    refuser.start()
    done = threading.Event()

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        try:
            await handle.connect_upstream(await _FramedSocket.open(link))
        except pump.UpstreamError as exc:
            errors.append(exc)
        done.set()

    host = _Host(_attach_db, on_serve)
    try:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(ws)["t"] == FT_HELLO_ACK
            assert done.wait(10)
            assert len(errors) == 1 and "no" in str(errors[0])
            _end_serve(ws, host)
    finally:
        refuser.join(5)
        theirs.close()
        link.close()


def test_two_connections_keep_independent_upstreams():
    dbs = [_UpstreamDb(), _UpstreamDb()]
    links = [db.link for db in dbs]
    lock = threading.Lock()

    async def resolve(handle: pump.LiveServe, name: str) -> None:
        with lock:
            link = links.pop(0)
        await handle.add_upstream(await handle.connect_upstream(await _FramedSocket.open(link)))

    host = _Host(_attach_db, resolve=resolve)
    app = host.app()
    try:
        with TestClient(app).websocket_connect("/ws") as ws1, TestClient(app).websocket_connect("/ws") as ws2:
            _hello_and_open(ws1, "remote")
            assert (reply := _open_reply(ws1))["t"] == FT_OPEN_ACK, reply
            _hello_and_open(ws2, "remote")
            assert (reply := _open_reply(ws2))["t"] == FT_OPEN_ACK, reply
            assert links == []
            for ws in (ws1, ws2):
                ws.send_bytes(pack_frame({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 1"}))
                while (frame := _recv_skipping_pings(ws))["t"] != FT_RESP_END:
                    assert frame["t"] != FT_ERR, frame
            _end_serve(ws2, host)
            _end_serve(ws1, host)
    finally:
        for db in dbs:
            db.close()


def test_cancelling_connect_upstream_during_hello_pending_returns_promptly_and_closes_the_socket():
    link, theirs = socket.socketpair()
    saw_eof = threading.Event()

    def stay_pending() -> None:
        n = struct.unpack(">IB", theirs.recv(5))[0]
        theirs.recv(n)
        frame = pack_frame({"t": FT_HELLO_PENDING, "message": "starting"})
        theirs.settimeout(0.5)
        while True:
            try:
                if theirs.recv(1) == b"":
                    saw_eof.set()
                    return
            except socket.timeout:
                pass
            except OSError:
                saw_eof.set()
                return
            try:
                theirs.sendall(struct.pack(">IB", len(frame), 0x02) + frame)
            except OSError:
                saw_eof.set()
                return

    pending = threading.Thread(target=stay_pending, daemon=True)
    pending.start()
    outcome: dict[str, Any] = {}
    done = threading.Event()

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        connecting = asyncio.create_task(handle.connect_upstream(await _FramedSocket.open(link)))
        await asyncio.sleep(1.5)
        started = time.monotonic()
        connecting.cancel()
        try:
            await connecting
        except asyncio.CancelledError:
            outcome["cancelled"] = True
        outcome["seconds"] = time.monotonic() - started
        done.set()

    host = _Host(_attach_db, on_serve)
    try:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(ws)["t"] == FT_HELLO_ACK
            assert done.wait(10)
            assert outcome.get("cancelled") is True
            assert outcome["seconds"] < 2
            assert saw_eof.wait(5), "the upstream socket was not closed"
            _end_serve(ws, host)
    finally:
        pending.join(5)
        theirs.close()
        link.close()


def test_a_lazy_catalog_is_listed_at_once_and_resolve_serves_it_from_an_upstream_on_open():
    upstream_db = _UpstreamDb()
    names: list[str] = []

    def authenticate(con: duckdb.DuckDBPyConnection, token: str) -> list[Any]:
        con.execute("ATTACH ':memory:' AS db")
        return ["db", LazyCatalog("remote")]

    async def resolve(handle: pump.LiveServe, name: str) -> None:
        names.append(name)
        await handle.add_upstream(await handle.connect_upstream(await _FramedSocket.open(upstream_db.link)))

    host = _Host(authenticate, resolve=resolve)
    try:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(ws)["t"] == FT_HELLO_ACK
            ws.send_bytes(pack_frame({"t": FT_REQ, "id": 9, "op": OP_CATALOG_LIST}))
            while (frame := _recv_skipping_pings(ws))["t"] != FT_RESP_CHUNK:
                pass
            assert frame["catalogs"] == ["db", "remote"]
            assert names == []
            ws.send_bytes(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "remote"}))
            assert (reply := _open_reply(ws))["t"] == FT_OPEN_ACK, reply
            assert names == ["remote"]
            _end_serve(ws, host)
    finally:
        upstream_db.close()


def test_a_lazy_catalog_without_resolve_refuses_the_client():
    host = _Host(lambda con, token: [LazyCatalog("remote")])
    with TestClient(host.app()).websocket_connect("/ws") as ws:
        ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
        assert _recv(ws)["t"] == FT_HELLO_ERR
        _expect_disconnect(ws)


def test_a_ranked_lazy_catalog_keeps_its_name_until_an_upstream_outranks_it():
    upstream_db = _UpstreamDb("sales")
    results: dict[str, list[str]] = {}
    done = threading.Event()

    def authenticate(con: duckdb.DuckDBPyConnection, token: str) -> list[Any]:
        return [LazyCatalog("sales", rank=1)]

    async def on_serve(ws: Any, handle: pump.LiveServe) -> None:
        up = await handle.connect_upstream(await _FramedSocket.open(upstream_db.link))
        results["added"] = await handle.add_upstream(up, rank=2)
        results["reranked"] = await handle.set_upstream_rank(up, 0)
        done.set()

    async def resolve(handle: pump.LiveServe, name: str) -> None:
        pass

    host = _Host(authenticate, on_serve, resolve=resolve)
    try:
        with TestClient(host.app()).websocket_connect("/ws") as ws:
            ws.send_bytes(pack_frame({"t": FT_HELLO, "token": "tok", "protocol_version": N6K_PROTOCOL_VERSION}))
            assert _recv(ws)["t"] == FT_HELLO_ACK
            assert done.wait(10)
            assert results == {"added": ["sales"], "reranked": []}
            _end_serve(ws, host)
    finally:
        upstream_db.close()
