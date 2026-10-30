"""`n6k_serve_ws` on a host-accepted socket, and the host callbacks shared with `n6k_serve_http`.

uv run pytest integration_tests/test_n6k_serve_ws.py -v
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import socket
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

import duckdb
import pyarrow.ipc as ipc
import pytest
import websockets
from duckdb.sqltypes import UUID, VARCHAR

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_PUSH,
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_OPEN_ACK,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
    N6K_PROTOCOL_VERSION,
    OP_QUERY,
)

from _paths import REPO_ROOT
from _targets import hello, open_session

SERVER_EXT = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT), reason=f"not built: {SERVER_EXT} (run `make release`)")

AUTH_ARGS = [UUID, VARCHAR, duckdb.list_type(VARCHAR), VARCHAR]
TIMEOUT = 10


def _read_request(conn: socket.socket) -> Dict[str, Any]:
    raw = b""
    while not raw.endswith(b"\r\n\r\n"):
        b = conn.recv(1)
        assert b, "peer closed during the upgrade request"
        raw += b
    lines = raw.decode("latin-1").split("\r\n")
    method, uri, version = lines[0].split(" ", 2)
    headers = {}
    for line in lines[1:]:
        if line:
            k, v = line.split(":", 1)
            headers[k.strip()] = v.strip()
    return {"method": method, "uri": uri, "version": version, "headers": headers}


def _server_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.load_extension(SERVER_EXT)
    con.execute("ATTACH ':memory:' AS db")
    con.execute("CREATE TABLE db.users (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO db.users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
    return con


def _register_auth(con: duckdb.DuckDBPyConnection, name: str, fn: Callable[..., str]) -> None:
    con.create_function(name, fn, AUTH_ARGS, VARCHAR, null_handling="special", side_effects=True)


class WsServed:
    """One `n6k_serve_ws` CALL on a thread, serving the first connection to `port`."""

    def __init__(self, con: duckdb.DuckDBPyConnection, args: str = "'db'") -> None:
        self.con = con
        self.args = args
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.port = int(self.listener.getsockname()[1])
        self.row: Optional[tuple[Any, ...]] = None
        self.error: Optional[BaseException] = None
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        try:
            conn, _ = self.listener.accept()
            request = _read_request(conn)
            fd = conn.detach()
            session_id = self.con.execute("SELECT session_id FROM n6k_serve_create_session(?)", [fd]).fetchone()[0]
            self.row = self.con.execute(
                f"CALL n6k_serve_ws(?, ?, {self.args})", [session_id, json.dumps(request)]
            ).fetchone()
        except BaseException as e:
            self.error = e

    def url(self, path: str = "/") -> str:
        return f"ws://127.0.0.1:{self.port}{path}"

    def finish(self) -> tuple[Any, ...]:
        self.thread.join(TIMEOUT)
        self.listener.close()
        assert not self.thread.is_alive(), "n6k_serve_ws did not return after the peer closed"
        assert self.error is None, self.error
        assert self.row is not None
        return self.row


async def _recv(ws: Any) -> tuple[Dict[str, Any], bytes]:
    raw = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT)
    assert not isinstance(raw, str), f"expected binary frame, got text: {raw!r}"
    header, body = unpack_frame(raw)
    return dict(header), bytes(body)


async def _query(ws: Any, sql: str, req_id: int = 1) -> Any:
    await ws.send(pack_frame({"t": FT_REQ, "id": req_id, "op": OP_QUERY, "sql": sql, "ns": 1}))
    parts = []
    while True:
        header, body = await _recv(ws)
        assert header.get("id") == req_id, header
        if header["t"] in (FT_RESP_SCHEMA, FT_RESP_CHUNK):
            parts.append(body)
        elif header["t"] == FT_RESP_END:
            return ipc.open_stream(io.BytesIO(b"".join(parts))).read_all()
        else:
            assert header["t"] != FT_ERR, f"server error: {header.get('exception_message')!r}"
            raise AssertionError(f"unexpected frame type {header['t']}")


async def _wait_closed(ws: Any) -> int:
    await asyncio.wait_for(ws.wait_closed(), timeout=TIMEOUT)
    return int(ws.close_code)


def test_serves_query_and_returns_on_close() -> None:
    served = WsServed(_server_con())

    async def client() -> None:
        async with websockets.connect(served.url()) as ws:
            assert (await hello(ws))["t"] == FT_HELLO_ACK
            assert (await open_session(ws, 1, "db"))["t"] == FT_OPEN_ACK
            table = await _query(ws, "SELECT id, name FROM db.users ORDER BY id")
            assert table.to_pydict() == {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}

    asyncio.run(asyncio.wait_for(client(), TIMEOUT))
    connected, requests, frames_in, bytes_in, frames_out, bytes_out, duration_ms, authenticate_ms = served.finish()
    assert connected is True
    assert requests == 1
    assert frames_in >= 2 and bytes_in > 0
    assert frames_out >= 4 and bytes_out > 0
    assert duration_ms >= 0 and authenticate_ms == 0


def test_authenticate_reject_sends_hello_err_and_close_code() -> None:
    con = _server_con()
    _register_auth(
        con, "auth_reject", lambda session_id, token, caps, request: json.dumps({"reject": "not you", "code": 4403})
    )
    served = WsServed(con, "'db', authenticate => 'auth_reject'")

    async def client() -> int:
        async with websockets.connect(served.url()) as ws:
            ack = await hello(ws)
            assert ack["t"] == FT_HELLO_ERR, ack
            assert "not you" in ack["exception_message"]
            return await _wait_closed(ws)

    assert asyncio.run(asyncio.wait_for(client(), TIMEOUT)) == 4403
    served.finish()


def test_authenticate_sees_upgrade_request() -> None:
    seen: List[Dict[str, Any]] = []

    def auth(session_id: uuid.UUID, token: str, caps: List[str], request: str) -> str:
        seen.append(json.loads(request))
        return json.dumps({"catalogs": ["db"]})

    con = _server_con()
    _register_auth(con, "auth_record", auth)
    served = WsServed(con, "'db', authenticate => 'auth_record'")

    async def client() -> None:
        async with websockets.connect(served.url("/app?x=1"), additional_headers={"X-Test": "1"}) as ws:
            assert (await hello(ws))["t"] == FT_HELLO_ACK

    asyncio.run(asyncio.wait_for(client(), TIMEOUT))
    served.finish()
    assert len(seen) == 1
    assert seen[0]["uri"] == "/app?x=1"
    headers = {k.lower(): v for k, v in seen[0]["headers"].items()}
    assert headers.get("x-test") == "1", headers


def test_on_text_echo() -> None:
    con = _server_con()
    _register_auth(
        con,
        "auth_rpc",
        lambda session_id, token, caps, request: json.dumps({"catalogs": ["db"], "app_capabilities": ["jsonrpc"]}),
    )
    con.create_function(
        "echo_text", lambda session_id, text: text, [UUID, VARCHAR], VARCHAR, null_handling="special", side_effects=True
    )
    served = WsServed(con, "'db', authenticate => 'auth_rpc', on_text => 'echo_text'")

    async def client() -> None:
        async with websockets.connect(served.url()) as ws:
            await ws.send(
                pack_frame(
                    {"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": ["jsonrpc"]}
                )
            )
            ack, _ = await _recv(ws)
            assert ack["t"] == FT_HELLO_ACK, ack
            assert "jsonrpc" in ack["capabilities"]
            await ws.send('{"jsonrpc":"2.0","id":1,"method":"ping"}')
            assert await asyncio.wait_for(ws.recv(), TIMEOUT) == '{"jsonrpc":"2.0","id":1,"method":"ping"}'

    asyncio.run(asyncio.wait_for(client(), TIMEOUT))
    served.finish()


def test_serve_close_ends_connection_with_code() -> None:
    session_ids: List[uuid.UUID] = []

    def auth(session_id: uuid.UUID, token: str, caps: List[str], request: str) -> str:
        session_ids.append(session_id)
        return json.dumps({"catalogs": ["db"]})

    con = _server_con()
    _register_auth(con, "auth_fd", auth)
    served = WsServed(con, "'db', authenticate => 'auth_fd'")

    async def client() -> int:
        async with websockets.connect(served.url()) as ws:
            assert (await hello(ws))["t"] == FT_HELLO_ACK
            con.cursor().execute("CALL n6k_serve_close(?, 4000, 'bye')", [session_ids[0]])
            return await _wait_closed(ws)

    assert asyncio.run(asyncio.wait_for(client(), TIMEOUT)) == 4000
    served.finish()


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def test_serve_http_add_catalog_after_authenticate() -> None:
    session_ids: List[uuid.UUID] = []

    def auth(session_id: uuid.UUID, token: str, caps: List[str], request: str) -> str:
        session_ids.append(session_id)
        return json.dumps({"catalogs": []})

    con = _server_con()
    _register_auth(con, "auth_http", auth)
    port = _free_port()
    errors: List[BaseException] = []

    def serve() -> None:
        try:
            con.execute(f"CALL n6k_serve_http('127.0.0.1', {port}, authenticate => 'auth_http')").fetchall()
        except BaseException as e:
            errors.append(e)

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    try:
        deadline = time.time() + TIMEOUT
        while not _port_open(port):
            assert t.is_alive(), errors
            assert time.time() < deadline, "n6k_serve_http never started listening"
            time.sleep(0.05)

        async def client() -> None:
            async with websockets.connect(f"ws://127.0.0.1:{port}/") as ws:
                assert (await hello(ws))["t"] == FT_HELLO_ACK
                con.cursor().execute("CALL n6k_serve_add_catalog(?, 'db')", [session_ids[0]])
                pushed, _ = await _recv(ws)
                assert pushed["t"] == FT_PUSH and list(pushed["catalogs"]) == ["db"], pushed
                opened = await open_session(ws, 1, "db")
                assert opened["t"] == FT_OPEN_ACK, opened

        asyncio.run(asyncio.wait_for(client(), TIMEOUT))
    finally:
        con.interrupt()
        t.join(TIMEOUT)
    assert not t.is_alive(), "n6k_serve_http did not end on interrupt"
