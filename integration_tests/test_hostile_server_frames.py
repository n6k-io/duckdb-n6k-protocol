"""A malformed frame from a server closes that connection, not the client process.

The client runs in a subprocess, so an abort fails the test instead of killing pytest.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any

import msgpack
import pytest
import websockets

from n6k_protocol.engine import unpack_frame
from n6k_protocol.protocol import (
    FT_HELLO,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_PUSH,
    N6K_PROTOCOL_VERSION,
    OP_CATALOG_INVALIDATED,
    STREAM_FRAME_N6K,
    STREAM_HEADER,
)

from _paths import EXT_PATH

pytestmark = pytest.mark.skipif(not os.path.exists(EXT_PATH), reason=f"not built: {EXT_PATH} (run `make release`)")

BAD_SCHEMAS = [{"a": 1}, [1, None, {"a": 1}], "main", 7]
ODD_SCHEMAS = [["nope"], [""], ["x" * 2_000_000], ["a'\n\"\\"]]

CHILD_PRELUDE = """
import json, sys, time
import duckdb
c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
try:
    c.load_extension("httpfs")
except Exception:
    pass
c.load_extension(sys.argv[1])
def alive():
    return sorted(row[0] for row in c.execute("SELECT alive FROM n6k_connections()").fetchall())
"""

WS_CHILD = CHILD_PRELUDE + """
port = sys.argv[2]
c.execute(f"ATTACH 'n6k://127.0.0.1:{port}' AS victim (TYPE n6k, anonymous true)")
c.execute(f"ATTACH 'n6k://127.0.0.1:{port}' AS bystander (TYPE n6k, token 'other')")
time.sleep(1.5)
print(json.dumps({"alive": alive()}), flush=True)
"""

FD_CHILD = CHILD_PRELUDE + """
cid = c.execute("SELECT connection_id FROM n6k_connect(wsFd => ?, token => 't')", [int(sys.argv[2])]).fetchone()[0]
c.execute(f"ATTACH '' AS victim (TYPE n6k, CONNECTION '{cid}')")
time.sleep(1.5)
print(json.dumps({"alive": alive()}), flush=True)
"""


def _env() -> dict[str, str]:
    return {**os.environ, "HOME": tempfile.mkdtemp()}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _push(ns: int, schemas: Any) -> dict[str, Any]:
    return {"t": FT_PUSH, "ns": ns, "op": OP_CATALOG_INVALIDATED, "schemas": schemas}


def _run_ws_client(schemas: Any) -> subprocess.CompletedProcess[str]:
    port = _free_port()

    async def handler(ws: Any) -> None:
        try:
            async for raw in ws:
                header, _ = unpack_frame(raw)
                if header.get("t") == FT_HELLO:
                    await ws.send(
                        msgpack.packb({"t": FT_HELLO_ACK, "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": []})
                    )
                elif header.get("t") == FT_OPEN:
                    await ws.send(msgpack.packb({"t": FT_OPEN_ACK, "ns": header["ns"], "capabilities": []}))
                    if header.get("catalog") == "victim":
                        await asyncio.sleep(0.5)
                        await ws.send(msgpack.packb(_push(header["ns"], schemas)))
        except websockets.ConnectionClosed:
            pass

    async def serve() -> None:
        async with websockets.serve(handler, "127.0.0.1", port):
            await asyncio.Future()

    threading.Thread(target=lambda: asyncio.run(serve()), daemon=True).start()
    time.sleep(0.3)
    return subprocess.run(
        [sys.executable, "-c", WS_CHILD, EXT_PATH, str(port)],
        capture_output=True,
        text=True,
        timeout=60,
        env=_env(),
    )


def _recv_exact(sock: socket.socket, count: int) -> bytes:
    buf = b""
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        assert chunk, "the extension closed the socket early"
        buf += chunk
    return buf


def _recv_frame(sock: socket.socket) -> dict[str, Any]:
    length, _ = STREAM_HEADER.unpack(_recv_exact(sock, STREAM_HEADER.size))
    header, _ = unpack_frame(_recv_exact(sock, length))
    return dict(header)


def _send_frame(sock: socket.socket, header: dict[str, Any]) -> None:
    payload = msgpack.packb(header)
    sock.sendall(STREAM_HEADER.pack(len(payload), STREAM_FRAME_N6K) + payload)


def _saw_eof(sock: socket.socket) -> bool:
    try:
        while sock.recv(65536):
            pass
    except socket.timeout:
        return False
    return True


def _assert_survived(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    assert result.returncode == 0, result.stdout + result.stderr
    return dict(json.loads(result.stdout.strip().splitlines()[-1]))


@pytest.mark.parametrize("schemas", BAD_SCHEMAS)
def test_bad_push_over_websocket_closes_only_that_connection(schemas: Any) -> None:
    assert _assert_survived(_run_ws_client(schemas))["alive"] == [False, True]


@pytest.mark.parametrize("schemas", ODD_SCHEMAS)
def test_odd_but_valid_schema_names_keep_the_connection(schemas: Any) -> None:
    assert _assert_survived(_run_ws_client(schemas))["alive"] == [True, True]


@pytest.mark.parametrize("schemas", BAD_SCHEMAS)
def test_bad_push_over_wsfd_gives_the_relay_eof(schemas: Any) -> None:
    relay, ext = socket.socketpair()
    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", FD_CHILD, EXT_PATH, str(ext.fileno())],
            pass_fds=[ext.fileno()],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=_env(),
        )
        ext.close()
        relay.settimeout(15)
        try:
            assert _recv_frame(relay)["t"] == FT_HELLO
            _send_frame(relay, {"t": FT_HELLO_ACK, "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": []})
            opened = _recv_frame(relay)
            assert opened["t"] == FT_OPEN
            _send_frame(relay, {"t": FT_OPEN_ACK, "ns": opened["ns"], "capabilities": []})
            time.sleep(0.5)
            _send_frame(relay, _push(opened["ns"], schemas))
            assert _saw_eof(relay), "the relay never saw EOF after the bad frame"
            stdout, stderr = proc.communicate(timeout=30)
        finally:
            proc.kill()
    finally:
        relay.close()
    assert proc.returncode == 0, stdout + stderr
    assert json.loads(stdout.strip().splitlines()[-1])["alive"] == [False]
