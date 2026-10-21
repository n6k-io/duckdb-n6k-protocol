"""End-to-end round trip for `n6k_serve_http`, the listening WebSocket server.

The extension binds the port itself, so the test connects straight to it. That also
means one server serves every test in the module — the fixture is session-scoped
and each test opens its own client.

    uv run pytest integration_tests/test_n6k_serve_http.py -v
"""

from __future__ import annotations

import asyncio
import io
import os
import socket
import subprocess
import time

import pyarrow.ipc as ipc
import pytest
import websockets

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO_ACK,
    FT_OPEN_ACK,
    FT_PING,
    FT_PONG,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
    OP_QUERY,
    OP_READ_PLAN,
)

from _paths import REPO_ROOT
from _targets import hello, open_session, substrait_field, substrait_nullable, substrait_plan, substrait_read

DUCKDB_BIN = os.path.join(REPO_ROOT, "build", "release", "duckdb")

SEED = (
    "CREATE TABLE users (id INTEGER, name VARCHAR);"
    "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');"
)


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


@pytest.fixture(scope="module")
def http_served() -> object:
    """Spawn a duckdb running `n6k_serve_http`; yield its port. Tears it down."""
    if not os.path.exists(DUCKDB_BIN):
        pytest.skip(f"not built: {DUCKDB_BIN} (run `make release`)")

    port = _free_port()
    proc = subprocess.Popen(
        [DUCKDB_BIN, "-unsigned", "-c", f"{SEED} CALL n6k_serve_http('127.0.0.1', {port}, 'memory');"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.time() + 30
        while not _port_open(port):
            if proc.poll() is not None:
                out = proc.stdout.read().decode(errors="replace") if proc.stdout else ""
                raise RuntimeError(f"duckdb exited before listening:\n{out[:2000]}")
            if time.time() > deadline:
                raise RuntimeError("n6k_serve_http never started listening")
            time.sleep(0.1)
        yield port
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


async def _recv(ws: object) -> tuple[dict, bytes]:
    raw = await asyncio.wait_for(ws.recv(), timeout=10)  # type: ignore[attr-defined]
    assert not isinstance(raw, str), f"expected binary frame, got text: {raw!r}"
    header, body = unpack_frame(raw)
    return header, bytes(body)


async def _connect(port: int) -> object:
    ws = await websockets.connect(f"ws://127.0.0.1:{port}/")
    assert (await hello(ws, "any-token"))["t"] == FT_HELLO_ACK
    assert (await open_session(ws, 1, "memory"))["t"] == FT_OPEN_ACK
    return ws


async def _drain_arrow(ws: object, req_id: int) -> object:
    parts = []
    while True:
        header, body = await _recv(ws)
        assert header.get("id") == req_id, header
        ftype = header["t"]
        if ftype in (FT_RESP_SCHEMA, FT_RESP_CHUNK):
            parts.append(body)
        elif ftype == FT_RESP_END:
            return ipc.open_stream(io.BytesIO(b"".join(parts))).read_all()
        elif ftype == FT_ERR:
            raise AssertionError(f"server error: {header.get('exception_message')!r}")
        else:
            raise AssertionError(f"unexpected frame type {ftype}")


async def _query(ws: object, sql: str, req_id: int = 1) -> object:
    frame = pack_frame({"t": FT_REQ, "id": req_id, "op": OP_QUERY, "sql": sql, "ns": 1})
    await ws.send(frame)  # type: ignore[attr-defined]
    return await _drain_arrow(ws, req_id)


@pytest.mark.asyncio
async def test_hello_with_any_token_is_acked(http_served) -> None:
    async with websockets.connect(f"ws://127.0.0.1:{http_served}/") as ws:
        assert (await hello(ws, "anything"))["t"] == FT_HELLO_ACK
        opened = await open_session(ws, 1, "memory")
        assert opened["t"] == FT_OPEN_ACK, opened
        assert opened["default_batch_credits"] > 0


@pytest.mark.asyncio
async def test_ping_pong(http_served) -> None:
    ws = await _connect(http_served)
    try:
        await ws.send(pack_frame({"t": FT_PING, "id": 7}))
        header, _ = await _recv(ws)
        assert header["t"] == FT_PONG and header.get("id") == 7, header
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_query_streams_arrow(http_served) -> None:
    ws = await _connect(http_served)
    try:
        table = await _query(ws, "SELECT id, name FROM users ORDER BY id")
        assert table.to_pydict() == {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_read_plan_with_projection_and_filter(http_served) -> None:
    rel = {
        "filter": {
            "input": substrait_read("main", "users", [("id", "i32"), ("name", "string")], [0, 1]),
            "condition": {
                "scalarFunction": {
                    "functionReference": 1,
                    "outputType": substrait_nullable("bool"),
                    "arguments": [{"value": substrait_field(0)}, {"value": {"literal": {"i32": 1}}}],
                }
            },
        }
    }
    rel = {"project": {"input": rel, "common": {"emit": {"outputMapping": [1]}}}}
    ws = await _connect(http_served)
    try:
        await ws.send(
            pack_frame(
                {
                    "t": FT_REQ,
                    "id": 3,
                    "op": OP_READ_PLAN,
                    "plan": substrait_plan(rel, ["name"], ["gt"]),
                    "ns": 1,
                }
            )
        )
        table = await _drain_arrow(ws, 3)
        assert table.column_names == ["name"]
        assert sorted(table.to_pydict()["name"]) == ["Bob", "Charlie"]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_serves_many_clients_concurrently(http_served) -> None:
    """The whole point of this form: `n6k_serve_socket` cannot do this at all."""
    a = await _connect(http_served)
    b = await _connect(http_served)
    try:
        ta, tb = await asyncio.gather(
            _query(a, "SELECT count(*) AS c FROM users"),
            _query(b, "SELECT max(id) AS m FROM users"),
        )
        assert ta.to_pydict() == {"c": [3]}
        assert tb.to_pydict()["m"] == [3]
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_server_outlives_a_disconnect(http_served) -> None:
    """A client leaving must not end the CALL — it ends only on interrupt."""
    ws = await _connect(http_served)
    await _query(ws, "SELECT 1 AS x")
    await ws.close()
    await asyncio.sleep(0.3)

    ws2 = await _connect(http_served)
    try:
        assert (await _query(ws2, "SELECT 2 AS x")).to_pydict() == {"x": [2]}
    finally:
        await ws2.close()


@pytest.mark.asyncio
async def test_clients_share_one_catalog(http_served) -> None:
    """Every client rides one DatabaseInstance, so a write is visible to the next.

    This is the real behavioural difference from every other deployment, all of
    which give a client its own process (and so its own catalogs).
    """
    ws = await _connect(http_served)
    try:
        await _query(ws, "CREATE TABLE shared_marker (v INTEGER)")
        await _query(ws, "INSERT INTO shared_marker VALUES (42)")
    finally:
        await ws.close()

    ws2 = await _connect(http_served)
    try:
        assert (await _query(ws2, "SELECT v FROM shared_marker")).to_pydict() == {"v": [42]}
    finally:
        await ws2.close()
