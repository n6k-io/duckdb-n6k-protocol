"""A client cannot grow a connection's threads or queues without bound: one resolver thread with a cap
on names being resolved, a cap on JSON-RPC messages waiting for on_text, and a cap on bytes queued for
a client that stops reading.

Drives the reactor directly over a socketpair, no websocket and no test server.

Run:
    uv run pytest integration_tests/test_reactor_bounds.py -v
"""

import itertools
import json
import os
import socket
import struct
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import msgpack
import pytest
from duckdb.sqltypes import UUID, VARCHAR

from _paths import REPO_ROOT

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_CREDIT,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_OPEN_PENDING,
    FT_REQ,
    MAX_CONCURRENT_REQS,
    N6K_PROTOCOL_VERSION,
    OP_QUERY,
    STREAM_FRAME_CLOSE,
    STREAM_FRAME_JSONRPC,
    STREAM_FRAME_N6K,
)

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")

_udf_ids = itertools.count()


class _Peer:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        sock.settimeout(10)

    def send(self, frame_type: int, payload: bytes) -> None:
        self.sock.sendall(struct.pack(">IB", len(payload), frame_type) + payload)

    def send_n6k(self, header: dict[str, Any]) -> None:
        self.send(STREAM_FRAME_N6K, pack_frame(header))

    def recv(self) -> tuple[int, bytes]:
        n, frame_type = struct.unpack(">IB", self._exactly(5))
        return frame_type, self._exactly(n)

    def recv_n6k(self) -> dict[str, Any]:
        while True:
            frame_type, payload = self.recv()
            if frame_type == STREAM_FRAME_N6K:
                return dict(unpack_frame(payload)[0])

    def _exactly(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            assert chunk, "reactor closed the socket"
            buf += chunk
        return buf


@contextmanager
def _serving(con: duckdb.DuckDBPyConnection, sql_tail: str, capabilities: list[str] | None = None) -> Iterator[_Peer]:
    peer_sock, served = socket.socketpair()
    session_id = con.execute("SELECT session_id FROM n6k_serve_create_session(?)", [served.fileno()]).fetchone()[0]

    def serve() -> None:
        try:
            con.execute(f"CALL n6k_serve_fd('{session_id}'::UUID{sql_tail})").fetchall()
        except BaseException:
            pass

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    peer = _Peer(peer_sock)
    try:
        hello: dict[str, Any] = {"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION}
        if capabilities:
            hello["capabilities"] = capabilities
        peer.send_n6k(hello)
        assert peer.recv_n6k()["t"] == FT_HELLO_ACK
        yield peer
    finally:
        peer_sock.close()
        worker.join(15)
        served.close()
    assert not worker.is_alive(), "serve did not end after the peer closed"


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    c.execute("ATTACH ':memory:' AS a")
    yield c
    c.close()


class _Blocking:
    """A host callback that blocks until released, counting how many calls are inside it at once."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.calls = 0
        self.inside = 0
        self.most_inside = 0
        self._lock = threading.Lock()

    def __call__(self, session_id: uuid.UUID, value: str) -> None:
        with self._lock:
            self.calls += 1
            self.inside += 1
            self.most_inside = max(self.most_inside, self.inside)
        self.release.wait(10)
        with self._lock:
            self.inside -= 1


def test_resolves_run_one_at_a_time_and_cap_the_names_waiting(con: duckdb.DuckDBPyConnection) -> None:
    blocking = _Blocking()
    name = f"resolve_{next(_udf_ids)}"
    con.create_function(name, blocking, [UUID, VARCHAR], VARCHAR, null_handling="special", side_effects=True)
    with _serving(con, f", resolve => '{name}'") as peer:
        try:
            for ns in range(1, MAX_CONCURRENT_REQS + 1):
                peer.send_n6k({"t": FT_OPEN, "ns": ns, "catalog": f"unknown_{ns}"})
            pending = set()
            while len(pending) < MAX_CONCURRENT_REQS:
                frame = peer.recv_n6k()
                assert frame["t"] != FT_OPEN_ERR, frame
                if frame["t"] == FT_OPEN_PENDING:
                    pending.add(frame["ns"])
            peer.send_n6k({"t": FT_OPEN, "ns": 1000, "catalog": "one_too_many"})
            frame = peer.recv_n6k()
            while frame["t"] == FT_OPEN_PENDING:
                frame = peer.recv_n6k()
            assert frame["t"] == FT_OPEN_ERR and frame["ns"] == 1000
            assert "too many catalogs resolving" in frame["exception_message"]
            assert blocking.most_inside == 1
        finally:
            blocking.release.set()


def test_an_open_for_a_name_already_resolving_is_not_capped(con: duckdb.DuckDBPyConnection) -> None:
    blocking = _Blocking()
    name = f"resolve_{next(_udf_ids)}"
    con.create_function(name, blocking, [UUID, VARCHAR], VARCHAR, null_handling="special", side_effects=True)
    with _serving(con, f", resolve => '{name}'") as peer:
        try:
            for ns in range(1, MAX_CONCURRENT_REQS + 1):
                peer.send_n6k({"t": FT_OPEN, "ns": ns, "catalog": f"unknown_{ns}"})
            peer.send_n6k({"t": FT_OPEN, "ns": 1000, "catalog": "unknown_1"})
            seen_1000 = None
            while seen_1000 is None:
                frame = peer.recv_n6k()
                if frame.get("ns") == 1000:
                    seen_1000 = frame
            assert seen_1000["t"] == FT_OPEN_PENDING
        finally:
            blocking.release.set()


def test_jsonrpc_past_the_queue_cap_is_answered_busy(con: duckdb.DuckDBPyConnection) -> None:
    blocking = _Blocking()
    name = f"on_text_{next(_udf_ids)}"
    con.create_function(name, blocking, [UUID, VARCHAR], VARCHAR, null_handling="special", side_effects=True)
    sent = MAX_CONCURRENT_REQS + 10
    with _serving(con, f", 'a', on_text => '{name}'", capabilities=["jsonrpc"]) as peer:
        try:
            for i in range(sent):
                message = {"jsonrpc": "2.0", "id": i, "method": "slow"}
                peer.send(STREAM_FRAME_JSONRPC, json.dumps(message).encode())
            busy = []
            deadline = time.monotonic() + 10
            # Relies on MAX_QUEUED_TEXT (serve_reactor.cpp) == MAX_CONCURRENT_REQS: one message is inside
            # on_text, the queue is full, the rest are refused.
            while len(busy) < sent - MAX_CONCURRENT_REQS - 1 and time.monotonic() < deadline:
                frame_type, payload = peer.recv()
                if frame_type == STREAM_FRAME_JSONRPC:
                    busy.append(json.loads(payload))
            assert len(busy) >= sent - MAX_CONCURRENT_REQS - 1
            assert all(reply["error"]["code"] == -32000 for reply in busy)
        finally:
            blocking.release.set()


def test_a_client_that_stops_reading_is_closed_not_buffered(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as peer:
        peer.send_n6k({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        assert peer.recv_n6k()["t"] == FT_OPEN_ACK
        # Must exceed MAX_QUEUED_OUT_BYTES (serve_reactor.cpp); all credited, none read.
        peer.send_n6k(
            {
                "t": FT_REQ,
                "id": 1,
                "ns": 1,
                "op": OP_QUERY,
                "sql": "SELECT repeat('x', 1048576) AS s FROM range(400)",
                "_batch_rows": 1,
            }
        )
        peer.send_n6k({"t": FT_CREDIT, "id": 1, "ns": 1, "n": 1000})
        time.sleep(5)
        while True:
            frame_type, payload = peer.recv()
            if frame_type == STREAM_FRAME_CLOSE:
                assert msgpack.unpackb(payload)["code"] == 1008
                break
