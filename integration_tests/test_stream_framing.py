import json
import os
import socket
import struct
import threading
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
import pytest

from _paths import REPO_ROOT

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_HELLO_ACK,
    FT_PING,
    FT_REQ,
    OP_CATALOG_LIST,
    STREAM_FRAME_JSONRPC,
    STREAM_FRAME_N6K,
)

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")


class _Peer:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        sock.settimeout(5)

    def send(self, frame_type: int, payload: bytes) -> None:
        self.sock.sendall(struct.pack(">IB", len(payload), frame_type) + payload)

    def recv(self) -> tuple[int, bytes]:
        while True:
            n, frame_type = struct.unpack(">IB", self._exactly(5))
            payload = self._exactly(n)
            if frame_type != STREAM_FRAME_N6K or unpack_frame(payload)[0]["t"] != FT_PING:
                return frame_type, payload

    def closed(self) -> bool:
        while True:
            try:
                chunk = self.sock.recv(4096)
            except ConnectionResetError:
                return True
            if not chunk:
                return True

    def _exactly(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            assert chunk, "reactor closed the socket"
            buf += chunk
        return buf


@contextmanager
def _serving(con: duckdb.DuckDBPyConnection) -> Iterator[_Peer]:
    peer_sock, served = socket.socketpair()
    fd = served.fileno()

    def serve() -> None:
        try:
            con.execute(f"CALL n6k_serve_fd({fd}, 'db')").fetchall()
        except BaseException:
            pass

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    peer = _Peer(peer_sock)
    try:
        frame_type, payload = peer.recv()
        assert frame_type == STREAM_FRAME_N6K
        assert unpack_frame(payload)[0]["t"] == FT_HELLO_ACK
        yield peer
    finally:
        peer_sock.close()
        worker.join(10)
        served.close()
    assert not worker.is_alive(), "serve did not end after the peer closed"


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    c.execute("ATTACH ':memory:' AS db")
    yield c
    c.close()


def _catalog_list(peer: _Peer) -> dict:
    peer.send(STREAM_FRAME_N6K, pack_frame({"t": FT_REQ, "id": 1, "op": OP_CATALOG_LIST}))
    frame_type, payload = peer.recv()
    assert frame_type == STREAM_FRAME_N6K
    return dict(unpack_frame(payload)[0])


def test_jsonrpc_request_gets_method_not_found(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con) as peer:
        peer.send(STREAM_FRAME_JSONRPC, json.dumps({"jsonrpc": "2.0", "id": "a1", "method": "ping"}).encode())
        frame_type, payload = peer.recv()
        assert frame_type == STREAM_FRAME_JSONRPC
        assert json.loads(payload) == {
            "jsonrpc": "2.0",
            "id": "a1",
            "error": {"code": -32601, "message": "Method not found"},
        }


def test_jsonrpc_notification_is_discarded(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con) as peer:
        peer.send(STREAM_FRAME_JSONRPC, json.dumps({"jsonrpc": "2.0", "method": "ping"}).encode())
        assert list(_catalog_list(peer)["catalogs"]) == ["db"]


def test_unknown_frame_type_closes_the_connection(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con) as peer:
        peer.send(0x00, b"x")
        assert peer.closed()
