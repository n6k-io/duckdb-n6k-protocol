"""`n6k_serve_fd(..., pending => MAP {...})`: OPEN of a pending catalog parks on FT_OPEN_PENDING
until `n6k_serve_open_ready` / `n6k_serve_open_failed` settles it.

Drives the reactor directly over a socketpair, no websocket and no test server.

Run:
    uv run pytest integration_tests/test_reactor_open_pending.py -v
"""

import os
import socket
import struct
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import pytest

from _paths import REPO_ROOT

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_CANCEL,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_OPEN_PENDING,
    FT_PING,
    FT_PONG,
    FT_PUSH,
    FT_REQ,
    OP_CATALOG_LIST,
    STREAM_FRAME_N6K,
)

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")
BEAT_MS = 100

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")


class _Peer:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        sock.settimeout(5)

    def send(self, header: dict[str, Any]) -> None:
        frame = pack_frame(header)
        self.sock.sendall(struct.pack(">IB", len(frame), STREAM_FRAME_N6K) + frame)

    def recv(self) -> dict[str, Any]:
        n, frame_type = struct.unpack(">IB", self._exactly(5))
        assert frame_type == STREAM_FRAME_N6K
        header, _ = unpack_frame(self._exactly(n))
        return dict(header)

    def recv_skipping_pings(self) -> dict[str, Any]:
        while (frame := self.recv())["t"] == FT_PING:
            pass
        return frame

    def _exactly(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            assert chunk, "reactor closed the socket"
            buf += chunk
        return buf


@contextmanager
def _serving(con: duckdb.DuckDBPyConnection, sql_tail: str) -> Iterator[tuple[_Peer, int]]:
    peer_sock, served = socket.socketpair()
    fd = served.fileno()
    errors: list[BaseException] = []

    def serve() -> None:
        try:
            con.execute(f"CALL n6k_serve_fd({fd}{sql_tail})").fetchall()
        except BaseException as e:
            errors.append(e)

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    peer = _Peer(peer_sock)
    try:
        assert peer.recv()["t"] == FT_HELLO_ACK
        yield peer, fd
    finally:
        peer_sock.close()
        worker.join(10)
        served.close()
    assert not worker.is_alive(), "serve did not end after the peer closed"
    assert not errors, errors


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    yield c
    c.close()


PENDING_LATE = f", pending => MAP {{'late': 'warming'}}, open_pending_interval_ms => {BEAT_MS}"


def test_open_parks_on_heartbeats_until_ready_then_acks(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, PENDING_LATE) as (peer, fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "late"})
        t0 = time.monotonic()
        beats = []
        while len(beats) < 4:
            frame = peer.recv_skipping_pings()
            assert frame == {"t": FT_OPEN_PENDING, "ns": 1, "message": "warming"}
            beats.append(time.monotonic() - t0)
        assert beats[0] < 0.5
        gaps = [b - a for a, b in zip(beats, beats[1:])]
        assert all(0.5 * BEAT_MS / 1000 < g < 1.0 for g in gaps), gaps

        side.execute("ATTACH ':memory:' AS late")
        side.execute("SELECT * FROM n6k_serve_open_ready(?, 'late')", [fd]).fetchall()
        while (frame := peer.recv_skipping_pings())["t"] == FT_OPEN_PENDING:
            pass
        assert frame["t"] == FT_OPEN_ACK
        assert frame["ns"] == 1

        # Ready now: a fresh OPEN is ACKed straight away.
        peer.send({"t": FT_OPEN, "ns": 2, "catalog": "late"})
        assert peer.recv_skipping_pings()["t"] == FT_OPEN_ACK


def test_catalog_list_includes_a_pending_catalog(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, PENDING_LATE) as (peer, _fd):
        peer.send({"t": FT_REQ, "id": 1, "op": OP_CATALOG_LIST})
        frame = peer.recv_skipping_pings()
        assert list(frame["catalogs"]) == ["late"]


def test_failed_errs_every_parked_open_and_every_later_one(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, PENDING_LATE) as (peer, fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "late"})
        peer.send({"t": FT_OPEN, "ns": 2, "catalog": "late"})
        side.execute("SELECT * FROM n6k_serve_open_failed(?, 'late', 'worker died')", [fd]).fetchall()
        errs = {}
        while len(errs) < 2:
            frame = peer.recv_skipping_pings()
            if frame["t"] == FT_OPEN_PENDING:
                continue
            assert frame["t"] == FT_OPEN_ERR
            errs[frame["ns"]] = frame["exception_message"]
        assert set(errs) == {1, 2}
        assert all("worker died" in m for m in errs.values())

        assert peer.recv_skipping_pings()["t"] == FT_PUSH  # CATALOGS_CHANGED: "late" left the list
        peer.send({"t": FT_OPEN, "ns": 3, "catalog": "late"})
        frame = peer.recv_skipping_pings()
        assert frame["t"] == FT_OPEN_ERR
        assert "worker died" in frame["exception_message"]


def test_duplicate_open_while_parked_is_refused(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, PENDING_LATE) as (peer, _fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "late"})
        assert peer.recv_skipping_pings()["t"] == FT_OPEN_PENDING
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "late"})
        while (frame := peer.recv_skipping_pings())["t"] == FT_OPEN_PENDING:
            pass
        assert frame["t"] == FT_OPEN_ERR
        assert "duplicate" in frame["exception_message"]


def test_a_cancelled_parked_open_gets_no_ack(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, PENDING_LATE) as (peer, fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "late"})
        assert peer.recv_skipping_pings()["t"] == FT_OPEN_PENDING
        peer.send({"t": FT_CANCEL, "id": 0, "ns": 1})
        side.execute("ATTACH ':memory:' AS late")
        side.execute("SELECT * FROM n6k_serve_open_ready(?, 'late')", [fd]).fetchall()
        # The PONG fences everything the reactor queued before it.
        time.sleep(0.5)
        peer.send({"t": FT_PING, "id": 9})
        while (frame := peer.recv())["t"] != FT_PONG:
            assert frame["t"] in (FT_OPEN_PENDING, FT_PING), frame


def test_ready_requires_the_catalog_to_be_attached(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, PENDING_LATE) as (_peer, fd):
        with pytest.raises(duckdb.BinderException, match="not attached"):
            side.execute("SELECT * FROM n6k_serve_open_ready(?, 'late')", [fd]).fetchall()


def test_settling_an_unknown_or_settled_catalog_raises(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, PENDING_LATE) as (_peer, fd):
        with pytest.raises(duckdb.InvalidInputException, match="not pending"):
            side.execute("SELECT * FROM n6k_serve_open_failed(?, 'other', 'x')", [fd]).fetchall()
        side.execute("SELECT * FROM n6k_serve_open_failed(?, 'late', 'x')", [fd]).fetchall()
        with pytest.raises(duckdb.InvalidInputException, match="not pending"):
            side.execute("SELECT * FROM n6k_serve_open_failed(?, 'late', 'x')", [fd]).fetchall()


def test_ready_before_the_serve_registers_waits_for_it(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    side.execute("ATTACH ':memory:' AS late")
    peer_sock, served = socket.socketpair()
    fd = served.fileno()
    resolved: list[BaseException | None] = []

    def resolve() -> None:
        try:
            side.execute("SELECT * FROM n6k_serve_open_ready(?, 'late')", [fd]).fetchall()
            resolved.append(None)
        except BaseException as e:
            resolved.append(e)

    resolver = threading.Thread(target=resolve, daemon=True)
    resolver.start()
    time.sleep(0.3)
    worker = threading.Thread(
        target=lambda: con.execute(f"CALL n6k_serve_fd({fd}{PENDING_LATE})").fetchall(), daemon=True
    )
    worker.start()
    peer = _Peer(peer_sock)
    try:
        assert peer.recv()["t"] == FT_HELLO_ACK
        resolver.join(10)
        assert resolved == [None]
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "late"})
        assert peer.recv_skipping_pings()["t"] == FT_OPEN_ACK
    finally:
        peer_sock.close()
        worker.join(10)
        served.close()


def test_pending_only_serves_no_other_attached_catalog(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("ATTACH ':memory:' AS other")
    with _serving(con, PENDING_LATE) as (peer, _fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "other"})
        frame = peer.recv_skipping_pings()
        assert frame["t"] == FT_OPEN_ERR
        assert "not served" in frame["exception_message"]


def test_a_pending_name_also_served_ready_is_refused(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("ATTACH ':memory:' AS late")
    with pytest.raises(duckdb.BinderException, match="more than once"):
        con.execute(f"CALL n6k_serve_fd(99, 'late'{PENDING_LATE})").fetchall()
