import json
import os
import socket
import struct
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Optional

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
    FT_PUSH,
    FT_REQ,
    FT_RESP_CHUNK,
    OP_CATALOG_LIST,
    OP_CATALOGS_CHANGED,
    STREAM_FRAME_HOST,
    STREAM_FRAME_N6K,
)

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")


class _Host:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.events: list[dict[str, Any]] = []
        sock.settimeout(5)

    def send(self, header: dict[str, Any]) -> None:
        frame = pack_frame(header)
        self.sock.sendall(struct.pack(">IB", len(frame), STREAM_FRAME_N6K) + frame)

    def recv(self) -> dict[str, Any]:
        while True:
            n, frame_type = struct.unpack(">IB", self._exactly(5))
            payload = self._exactly(n)
            if frame_type == STREAM_FRAME_HOST:
                self.events.append(json.loads(payload))
                continue
            assert frame_type == STREAM_FRAME_N6K
            header, _ = unpack_frame(payload)
            return dict(header)

    def recv_until(self, match: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
        while not match(frame := self.recv()):
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
def _serving(con: duckdb.DuckDBPyConnection, sql_tail: str) -> Iterator[tuple[_Host, int]]:
    host_sock, served = socket.socketpair()
    fd = served.fileno()
    errors: list[BaseException] = []

    def serve() -> None:
        try:
            con.execute(f"CALL n6k_serve_fd({fd}{sql_tail})").fetchall()
        except BaseException as e:
            errors.append(e)

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    host = _Host(host_sock)
    try:
        assert host.recv()["t"] == FT_HELLO_ACK
        yield host, fd
    finally:
        host_sock.close()
        worker.join(10)
        served.close()
    assert not worker.is_alive(), "serve did not end after the peer closed"
    assert not errors, errors


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    c.execute("ATTACH ':memory:' AS a")
    yield c
    c.close()


def _resolved(side: duckdb.DuckDBPyConnection, fd: int, name: str, error: Optional[str] = None) -> None:
    side.execute("SELECT * FROM n6k_serve_resolved(?, ?, ?)", [fd, name, error]).fetchall()


def _add(side: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    side.execute("SELECT * FROM n6k_serve_add_catalog(?, ?)", [fd, name]).fetchall()


def _pending(host: _Host, ns: int) -> dict[str, Any]:
    return host.recv_until(lambda f: f["t"] == FT_OPEN_PENDING and f["ns"] == ns)


def _settled(host: _Host, ns: int) -> dict[str, Any]:
    return host.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] == ns)


def test_unknown_open_asks_the_host_and_acks_once_served(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        assert "resolving" in _pending(host, 1)["message"]
        assert host.events == [{"resolve": "a"}]
        _add(side, fd, "a")
        assert _settled(host, 1)["t"] == FT_OPEN_ACK
        _resolved(side, fd, "a")


def test_resolved_without_serving_is_not_found(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "nope"})
        _pending(host, 1)
        _resolved(side, fd, "nope")
        err = _settled(host, 1)
        assert err["t"] == FT_OPEN_ERR
        assert "not found" in err["exception_message"]


def test_resolved_with_an_error_carries_its_message(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "pg"})
        _pending(host, 1)
        _resolved(side, fd, "pg", "connection refused")
        err = _settled(host, 1)
        assert err["t"] == FT_OPEN_ERR
        assert "connection refused" in err["exception_message"]


def test_a_failed_name_is_resolved_again_on_its_next_open(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        _pending(host, 1)
        _resolved(side, fd, "a")
        assert _settled(host, 1)["t"] == FT_OPEN_ERR
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "a"})
        _pending(host, 2)
        assert host.events == [{"resolve": "a"}, {"resolve": "a"}]
        _add(side, fd, "a")
        _resolved(side, fd, "a")
        assert _settled(host, 2)["t"] == FT_OPEN_ACK


def test_concurrent_opens_share_one_resolve(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "a"})
        _pending(host, 1)
        _pending(host, 2)
        _add(side, fd, "a")
        _resolved(side, fd, "a")
        assert {_settled(host, 1)["t"], _settled(host, 2)["t"]} == {FT_OPEN_ACK}
        assert host.events == [{"resolve": "a"}]


def test_a_cancelled_open_is_not_settled(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true, open_pending_interval_ms => 50") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "a"})
        _pending(host, 1)
        host.send({"t": FT_CANCEL, "ns": 1, "id": 0})
        _add(side, fd, "a")
        _resolved(side, fd, "a")
        assert _settled(host, 2)["t"] == FT_OPEN_ACK
        # Ask for an unrelated settle as a barrier: nothing for ns 1 may come before it.
        host.send({"t": FT_OPEN, "ns": 3, "catalog": "a"})
        frame = host.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] in (1, 3))
        assert frame["ns"] == 3


def test_pending_beats_continue_while_resolving(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true, open_pending_interval_ms => 50") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        for _ in range(3):
            _pending(host, 1)
        _add(side, fd, "a")
        _resolved(side, fd, "a")
        assert _settled(host, 1)["t"] == FT_OPEN_ACK


def test_resolved_for_a_name_not_being_resolved_raises(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true") as (_, fd):
        with pytest.raises(duckdb.InvalidInputException, match="not being resolved"):
            _resolved(side, fd, "a")


def test_without_resolve_an_unknown_open_fails_as_before(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, "") as (host, _):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        err = _settled(host, 1)
        assert err["t"] == FT_OPEN_ERR
        assert "not served" in err["exception_message"]
        assert host.events == []


def _listed(host: _Host, req_id: int = 99) -> list[str]:
    host.send({"t": FT_REQ, "id": req_id, "op": OP_CATALOG_LIST})
    return list(host.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f["id"] == req_id)["catalogs"])


def _pushed(host: _Host) -> list[str]:
    return list(host.recv_until(lambda f: f["t"] == FT_PUSH and f.get("op") == OP_CATALOGS_CHANGED)["catalogs"])


def test_a_lazy_catalog_is_listed_resolved_on_open_and_then_listed_once(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true, lazy => MAP {'a': NULL}") as (host, fd):
        assert _listed(host) == ["a"]
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        _pending(host, 1)
        assert host.events == [{"resolve": "a"}]
        _add(side, fd, "a")
        assert _settled(host, 1)["t"] == FT_OPEN_ACK
        _resolved(side, fd, "a")
        assert _listed(host) == ["a"]


def test_a_lazy_catalog_stays_listed_when_its_resolve_fails(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true, lazy => MAP {'lazy': NULL}") as (host, fd):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "lazy"})
        _pending(host, 1)
        _resolved(side, fd, "lazy", "not ready")
        assert _settled(host, 1)["t"] == FT_OPEN_ERR
        assert _listed(host) == ["lazy"]
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "lazy"})
        _pending(host, 2)
        assert host.events == [{"resolve": "lazy"}, {"resolve": "lazy"}]


def test_add_and_remove_a_lazy_catalog_push_the_list(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true") as (host, fd):
        side.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, 'later')", [fd]).fetchall()
        assert _pushed(host) == ["later"]
        side.execute("SELECT * FROM n6k_serve_remove_catalog(?, 'later')", [fd]).fetchall()
        assert _pushed(host) == []
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "later"})
        _pending(host, 1)
        assert host.events == [{"resolve": "later"}]
        _resolved(side, fd, "later")


def test_removing_a_resolved_lazy_catalog_stops_listing_it(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", resolve => true, lazy => MAP {'a': NULL}") as (host, fd):
        _add(side, fd, "a")
        assert _pushed(host) == ["a"]
        side.execute("SELECT * FROM n6k_serve_remove_catalog(?, 'a')", [fd]).fetchall()
        assert _pushed(host) == []


def test_lazy_catalogs_need_resolve(con: duckdb.DuckDBPyConnection) -> None:
    host_sock, served = socket.socketpair()
    try:
        with pytest.raises(duckdb.BinderException, match="needs resolve"):
            con.execute(f"CALL n6k_serve_fd({served.fileno()}, lazy => MAP {{'a': NULL}})").fetchall()
        with pytest.raises(duckdb.BinderException, match="already served"):
            con.execute(
                f"CALL n6k_serve_fd({served.fileno()}, 'a', resolve => true, lazy => MAP {{'a': NULL}})"
            ).fetchall()
    finally:
        host_sock.close()
        served.close()
    side = con.cursor()
    with _serving(con, "") as (_, fd):
        with pytest.raises(duckdb.InvalidInputException, match="needs n6k_serve_fd"):
            side.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, 'x')", [fd]).fetchall()
    with _serving(con, ", 'a', resolve => true") as (_, fd):
        with pytest.raises(duckdb.InvalidInputException, match="already served, pending or lazy"):
            side.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, 'a')", [fd]).fetchall()
