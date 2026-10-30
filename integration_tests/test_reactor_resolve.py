import itertools
import json
import os
import queue
import socket
import struct
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Optional

import duckdb
import msgpack
import pytest
from duckdb.sqltypes import UUID, VARCHAR

from _paths import REPO_ROOT

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_CANCEL,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_OPEN_PENDING,
    FT_PUSH,
    FT_REQ,
    FT_RESP_CHUNK,
    N6K_PROTOCOL_VERSION,
    OP_CATALOG_LIST,
    OP_CATALOGS_CHANGED,
    STREAM_FRAME_CLOSE,
    STREAM_FRAME_N6K,
)

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")

_udf_ids = itertools.count()


class _Host:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        sock.settimeout(5)

    def send(self, header: dict[str, Any]) -> None:
        frame = pack_frame(header)
        self.sock.sendall(struct.pack(">IB", len(frame), STREAM_FRAME_N6K) + frame)

    def hello(self) -> dict[str, Any]:
        self.send({"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION})
        return self.recv()

    def recv(self) -> dict[str, Any]:
        n, frame_type = struct.unpack(">IB", self._exactly(5))
        assert frame_type == STREAM_FRAME_N6K
        header, _ = unpack_frame(self._exactly(n))
        return dict(header)

    def recv_close(self) -> dict[str, Any]:
        n, frame_type = struct.unpack(">IB", self._exactly(5))
        assert frame_type == STREAM_FRAME_CLOSE
        return dict(msgpack.unpackb(self._exactly(n)))

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


class _Resolver:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.name = f"resolve_{next(_udf_ids)}"
        self._calls: queue.Queue[str] = queue.Queue()
        self._answers: queue.Queue[Optional[str]] = queue.Queue()
        self._closed = threading.Event()
        con.create_function(
            self.name, self._resolve, [UUID, VARCHAR], VARCHAR, null_handling="special", side_effects=True
        )

    def _resolve(self, session_id: uuid.UUID, catalog: str) -> Optional[str]:
        self._calls.put(catalog)
        while not self._closed.is_set():
            try:
                return self._answers.get(timeout=0.05)
            except queue.Empty:
                pass
        return None

    def called(self) -> str:
        return self._calls.get(timeout=5)

    def calls_so_far(self) -> list[str]:
        calls = []
        while not self._calls.empty():
            calls.append(self._calls.get_nowait())
        return calls

    def answer(self, error: Optional[str] = None) -> None:
        self._answers.put(error)

    def close(self) -> None:
        self._closed.set()


def _authenticate(con: duckdb.DuckDBPyConnection, result: dict[str, Any]) -> str:
    name = f"authenticate_{next(_udf_ids)}"
    con.create_function(
        name,
        lambda session_id, token, caps, request: json.dumps(result),
        [UUID, VARCHAR, duckdb.list_type(VARCHAR), VARCHAR],
        VARCHAR,
        null_handling="special",
        side_effects=True,
    )
    return f", authenticate => '{name}'"


def _sql_tail(sql_tail: str, resolver: Optional[_Resolver]) -> str:
    return sql_tail + (f", resolve => '{resolver.name}'" if resolver else "")


@contextmanager
def _serving(
    con: duckdb.DuckDBPyConnection, sql_tail: str, resolver: Optional[_Resolver] = None
) -> Iterator[tuple[_Host, uuid.UUID]]:
    host_sock, served = socket.socketpair()
    session_id = con.execute("SELECT session_id FROM n6k_serve_create_session(?)", [served.fileno()]).fetchone()[0]
    errors: list[BaseException] = []
    sql = f"CALL n6k_serve_fd('{session_id}'::UUID{_sql_tail(sql_tail, resolver)})"

    def serve() -> None:
        try:
            con.execute(sql).fetchall()
        except BaseException as e:
            errors.append(e)

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    host = _Host(host_sock)
    try:
        assert host.hello()["t"] == FT_HELLO_ACK
        yield host, session_id
    finally:
        host_sock.close()
        if resolver:
            resolver.close()
        worker.join(10)
        served.close()
    assert not worker.is_alive(), "serve did not end after the peer closed"
    assert not errors, errors


def _rejected(con: duckdb.DuckDBPyConnection, sql_tail: str, resolver: Optional[_Resolver] = None) -> dict[str, Any]:
    host_sock, served = socket.socketpair()
    session_id = con.execute("SELECT session_id FROM n6k_serve_create_session(?)", [served.fileno()]).fetchone()[0]
    sql = f"CALL n6k_serve_fd('{session_id}'::UUID{_sql_tail(sql_tail, resolver)})"

    def serve() -> None:
        try:
            con.execute(sql).fetchall()
        except BaseException:
            pass

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    host = _Host(host_sock)
    try:
        err = host.hello()
        assert err["t"] == FT_HELLO_ERR
        assert host.recv_close()["code"] == 1011
        return err
    finally:
        host_sock.close()
        worker.join(10)
        served.close()


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    c.execute("ATTACH ':memory:' AS a")
    yield c
    c.close()


@pytest.fixture
def resolver(con: duckdb.DuckDBPyConnection) -> _Resolver:
    return _Resolver(con)


def _add(side: duckdb.DuckDBPyConnection, session_id: uuid.UUID, name: str) -> None:
    side.execute("SELECT * FROM n6k_serve_add_catalog(?, ?)", [session_id, name]).fetchall()


def _pending(host: _Host, ns: int) -> dict[str, Any]:
    return host.recv_until(lambda f: f["t"] == FT_OPEN_PENDING and f["ns"] == ns)


def _settled(host: _Host, ns: int) -> dict[str, Any]:
    return host.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] == ns)


def test_unknown_open_asks_the_host_and_acks_once_served(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    side = con.cursor()
    with _serving(con, "", resolver) as (host, session_id):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        assert "resolving" in _pending(host, 1)["message"]
        assert resolver.called() == "a"
        _add(side, session_id, "a")
        assert _settled(host, 1)["t"] == FT_OPEN_ACK
        resolver.answer()


def test_resolved_without_serving_is_not_found(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    with _serving(con, "", resolver) as (host, _):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "nope"})
        _pending(host, 1)
        resolver.answer()
        err = _settled(host, 1)
        assert err["t"] == FT_OPEN_ERR
        assert "not found" in err["exception_message"]


def test_resolved_with_an_error_carries_its_message(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    with _serving(con, "", resolver) as (host, _):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "pg"})
        _pending(host, 1)
        resolver.answer("connection refused")
        err = _settled(host, 1)
        assert err["t"] == FT_OPEN_ERR
        assert "connection refused" in err["exception_message"]


def test_a_failed_name_is_resolved_again_on_its_next_open(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    side = con.cursor()
    with _serving(con, "", resolver) as (host, session_id):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        _pending(host, 1)
        resolver.answer()
        assert _settled(host, 1)["t"] == FT_OPEN_ERR
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "a"})
        _pending(host, 2)
        assert [resolver.called(), resolver.called()] == ["a", "a"]
        _add(side, session_id, "a")
        resolver.answer()
        assert _settled(host, 2)["t"] == FT_OPEN_ACK


def test_concurrent_opens_share_one_resolve(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    side = con.cursor()
    with _serving(con, "", resolver) as (host, session_id):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "a"})
        _pending(host, 1)
        _pending(host, 2)
        _add(side, session_id, "a")
        resolver.answer()
        assert {_settled(host, 1)["t"], _settled(host, 2)["t"]} == {FT_OPEN_ACK}
        assert resolver.calls_so_far() == ["a"]


def test_a_cancelled_open_is_not_settled(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    side = con.cursor()
    with _serving(con, ", open_pending_interval_ms => 50", resolver) as (host, session_id):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "a"})
        _pending(host, 1)
        host.send({"t": FT_CANCEL, "ns": 1, "id": 0})
        _add(side, session_id, "a")
        resolver.answer()
        assert _settled(host, 2)["t"] == FT_OPEN_ACK
        # Ask for an unrelated settle as a barrier: nothing for ns 1 may come before it.
        host.send({"t": FT_OPEN, "ns": 3, "catalog": "a"})
        frame = host.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] in (1, 3))
        assert frame["ns"] == 3


def test_pending_beats_continue_while_resolving(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    side = con.cursor()
    with _serving(con, ", open_pending_interval_ms => 50", resolver) as (host, session_id):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        for _ in range(3):
            _pending(host, 1)
        _add(side, session_id, "a")
        resolver.answer()
        assert _settled(host, 1)["t"] == FT_OPEN_ACK


def test_without_resolve_an_unknown_open_fails_as_before(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, "") as (host, _):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        err = _settled(host, 1)
        assert err["t"] == FT_OPEN_ERR
        assert "not served" in err["exception_message"]


def _listed(host: _Host, req_id: int = 99) -> list[str]:
    host.send({"t": FT_REQ, "id": req_id, "op": OP_CATALOG_LIST})
    return list(host.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f["id"] == req_id)["catalogs"])


def _pushed(host: _Host) -> list[str]:
    return list(host.recv_until(lambda f: f["t"] == FT_PUSH and f.get("op") == OP_CATALOGS_CHANGED)["catalogs"])


def test_a_lazy_catalog_is_listed_resolved_on_open_and_then_listed_once(
    con: duckdb.DuckDBPyConnection, resolver: _Resolver
) -> None:
    side = con.cursor()
    with _serving(con, _authenticate(con, {"lazy": {"a": None}}), resolver) as (host, session_id):
        assert _listed(host) == ["a"]
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        _pending(host, 1)
        assert resolver.called() == "a"
        _add(side, session_id, "a")
        assert _settled(host, 1)["t"] == FT_OPEN_ACK
        resolver.answer()
        assert _listed(host) == ["a"]


def test_a_lazy_catalog_stays_listed_when_its_resolve_fails(
    con: duckdb.DuckDBPyConnection, resolver: _Resolver
) -> None:
    with _serving(con, _authenticate(con, {"lazy": {"lazy": None}}), resolver) as (host, _):
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "lazy"})
        _pending(host, 1)
        resolver.answer("not ready")
        assert _settled(host, 1)["t"] == FT_OPEN_ERR
        assert _listed(host) == ["lazy"]
        host.send({"t": FT_OPEN, "ns": 2, "catalog": "lazy"})
        _pending(host, 2)
        assert [resolver.called(), resolver.called()] == ["lazy", "lazy"]


def test_add_and_remove_a_lazy_catalog_push_the_list(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    side = con.cursor()
    with _serving(con, "", resolver) as (host, session_id):
        side.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, 'later')", [session_id]).fetchall()
        assert _pushed(host) == ["later"]
        side.execute("SELECT * FROM n6k_serve_remove_catalog(?, 'later')", [session_id]).fetchall()
        assert _pushed(host) == []
        host.send({"t": FT_OPEN, "ns": 1, "catalog": "later"})
        _pending(host, 1)
        assert resolver.called() == "later"
        resolver.answer()


def test_removing_a_resolved_lazy_catalog_stops_listing_it(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    side = con.cursor()
    with _serving(con, _authenticate(con, {"lazy": {"a": None}}), resolver) as (host, session_id):
        _add(side, session_id, "a")
        assert _pushed(host) == ["a"]
        side.execute("SELECT * FROM n6k_serve_remove_catalog(?, 'a')", [session_id]).fetchall()
        assert _pushed(host) == []


def test_lazy_catalogs_need_resolve(con: duckdb.DuckDBPyConnection, resolver: _Resolver) -> None:
    assert _rejected(con, _authenticate(con, {"lazy": {"a": None}}))["exception_message"] == "internal error"
    err = _rejected(con, ", 'a'" + _authenticate(con, {"lazy": {"a": None}}), resolver)
    assert err["exception_message"] == "internal error"
    side = con.cursor()
    with _serving(con, "") as (_, session_id):
        with pytest.raises(duckdb.InvalidInputException, match="needs a resolve callback"):
            side.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, 'x')", [session_id]).fetchall()
    with _serving(con, ", 'a'", resolver) as (_, session_id):
        with pytest.raises(duckdb.InvalidInputException, match="already served, pending or lazy"):
            side.execute("SELECT * FROM n6k_serve_add_lazy_catalog(?, 'a')", [session_id]).fetchall()
