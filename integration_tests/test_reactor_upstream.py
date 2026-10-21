import json
import os
import socket
import struct
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Optional

import duckdb
import pytest

from _paths import REPO_ROOT

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    CAP_CATALOG_PLANS,
    DEFAULT_BATCH_CREDITS,
    FT_CANCEL,
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_HELLO_PENDING,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_PING,
    FT_PONG,
    FT_PUSH,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    MAX_CONCURRENT_REQS,
    N6K_PROTOCOL_VERSION,
    OP_CATALOG_LIST,
    OP_CATALOGS_CHANGED,
    OP_QUERY,
    STREAM_FRAME_HOST,
    STREAM_FRAME_N6K,
)

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")


class _Peer:
    def __init__(self, sock: socket.socket, timeout: float = 10) -> None:
        self.sock = sock
        self.events: list[dict[str, Any]] = []
        sock.settimeout(timeout)

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

    def recv_push(self) -> list[str]:
        frame = self.recv_until(lambda f: f["t"] == FT_PUSH and f.get("op") == OP_CATALOGS_CHANGED)
        return list(frame["catalogs"])

    def _exactly(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            assert chunk, "peer closed the socket"
            buf += chunk
        return buf


def _connect() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    return c


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
        worker.join(15)
        served.close()
    assert not worker.is_alive(), "serve did not end after the peer closed"
    assert not errors, errors


class _UpstreamServer:
    def __init__(self, catalogs: list[str]) -> None:
        self.con = _connect()
        for name in catalogs:
            self.con.execute(f"ATTACH ':memory:' AS {name}")
            self.con.execute(f"CREATE TABLE {name}.main.t AS SELECT range AS i FROM range(100000)")
        self.link, self._served = socket.socketpair()
        self.fd = self._served.fileno()
        self.errors: list[BaseException] = []
        names = ", ".join(f"'{n}'" for n in catalogs)

        def serve() -> None:
            try:
                row = self.con.execute("SELECT capabilities FROM n6k_handshake(?)", [self.fd]).fetchone()
                assert row is not None
                caps = ", ".join(f"'{c}'" for c in row[0])
                self.con.execute(f"CALL n6k_serve_fd({self.fd}, {names}, capabilities => [{caps}])").fetchall()
            except BaseException as e:
                self.errors.append(e)

        self._worker = threading.Thread(target=serve, daemon=True)
        self._worker.start()

    def kill(self) -> None:
        self._served.shutdown(socket.SHUT_RDWR)

    def close(self) -> None:
        try:
            self._served.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._worker.join(15)
        self._served.close()
        self.link.close()
        self.con.close()


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = _connect()
    c.execute("ATTACH ':memory:' AS x")
    c.execute("CREATE TABLE x.main.t AS SELECT 7 AS i")
    yield c
    c.close()


@pytest.fixture
def upstream() -> Iterator[_UpstreamServer]:
    up = _UpstreamServer(["a"])
    yield up
    up.close()


def _hello(side: duckdb.DuckDBPyConnection, up_fd: int, token: str = "") -> tuple[list[str], list[str]]:
    row = side.execute("SELECT catalogs, capabilities FROM n6k_upstream_hello(?, ?)", [up_fd, token]).fetchone()
    assert row is not None
    return list(row[0]), list(row[1])


def _add_upstream(side: duckdb.DuckDBPyConnection, fd: int, up_fd: int, catalogs: list[str]) -> list[str]:
    row = side.execute("SELECT skipped FROM n6k_serve_add_upstream(?, ?, ?)", [fd, up_fd, catalogs]).fetchone()
    assert row is not None
    return list(row[0])


def _status(side: duckdb.DuckDBPyConnection, fd: int, up_fd: int) -> tuple[bool, Optional[str]]:
    row = side.execute("SELECT open, reason FROM n6k_serve_upstream_status(?, ?)", [fd, up_fd]).fetchone()
    assert row is not None
    return bool(row[0]), row[1]


def _open(peer: _Peer, ns: int, catalog: str) -> dict[str, Any]:
    peer.send({"t": FT_OPEN, "ns": ns, "catalog": catalog})
    return peer.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] == ns)


def _query(peer: _Peer, ns: int, req_id: int, sql: str) -> list[dict[str, Any]]:
    peer.send({"t": FT_REQ, "id": req_id, "ns": ns, "op": OP_QUERY, "sql": sql})
    frames = []
    while True:
        frame = peer.recv_until(lambda f: f.get("ns") == ns and f.get("id") == req_id)
        frames.append(frame)
        if frame["t"] in (FT_RESP_END, FT_ERR):
            return frames


def _attach_upstream(side: duckdb.DuckDBPyConnection, peer: _Peer, fd: int, up: _UpstreamServer) -> None:
    catalogs, _ = _hello(side, up.link.fileno())
    assert catalogs == ["a"]
    assert _add_upstream(side, fd, up.link.fileno(), catalogs) == []
    assert peer.recv_push() == ["x", "a"]


def _wait_closed(side: duckdb.DuckDBPyConnection, fd: int, up_fd: int, timeout: float = 15) -> str:
    deadline = time.monotonic() + timeout
    while True:
        is_open, reason = _status(side, fd, up_fd)
        if not is_open:
            assert reason is not None
            return reason
        assert time.monotonic() < deadline, "upstream never closed"
        time.sleep(0.05)


def test_upstream_catalog_is_listed_opened_and_queried_alongside_a_local_one(
    con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer
) -> None:
    side = con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        assert _open(peer, 2, "x")["t"] == FT_OPEN_ACK
        assert _query(peer, 1, 1, "SELECT count(*) AS n FROM a.main.t")[-1]["t"] == FT_RESP_END
        assert _query(peer, 2, 1, "SELECT * FROM x.main.t")[-1]["t"] == FT_RESP_END
        peer.send({"t": FT_REQ, "id": 9, "op": OP_CATALOG_LIST})
        chunk = peer.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f["id"] == 9)
        assert chunk["catalogs"] == ["x", "a"]
        assert _status(side, fd, upstream.link.fileno()) == (True, None)


def test_catalog_plans_is_granted_for_a_local_catalog_but_not_a_forwarded_one(
    con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer
) -> None:
    side = con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        for ns, catalog, granted in ((1, "x", [CAP_CATALOG_PLANS]), (2, "a", [])):
            peer.send({"t": FT_OPEN, "ns": ns, "catalog": catalog, "capabilities": [CAP_CATALOG_PLANS]})
            ack = peer.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] == ns)
            assert ack["t"] == FT_OPEN_ACK
            assert ack["capabilities"] == granted


def test_a_name_already_served_is_skipped(con: duckdb.DuckDBPyConnection) -> None:
    up = _UpstreamServer(["a", "x"])
    side = con.cursor()
    try:
        with _serving(con, ", 'x'") as (peer, fd):
            catalogs, _ = _hello(side, up.link.fileno())
            assert sorted(catalogs) == ["a", "x"]
            assert _add_upstream(side, fd, up.link.fileno(), catalogs) == ["x"]
            assert peer.recv_push() == ["x", "a"]
            assert _open(peer, 1, "x")["t"] == FT_OPEN_ACK
    finally:
        up.close()


def test_the_upstreams_catalog_changes_follow(con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer) -> None:
    side = con.cursor()
    up_side = upstream.con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        up_side.execute("ATTACH ':memory:' AS d")
        up_side.execute("SELECT * FROM n6k_serve_add_catalog(?, 'd')", [upstream.fd]).fetchall()
        assert peer.recv_push() == ["x", "a", "d"]
        assert _open(peer, 1, "d")["t"] == FT_OPEN_ACK
        up_side.execute("SELECT * FROM n6k_serve_remove_catalog(?, 'a')", [upstream.fd]).fetchall()
        assert peer.recv_push() == ["x", "d"]


def test_upstream_eof_fails_its_sessions_with_the_reason(
    con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer
) -> None:
    side = con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        assert _open(peer, 2, "x")["t"] == FT_OPEN_ACK
        # Stalls once its batch credits run out, so it is still in flight when the upstream dies.
        peer.send({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT * FROM a.main.t", "_batch_rows": 1})
        peer.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f.get("ns") == 1)
        upstream.kill()
        err = peer.recv_until(lambda f: f["t"] == FT_ERR and f.get("ns") == 1 and f["id"] == 1)
        assert 'upstream for catalog "a" closed' in err["exception_message"]
        assert peer.recv_push() == ["x"]
        assert "closed" in _wait_closed(side, fd, upstream.link.fileno())
        assert _open(peer, 3, "a")["t"] == FT_OPEN_ERR
        assert _query(peer, 2, 1, "SELECT * FROM x.main.t")[-1]["t"] == FT_RESP_END


def test_remove_upstream_fails_its_sessions_as_removed_by_host(
    con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer
) -> None:
    side = con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        side.execute("SELECT * FROM n6k_serve_remove_upstream(?, ?)", [fd, upstream.link.fileno()]).fetchall()
        assert peer.recv_push() == ["x"]
        assert _wait_closed(side, fd, upstream.link.fileno()) == "removed by host"
        err = _query(peer, 1, 1, "SELECT 1")[-1]
        assert err["t"] == FT_ERR
        assert "unknown session" in err["exception_message"]


def test_local_catalog_calls_refuse_an_upstream_name(con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer) -> None:
    side = con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        with pytest.raises(duckdb.InvalidInputException, match="served by an upstream"):
            side.execute("SELECT * FROM n6k_serve_remove_catalog(?, 'a')", [fd]).fetchall()
        with pytest.raises(duckdb.InvalidInputException, match="served by an upstream"):
            side.execute("SELECT * FROM n6k_serve_pause(?, 'a')", [fd]).fetchall()


def test_resolve_can_add_an_upstream_for_a_parked_open(
    con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer
) -> None:
    side = con.cursor()
    with _serving(con, ", 'x', resolve => true") as (peer, fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
        peer.recv_until(lambda f: f.get("ns") == 1)
        assert peer.events == [{"resolve": "a"}]
        catalogs, _ = _hello(side, upstream.link.fileno())
        _add_upstream(side, fd, upstream.link.fileno(), catalogs)
        side.execute("SELECT * FROM n6k_serve_resolved(?, 'a', NULL)", [fd]).fetchall()
        assert peer.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] == 1)["t"] == FT_OPEN_ACK
        assert _query(peer, 1, 1, "SELECT count(*) FROM a.main.t")[-1]["t"] == FT_RESP_END


def test_a_detached_forwarded_session_is_closed_upstream_too(
    con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer
) -> None:
    side = con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        peer.send({"t": FT_CANCEL, "ns": 1, "id": 0})
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK


class _ScriptedUpstream:
    def __init__(self) -> None:
        self.link, theirs = socket.socketpair()
        self.peer = _Peer(theirs, timeout=30)
        self.thread: Optional[threading.Thread] = None

    def run(self, script: Callable[[_Peer], None]) -> None:
        self.thread = threading.Thread(target=script, args=(self.peer,), daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.peer.sock.close()
        self.link.close()
        if self.thread is not None:
            self.thread.join(10)


def _answer_hello(peer: _Peer, catalogs: list[str]) -> None:
    hello = peer.recv()
    assert hello["t"] == FT_HELLO
    peer.send({"t": FT_HELLO_ACK, "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": []})
    req = peer.recv()
    assert req["op"] == OP_CATALOG_LIST
    peer.send({"t": FT_RESP_CHUNK, "id": req["id"], "catalogs": catalogs})
    peer.send({"t": FT_RESP_END, "id": req["id"]})


def test_hello_waits_through_a_long_hello_pending(con: duckdb.DuckDBPyConnection) -> None:
    scripted = _ScriptedUpstream()

    def script(peer: _Peer) -> None:
        assert peer.recv()["t"] == FT_HELLO
        for _ in range(7):
            peer.send({"t": FT_HELLO_PENDING, "message": "starting"})
            time.sleep(1)
        peer.send({"t": FT_HELLO_ACK, "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": []})
        req = peer.recv()
        peer.send({"t": FT_PING, "id": 1})
        assert peer.recv()["t"] == FT_PONG
        peer.send({"t": FT_RESP_CHUNK, "id": req["id"], "catalogs": ["a"]})
        peer.send({"t": FT_RESP_END, "id": req["id"]})

    scripted.run(script)
    try:
        assert _hello(con, scripted.link.fileno())[0] == ["a"]
    finally:
        scripted.close()


def test_hello_err_is_raised_with_its_message(con: duckdb.DuckDBPyConnection) -> None:
    scripted = _ScriptedUpstream()

    def script(peer: _Peer) -> None:
        peer.recv()
        peer.send({"t": FT_HELLO_ERR, "exception_type": "InvalidInputException", "exception_message": "bad token"})

    scripted.run(script)
    try:
        with pytest.raises(duckdb.IOException, match="bad token"):
            _hello(con, scripted.link.fileno())
    finally:
        scripted.close()


def test_a_silent_upstream_is_dropped_as_a_ping_timeout(con: duckdb.DuckDBPyConnection) -> None:
    scripted = _ScriptedUpstream()
    scripted.run(lambda peer: _answer_hello(peer, ["a"]))
    side = con.cursor()
    try:
        with _serving(con, ", 'x', ping_interval_ms => 200") as (peer, fd):
            catalogs, _ = _hello(side, scripted.link.fileno())
            _add_upstream(side, fd, scripted.link.fileno(), catalogs)
            reason = _wait_closed(side, fd, scripted.link.fileno(), timeout=20)
            assert "ping timeout" in reason
            assert peer.recv_until(lambda f: f["t"] == FT_PUSH and f["catalogs"] == ["x"])
    finally:
        scripted.close()


def test_an_upstreams_frames_on_a_session_not_forwarded_to_it_are_dropped(con: duckdb.DuckDBPyConnection) -> None:
    scripted = _ScriptedUpstream()

    def script(peer: _Peer) -> None:
        _answer_hello(peer, ["a"])
        opened = peer.recv_until(lambda f: f["t"] == FT_OPEN)
        ack = {
            "t": FT_OPEN_ACK,
            "max_concurrent_reqs": MAX_CONCURRENT_REQS,
            "default_batch_credits": DEFAULT_BATCH_CREDITS,
            "capabilities": [],
        }
        peer.send({**ack, "ns": opened["ns"]})
        peer.send({**ack, "ns": 7})
        peer.send({"t": FT_RESP_END, "ns": 2, "id": 1})
        peer.send({"t": FT_RESP_END, "ns": opened["ns"], "id": 99})

    scripted.run(script)
    side = con.cursor()
    try:
        with _serving(con, ", 'x'") as (peer, fd):
            catalogs, _ = _hello(side, scripted.link.fileno())
            _add_upstream(side, fd, scripted.link.fileno(), catalogs)
            assert peer.recv_push() == ["x", "a"]
            assert _open(peer, 2, "x")["t"] == FT_OPEN_ACK
            assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
            seen = []
            while not ((frame := peer.recv()).get("ns") == 1 and frame.get("id") == 99):
                seen.append(frame)
            assert [f for f in seen if f.get("ns") in (2, 7)] == []
            assert _query(peer, 2, 1, "SELECT 1 AS n")[-1]["t"] == FT_RESP_END
    finally:
        scripted.close()


def test_add_upstream_takes_over_a_lazy_name_which_reverts_when_the_upstream_dies(
    con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer
) -> None:
    side = con.cursor()
    with _serving(con, ", 'x', resolve => true, lazy => MAP {'a': NULL}") as (peer, fd):
        catalogs, _ = _hello(side, upstream.link.fileno())
        assert _add_upstream(side, fd, upstream.link.fileno(), catalogs) == []
        assert peer.recv_push() == ["x", "a"]
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        upstream.kill()
        assert peer.recv_push() == ["x", "a"]
        _wait_closed(side, fd, upstream.link.fileno())
        peer.send({"t": FT_OPEN, "ns": 2, "catalog": "a"})
        peer.recv_until(lambda f: f.get("ns") == 2)
        assert peer.events == [{"resolve": "a"}]
        side.execute("SELECT * FROM n6k_serve_resolved(?, 'a', NULL)", [fd]).fetchall()


def _add_ranked(side: duckdb.DuckDBPyConnection, fd: int, up: _UpstreamServer, rank: int) -> list[str]:
    catalogs, _ = _hello(side, up.link.fileno())
    row = side.execute(
        "SELECT skipped FROM n6k_serve_add_upstream(?, ?, ?, rank => ?)", [fd, up.link.fileno(), catalogs, rank]
    ).fetchone()
    assert row is not None
    return list(row[0])


def test_a_lazy_name_of_better_rank_keeps_the_name_from_an_upstream(con: duckdb.DuckDBPyConnection) -> None:
    up = _UpstreamServer(["a", "b"])
    side = con.cursor()
    try:
        with _serving(con, ", 'x', resolve => true, lazy => MAP {'a': 1}") as (peer, fd):
            assert _add_ranked(side, fd, up, 2) == ["a"]
            assert peer.recv_push() == ["x", "b", "a"]
            peer.send({"t": FT_OPEN, "ns": 1, "catalog": "a"})
            peer.recv_until(lambda f: f.get("ns") == 1)
            assert peer.events == [{"resolve": "a"}]
            side.execute("SELECT * FROM n6k_serve_resolved(?, 'a', NULL)", [fd]).fetchall()
    finally:
        up.close()


def test_an_upstream_of_better_rank_takes_a_lazy_name(con: duckdb.DuckDBPyConnection) -> None:
    up = _UpstreamServer(["a"])
    side = con.cursor()
    try:
        with _serving(con, ", resolve => true, lazy => MAP {'a': 5}") as (peer, fd):
            assert _add_ranked(side, fd, up, 2) == []
            assert peer.recv_push() == ["a"]
            assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
            assert peer.events == []
    finally:
        up.close()


def _assert_session_gone(peer: _Peer, ns: int) -> None:
    peer.send({"t": FT_REQ, "id": 99, "ns": ns, "op": OP_QUERY, "sql": "SELECT 1"})
    err = peer.recv_until(lambda f: f.get("ns") == ns and f.get("id") == 99)
    assert err["t"] == FT_ERR and "unknown session" in err["exception_message"], err


def test_the_better_ranked_upstream_wins_and_a_rank_change_moves_the_name(con: duckdb.DuckDBPyConnection) -> None:
    up1 = _UpstreamServer(["a", "b"])
    up2 = _UpstreamServer(["b", "c"])
    side = con.cursor()
    try:
        with _serving(con, "") as (peer, fd):
            assert _add_ranked(side, fd, up1, 2) == []
            assert peer.recv_push() == ["a", "b"]
            assert _open(peer, 1, "b")["t"] == FT_OPEN_ACK
            # Added later but ranked better: it takes b, and the session on up1's b is closed.
            assert _add_ranked(side, fd, up2, 1) == []
            assert peer.recv_push() == ["a", "b", "c"]
            _assert_session_gone(peer, 1)
            assert _open(peer, 2, "b")["t"] == FT_OPEN_ACK
            row = side.execute(
                "SELECT skipped FROM n6k_serve_set_upstream_rank(?, ?, 0)", [fd, up1.link.fileno()]
            ).fetchone()
            assert row is not None and list(row[0]) == []
            peer.recv_push()
            _assert_session_gone(peer, 2)
            assert _open(peer, 3, "b")["t"] == FT_OPEN_ACK
    finally:
        up1.close()
        up2.close()


def test_a_local_catalog_beats_every_upstream(con: duckdb.DuckDBPyConnection, upstream: _UpstreamServer) -> None:
    side = con.cursor()
    with _serving(con, ", 'x'") as (peer, fd):
        _attach_upstream(side, peer, fd, upstream)
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        side.execute("ATTACH ':memory:' AS a")
        side.execute("SELECT * FROM n6k_serve_add_catalog(?, 'a')", [fd]).fetchall()
        peer.recv_push()
        _assert_session_gone(peer, 1)
        assert _open(peer, 2, "a")["t"] == FT_OPEN_ACK
        # The local a has no table t; the upstream's did.
        assert _query(peer, 2, 1, "SELECT count(*) FROM a.main.t")[-1]["t"] == FT_ERR
    second = _UpstreamServer(["x"])
    try:
        with _serving(con, ", 'x'") as (peer, fd):
            assert _add_ranked(side, fd, second, -100) == ["x"]
    finally:
        second.close()


def test_when_the_winning_upstream_dies_the_next_ranked_one_takes_the_name(con: duckdb.DuckDBPyConnection) -> None:
    up1 = _UpstreamServer(["b"])
    up2 = _UpstreamServer(["b"])
    side = con.cursor()
    try:
        with _serving(con, "") as (peer, fd):
            assert _add_ranked(side, fd, up1, 1) == []
            assert peer.recv_push() == ["b"]
            assert _add_ranked(side, fd, up2, 2) == ["b"]
            up1.kill()
            assert peer.recv_until(lambda f: f["t"] == FT_PUSH and f["catalogs"] == ["b"])
            _wait_closed(side, fd, up1.link.fileno())
            assert _open(peer, 1, "b")["t"] == FT_OPEN_ACK
    finally:
        up1.close()
        up2.close()


def test_the_first_upstream_to_serve_a_name_keeps_it(con: duckdb.DuckDBPyConnection) -> None:
    up1 = _UpstreamServer(["a", "b"])
    up2 = _UpstreamServer(["b", "c"])
    side = con.cursor()
    try:
        with _serving(con, "") as (peer, fd):
            assert _add_upstream(side, fd, up1.link.fileno(), _hello(side, up1.link.fileno())[0]) == []
            assert _add_upstream(side, fd, up2.link.fileno(), _hello(side, up2.link.fileno())[0]) == ["b"]
            peer.recv_until(lambda f: f["t"] == FT_PUSH and f["catalogs"] == ["a", "b", "c"])
    finally:
        up1.close()
        up2.close()
