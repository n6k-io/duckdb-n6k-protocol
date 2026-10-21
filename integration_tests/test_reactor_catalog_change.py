"""`n6k_serve_add_catalog` / `n6k_serve_remove_catalog` change a live serve's catalogs, and every
change of the listed set reaches the client as a connection-scoped PUSH CATALOGS_CHANGED.

Drives the reactor directly over a socketpair, no websocket and no test server.

Run:
    uv run pytest integration_tests/test_reactor_catalog_change.py -v
"""

import os
import socket
import struct
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import pytest

from _paths import REPO_ROOT
from _targets import substrait_plan, substrait_read

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    CAP_CATALOG_PLANS,
    FT_CANCEL,
    FT_ERR,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_OPEN_PENDING,
    FT_PING,
    FT_PUSH,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    OP_CATALOG_LIST,
    OP_CATALOGS_CHANGED,
    STREAM_FRAME_N6K,
    OP_QUERY,
    OP_READ_PLAN,
    OP_TXN_BEGIN,
)

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

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

    def recv_until(self, match: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
        while not match(frame := self.recv()):
            pass
        return frame

    def recv_push(self) -> list[str]:
        frame = self.recv_until(lambda f: f["t"] == FT_PUSH and f.get("op") == OP_CATALOGS_CHANGED)
        assert "ns" not in frame
        return list(frame["catalogs"])

    def recv_skipping_pings(self) -> dict[str, Any]:
        return self.recv_until(lambda f: f["t"] != FT_PING)

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
    c.execute("ATTACH ':memory:' AS a")
    c.execute("ATTACH ':memory:' AS c")
    yield c
    c.close()


def _add(side: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    side.execute("SELECT * FROM n6k_serve_add_catalog(?, ?)", [fd, name]).fetchall()


def _remove(side: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    side.execute("SELECT * FROM n6k_serve_remove_catalog(?, ?)", [fd, name]).fetchall()


def _open(peer: _Peer, ns: int, catalog: str) -> dict[str, Any]:
    peer.send({"t": FT_OPEN, "ns": ns, "catalog": catalog})
    return peer.recv_until(lambda f: f["t"] in (FT_OPEN_ACK, FT_OPEN_ERR) and f["ns"] == ns)


def test_added_catalog_is_pushed_listed_and_opens(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a'") as (peer, fd):
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ERR
        _add(side, fd, "c")
        assert peer.recv_push() == ["a", "c"]
        peer.send({"t": FT_REQ, "id": 1, "op": OP_CATALOG_LIST})
        assert list(peer.recv_until(lambda f: f["t"] == FT_RESP_CHUNK)["catalogs"]) == ["a", "c"]
        assert _open(peer, 2, "c")["t"] == FT_OPEN_ACK


def test_removing_a_served_catalog_errs_in_flight_and_closes_its_session(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ACK
        # One row per batch outruns the default credit window, so the worker parks mid-stream.
        peer.send(
            {"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT * FROM range(100000)", "_batch_rows": 1}
        )
        peer.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f["ns"] == 1)

        _remove(side, fd, "c")
        err = peer.recv_until(lambda f: f["t"] == FT_ERR and f.get("ns") == 1 and f["id"] == 1)
        assert "was removed" in err["exception_message"]
        assert peer.recv_push() == ["a"]

        peer.send({"t": FT_REQ, "id": 2, "ns": 1, "op": OP_QUERY, "sql": "SELECT 1"})
        err = peer.recv_until(lambda f: f["t"] == FT_ERR and f["id"] == 2)
        assert "unknown session" in err["exception_message"]
        frame = _open(peer, 2, "c")
        assert frame["t"] == FT_OPEN_ERR
        assert "not served" in frame["exception_message"]


def test_removing_a_catalog_mid_transaction_stream_leaves_the_reactor_serving(
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute("CREATE TABLE c.main.t AS SELECT range AS i FROM range(100000)")
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ACK
        peer.send({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_TXN_BEGIN})
        txn = peer.recv_until(lambda f: f["t"] == FT_RESP_END and f.get("ns") == 1)["txn"]
        plan = substrait_plan(substrait_read("main", "t", [("i", "i64")]), ["i"])
        peer.send({"t": FT_REQ, "id": 2, "ns": 1, "op": OP_READ_PLAN, "plan": plan, "txn": txn, "_batch_rows": 1})
        peer.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f["ns"] == 1)

        _remove(side, fd, "c")
        assert peer.recv_push() == ["a"]
        assert _open(peer, 2, "a")["t"] == FT_OPEN_ACK
        peer.send({"t": FT_REQ, "id": 1, "ns": 2, "op": OP_QUERY, "sql": "SELECT 42 AS n"})
        end = peer.recv_until(lambda f: f["t"] in (FT_RESP_END, FT_ERR) and f.get("ns") == 2)
        assert end["t"] == FT_RESP_END


def test_removing_one_catalog_leaves_a_sibling_session_serving(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        assert _open(peer, 2, "c")["t"] == FT_OPEN_ACK
        _remove(side, fd, "c")
        assert peer.recv_push() == ["a"]
        peer.send({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 42 AS n"})
        end = peer.recv_until(lambda f: f["t"] in (FT_RESP_END, FT_ERR) and f.get("ns") == 1)
        assert end["t"] == FT_RESP_END


def test_a_removed_catalog_can_be_added_again(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        _remove(side, fd, "c")
        assert peer.recv_push() == ["a"]
        _add(side, fd, "c")
        assert peer.recv_push() == ["a", "c"]
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ACK


PENDING_LATE = ", 'a', pending => MAP {'late': 'warming'}, open_pending_interval_ms => 100"


def test_removing_a_pending_catalog_errs_its_parked_opens(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, PENDING_LATE) as (peer, fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "late"})
        assert peer.recv_skipping_pings()["t"] == FT_OPEN_PENDING
        _remove(side, fd, "late")
        frame = peer.recv_until(lambda f: f["t"] == FT_OPEN_ERR)
        assert "was removed" in frame["exception_message"]
        assert peer.recv_push() == ["a"]
        side.execute("ATTACH ':memory:' AS late")
        with pytest.raises(duckdb.InvalidInputException, match="not pending"):
            side.execute("SELECT * FROM n6k_serve_open_ready(?, 'late')", [fd]).fetchall()


def test_a_failed_pending_catalog_is_pushed_out_of_the_list(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, PENDING_LATE) as (peer, fd):
        side.execute("SELECT * FROM n6k_serve_open_failed(?, 'late', 'boom')", [fd]).fetchall()
        assert peer.recv_push() == ["a"]


def test_invalid_changes_raise(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a'") as (_peer, fd):
        with pytest.raises(duckdb.InvalidInputException, match="already served"):
            _add(side, fd, "a")
        with pytest.raises(duckdb.InvalidInputException, match="not served or pending"):
            _remove(side, fd, "c")
        with pytest.raises(duckdb.BinderException, match="not attached"):
            _add(side, fd, "nope")


def _pause(side: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    side.execute("SELECT * FROM n6k_serve_pause(?, ?)", [fd, name]).fetchall()


def _resume(side: duckdb.DuckDBPyConnection, fd: int, name: str) -> None:
    side.execute("SELECT * FROM n6k_serve_resume(?, ?)", [fd, name]).fetchall()


def _await_inflight(side: duckdb.DuckDBPyConnection, fd: int, name: str, want: int) -> None:
    deadline = time.monotonic() + 5
    while True:
        row = side.execute("SELECT holding, running FROM n6k_serve_inflight(?, ?)", [fd, name]).fetchone()
        if row is not None and row[0] and row[1] == want:
            return
        assert time.monotonic() < deadline, row
        time.sleep(0.02)


def _assert_silent(peer: _Peer, seconds: float) -> None:
    peer.sock.settimeout(seconds)
    try:
        while (frame := peer.recv())["t"] == FT_PING:
            pass
        raise AssertionError(f"expected no frame while paused, got {frame}")
    except socket.timeout:
        pass
    finally:
        peer.sock.settimeout(5)


def _end_of(peer: _Peer, ns: int, req_id: int) -> dict[str, Any]:
    return peer.recv_until(lambda f: f["t"] in (FT_RESP_END, FT_ERR) and f.get("ns") == ns and f.get("id") == req_id)


def test_a_paused_catalog_holds_requests_until_resume_and_they_see_the_swapped_catalog(
    con: duckdb.DuckDBPyConnection,
) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ACK
        _pause(side, fd, "c")
        _await_inflight(side, fd, "c", 0)
        peer.send({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT * FROM c.main.fresh"})
        _assert_silent(peer, 0.5)

        side.execute("DETACH c")
        side.execute("ATTACH ':memory:' AS c")
        side.execute("CREATE TABLE c.main.fresh AS SELECT 1 AS v")
        _resume(side, fd, "c")
        assert _end_of(peer, 1, 1)["t"] == FT_RESP_END
        assert _open(peer, 2, "c")["t"] == FT_OPEN_ACK


def test_pausing_one_catalog_leaves_a_sibling_serving(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _pause(side, fd, "c")
        _await_inflight(side, fd, "c", 0)
        peer.send({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 42 AS n"})
        assert _end_of(peer, 1, 1)["t"] == FT_RESP_END
        _resume(side, fd, "c")


def test_cancel_inflight_stops_a_stream_running_on_a_paused_catalog(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ACK
        peer.send(
            {"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT * FROM range(100000)", "_batch_rows": 1}
        )
        peer.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f["ns"] == 1)
        _pause(side, fd, "c")
        _await_inflight(side, fd, "c", 1)
        assert side.execute("SELECT * FROM n6k_serve_cancel_inflight(?, 'c')", [fd]).fetchone() == (1,)
        end = _end_of(peer, 1, 1)
        assert end["t"] == FT_ERR or end.get("cancelled") is True
        _await_inflight(side, fd, "c", 0)
        _resume(side, fd, "c")


def test_a_held_request_can_be_cancelled(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ACK
        _pause(side, fd, "c")
        _await_inflight(side, fd, "c", 0)
        peer.send({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 1"})
        peer.send({"t": FT_CANCEL, "id": 1, "ns": 1})
        assert _end_of(peer, 1, 1)["cancelled"] is True
        _resume(side, fd, "c")
        _assert_silent(peer, 0.5)


def test_removing_a_paused_catalog_errs_its_held_requests(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "c")["t"] == FT_OPEN_ACK
        _pause(side, fd, "c")
        _await_inflight(side, fd, "c", 0)
        peer.send({"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 1"})
        _assert_silent(peer, 0.3)
        _remove(side, fd, "c")
        err = _end_of(peer, 1, 1)
        assert err["t"] == FT_ERR
        assert "was removed" in err["exception_message"]
        _resume(side, fd, "c")


def test_invalid_pauses_raise(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a'") as (_peer, fd):
        with pytest.raises(duckdb.InvalidInputException, match="not served or pending"):
            _pause(side, fd, "nope")
        with pytest.raises(duckdb.InvalidInputException, match="not paused"):
            _resume(side, fd, "a")
        with pytest.raises(duckdb.InvalidInputException, match="not paused"):
            side.execute("SELECT * FROM n6k_serve_inflight(?, 'a')", [fd]).fetchall()
        _pause(side, fd, "a")
        with pytest.raises(duckdb.InvalidInputException, match="already paused"):
            _pause(side, fd, "a")
        _resume(side, fd, "a")


def _read_plan(peer: _Peer, ns: int, req_id: int, names: list[str], **extra: Any) -> None:
    rel = substrait_read(names[-2], names[-1], [("i", "i64")])
    rel["read"]["namedTable"]["names"] = names
    plan = substrait_plan(rel, ["i"])
    peer.send({"t": FT_REQ, "id": req_id, "ns": ns, "op": OP_READ_PLAN, "plan": plan, **extra})


def test_a_plan_reads_a_catalog_another_session_has_open(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE c.main.only_in_c AS SELECT 1::BIGINT AS i")
    with _serving(con, ", 'a', 'c'") as (peer, _fd):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        assert _open(peer, 2, "c")["t"] == FT_OPEN_ACK
        _read_plan(peer, 1, 1, ["c", "main", "only_in_c"])
        assert _end_of(peer, 1, 1)["t"] == FT_RESP_END


def test_a_plan_naming_a_catalog_no_session_has_open_is_refused(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE c.main.only_in_c AS SELECT 1::BIGINT AS i")
    with _serving(con, ", 'a', 'c'") as (peer, _fd):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _read_plan(peer, 1, 1, ["c", "main", "only_in_c"])
        err = _end_of(peer, 1, 1)
        assert err["t"] == FT_ERR
        assert err["exception_type"] == "CatalogException"
        assert 'catalog "c"' in err["exception_message"]


def test_a_plan_naming_a_paused_catalog_is_refused_as_retriable(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE c.main.only_in_c AS SELECT 1::BIGINT AS i")
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        assert _open(peer, 2, "c")["t"] == FT_OPEN_ACK
        _pause(side, fd, "c")
        _await_inflight(side, fd, "c", 0)
        _read_plan(peer, 1, 1, ["c", "main", "only_in_c"])
        err = _end_of(peer, 1, 1)
        assert err["t"] == FT_ERR
        assert err["exception_type"] == "TransactionException"
        assert err["retriable"] is True
        _resume(side, fd, "c")


def test_a_plan_on_another_session_counts_as_inflight_on_the_catalog_it_reads(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE c.main.big AS SELECT range AS i FROM range(100000)")
    side = con.cursor()
    with _serving(con, ", 'a', 'c'") as (peer, fd):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        assert _open(peer, 2, "c")["t"] == FT_OPEN_ACK
        _read_plan(peer, 1, 1, ["c", "main", "big"], _batch_rows=1)
        peer.recv_until(lambda f: f["t"] == FT_RESP_CHUNK and f["ns"] == 1)
        _pause(side, fd, "c")
        _await_inflight(side, fd, "c", 1)
        assert side.execute("SELECT * FROM n6k_serve_cancel_inflight(?, 'c')", [fd]).fetchone() == (1,)
        end = _end_of(peer, 1, 1)
        assert end["t"] == FT_ERR or end.get("cancelled") is True
        _await_inflight(side, fd, "c", 0)
        _resume(side, fd, "c")


def test_open_grants_catalog_plans(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _fd):
        peer.send({"t": FT_OPEN, "ns": 1, "catalog": "a", "capabilities": [CAP_CATALOG_PLANS]})
        ack = peer.recv_until(lambda f: f["t"] == FT_OPEN_ACK and f["ns"] == 1)
        assert ack["capabilities"] == [CAP_CATALOG_PLANS]
