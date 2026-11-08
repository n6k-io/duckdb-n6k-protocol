"""The reactor runs requests on at most `workers` threads, and a stream waiting for CREDIT holds none.

Drives the reactor directly over a socketpair, no websocket and no test server.

Run:
    uv run pytest integration_tests/test_reactor_workers.py -v
"""

import os
import time
import uuid
from collections.abc import Iterator
from typing import Any

import duckdb
import pytest

from test_reactor_catalog_change import SERVER_EXT_PATH, _collect_until, _of, _open, _Peer, _serving

from n6k_protocol.protocol import (
    DEFAULT_BATCH_CREDITS,
    FT_CANCEL,
    FT_CREDIT,
    FT_ERR,
    FT_OPEN_ACK,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    MAX_CONCURRENT_REQS,
    OP_QUERY,
)

# One row per frame, far more rows than any credit window: the stream starves until credited.
_STARVED = {"op": OP_QUERY, "sql": "SELECT range AS i FROM range(100000)", "_batch_rows": 1}


pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    c.execute("ATTACH ':memory:' AS a")
    yield c
    c.close()


def _end(peer: _Peer, req_id: int) -> dict[str, Any]:
    return peer.recv_until(lambda f: f["t"] in (FT_RESP_END, FT_ERR) and f.get("id") == req_id)


def _start_starved(peer: _Peer, ids: range) -> None:
    """Sends a starved stream per id and waits until each has used up its window."""
    for req_id in ids:
        peer.send({"t": FT_REQ, "id": req_id, "ns": 1, **_STARVED})
    chunks = dict.fromkeys(ids, 0)
    while any(n < DEFAULT_BATCH_CREDITS for n in chunks.values()):
        frame = peer.recv()
        if frame["t"] == FT_RESP_CHUNK and frame.get("id") in chunks:
            chunks[frame["id"]] += 1
        assert frame["t"] != FT_ERR, frame


def _peak_workers(side: duckdb.DuckDBPyConnection, session_id: uuid.UUID) -> int:
    row = side.execute("SELECT peak_workers FROM n6k_serve_session_stats(?)", [session_id]).fetchone()
    assert row is not None
    return int(row[0])


def test_starved_streams_leave_the_only_worker_free(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', workers => 1") as (peer, session_id):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _start_starved(peer, range(1, 3))
        peer.send({"t": FT_REQ, "id": 3, "ns": 1, "op": OP_QUERY, "sql": "SELECT 42 AS n"})
        assert _end(peer, 3)["t"] == FT_RESP_END
        assert _peak_workers(side, session_id) == 1


def test_a_credited_stream_resumes_after_parking(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a', workers => 1") as (peer, _):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        peer.send(
            {"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT range AS i FROM range(20)", "_batch_rows": 1}
        )
        chunks = 0
        while chunks < DEFAULT_BATCH_CREDITS:
            chunks += peer.recv()["t"] == FT_RESP_CHUNK
        peer.send({"t": FT_CREDIT, "id": 1, "ns": 1, "n": 100})
        end = _end(peer, 1)
        assert end["t"] == FT_RESP_END and not end.get("cancelled")


def test_a_parked_stream_can_be_cancelled(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a', workers => 1") as (peer, _):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _start_starved(peer, range(1, 2))
        peer.send({"t": FT_CANCEL, "id": 1, "ns": 1})
        assert _end(peer, 1)["cancelled"] is True


def test_workers_cap_the_threads_however_many_streams_are_open(con: duckdb.DuckDBPyConnection) -> None:
    side = con.cursor()
    with _serving(con, ", 'a', workers => 2") as (peer, session_id):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _start_starved(peer, range(1, 11))
        assert _peak_workers(side, session_id) <= 2


def test_the_window_still_refuses_past_its_limit_when_every_stream_is_parked(
    con: duckdb.DuckDBPyConnection,
) -> None:
    with _serving(con, ", 'a', workers => 1") as (peer, _):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _start_starved(peer, range(1, MAX_CONCURRENT_REQS + 1))
        peer.send({"t": FT_REQ, "id": MAX_CONCURRENT_REQS + 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT 1"})
        err = _end(peer, MAX_CONCURRENT_REQS + 1)
        assert err["t"] == FT_ERR and "too many concurrent requests" in err["exception_message"]


@pytest.mark.parametrize("workers", [1, 4])
def test_queued_requests_all_answer(con: duckdb.DuckDBPyConnection, workers: int) -> None:
    with _serving(con, f", 'a', workers => {workers}") as (peer, _):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        ids = range(1, 33)
        for req_id in ids:
            peer.send({"t": FT_REQ, "id": req_id, "ns": 1, "op": OP_QUERY, "sql": f"SELECT {req_id} AS n"})
        ended = set()
        while len(ended) < len(ids):
            frame = peer.recv()
            assert frame["t"] != FT_ERR, frame
            if frame["t"] == FT_RESP_END:
                ended.add(frame["id"])
        assert ended == set(ids)


def test_an_idle_parked_stream_is_ended_when_the_timeout_is_set(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a', idle_stream_timeout_ms => 300") as (peer, _):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _start_starved(peer, range(1, 2))
        err = _end(peer, 1)
        assert err["t"] == FT_ERR and "idle" in err["exception_message"]


def test_an_idle_stream_gets_its_err_and_nothing_after(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a', workers => 1, idle_stream_timeout_ms => 300") as (peer, _):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        _start_starved(peer, range(1, 2))
        assert _end(peer, 1)["t"] == FT_ERR
        # Queued behind the cancel the timeout handed the one worker.
        peer.send({"t": FT_REQ, "id": 2, "ns": 1, "op": OP_QUERY, "sql": "SELECT 1"})
        before = _collect_until(peer, lambda f: f["t"] == FT_RESP_END and f.get("id") == 2)
        assert _of(before, 1, 1) == []


def test_a_parked_stream_waits_indefinitely_by_default(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, 1, "a")["t"] == FT_OPEN_ACK
        peer.send(
            {"t": FT_REQ, "id": 1, "ns": 1, "op": OP_QUERY, "sql": "SELECT range AS i FROM range(20)", "_batch_rows": 1}
        )
        chunks = 0
        while chunks < DEFAULT_BATCH_CREDITS:
            chunks += peer.recv()["t"] == FT_RESP_CHUNK
        time.sleep(1)
        peer.send({"t": FT_CREDIT, "id": 1, "ns": 1, "n": 100})
        end = _end(peer, 1)
        assert end["t"] == FT_RESP_END and not end.get("cancelled")
