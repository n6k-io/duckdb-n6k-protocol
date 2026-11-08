"""Links on the wire: one far transaction and its streams, driven directly over a socketpair.

Run:
    uv run pytest integration_tests/test_link_wire.py -v
"""

import os
import time
from collections.abc import Iterator
from typing import Any

import duckdb
import pytest

from _targets import (
    CONTROL_STREAM,
    parse_stream_payload,
    plan_header,
    slice_stream,
    stream_payload,
    substrait_plan,
    substrait_read,
)
from test_reactor_catalog_change import SERVER_EXT_PATH, _open, _Peer, _serving

from n6k_protocol.protocol import (
    DEFAULT_BATCH_CREDITS,
    FT_LINK_CANCEL,
    FT_LINK_CLOSE,
    FT_LINK_END,
    FT_LINK_OPEN,
    FT_OPEN_ACK,
    FT_PING,
    FT_PONG,
    FT_STREAM_CREDIT,
    FT_STREAM_DATA,
    FT_STREAM_END,
)

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT_PATH), reason=f"Extension not built: {SERVER_EXT_PATH}")

NS = 1
SLICE = slice_stream()


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension(SERVER_EXT_PATH)
    c.execute("ATTACH ':memory:' AS a")
    c.execute("CREATE TABLE a.main.t AS SELECT range AS i FROM range(100000)")
    yield c
    c.close()


def _plan() -> str:
    return substrait_plan(substrait_read("main", "t", [("i", "i64")]), ["i"])


def _open_link(peer: _Peer, link: int = 1) -> None:
    peer.send({"t": FT_LINK_OPEN, "ns": NS, "link": link})


def _control(peer: _Peer, header: str, link: int = 1) -> None:
    peer.send(
        {"t": FT_STREAM_DATA, "ns": NS, "link": link, "stream": CONTROL_STREAM, "fin": True}, stream_payload(header)
    )


def _control_reply(peer: _Peer, link: int = 1) -> str:
    while True:
        frame, body = peer.recv_frame()
        if frame["t"] == FT_STREAM_DATA and frame["stream"] == CONTROL_STREAM and frame["link"] == link:
            return parse_stream_payload(body)[0]


def _link_end(peer: _Peer, link: int = 1) -> dict[str, Any]:
    return peer.recv_until(lambda f: f["t"] == FT_LINK_END and f["link"] == link)


def _data_frames(peer: _Peer, count: int, link: int = 1) -> None:
    seen = 0
    while seen < count:
        frame, _ = peer.recv_frame()
        if frame["t"] == FT_STREAM_DATA and frame["link"] == link and frame["stream"] == SLICE and frame["fin"]:
            seen += 1


def _pings_back(peer: _Peer, ping: int = 99) -> list[dict[str, Any]]:
    peer.send({"t": FT_PING, "id": ping})
    frames = []
    while True:
        frame = peer.recv()
        if frame["t"] == FT_PONG and frame.get("id") == ping:
            return frames
        frames.append(frame)


def test_a_statement_stops_at_its_credit_and_resumes_when_granted(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        _control(peer, plan_header(_plan()))
        _data_frames(peer, DEFAULT_BATCH_CREDITS)
        time.sleep(0.3)
        assert not [f for f in _pings_back(peer) if f["t"] == FT_STREAM_DATA]
        peer.send({"t": FT_STREAM_CREDIT, "ns": NS, "link": 1, "stream": SLICE, "n": 1})
        _data_frames(peer, 1)


def test_a_peer_past_its_credit_loses_the_connection(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        for _ in range(DEFAULT_BATCH_CREDITS + 1):
            peer.send({"t": FT_STREAM_DATA, "ns": NS, "link": 1, "stream": SLICE, "fin": True}, stream_payload("0"))
        with pytest.raises(AssertionError, match="closed the socket"):
            peer.recv_until(lambda f: False)


def test_a_frame_arrives_in_pieces_and_a_large_one_leaves_in_pieces(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE a.main.wide AS SELECT repeat('x', 3000000) AS s")
    plan = substrait_plan(substrait_read("main", "wide", [("s", "string")]), ["s"])
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        # Padded past any one frame the transport would carry: JSON ignores the whitespace.
        payload = stream_payload(plan_header(plan + " " * (2 * 1024 * 1024)))
        middle = len(payload) // 2
        peer.send({"t": FT_STREAM_DATA, "ns": NS, "link": 1, "stream": 0, "fin": False}, payload[:middle])
        peer.send({"t": FT_STREAM_DATA, "ns": NS, "link": 1, "stream": 0, "fin": True}, payload[middle:])
        pieces = []
        while True:
            frame, body = peer.recv_frame()
            if frame["t"] == FT_STREAM_DATA and frame["stream"] == SLICE:
                assert len(body) <= 512 * 1024
                pieces.append(frame["fin"])
            if frame["t"] == FT_LINK_END:
                break
        assert "exception_type" not in frame, frame
        assert len(pieces) > 1 and pieces[-1] is True and pieces[0] is False


def test_max_links_refuses_the_next_and_a_close_frees_its_place(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a', max_links => 1") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer, 1)
        _open_link(peer, 2)
        assert "too many" in _link_end(peer, 2)["exception_message"]
        _control(peer, "ROLLBACK", 1)
        assert _control_reply(peer, 1) == "DONE"
        _link_end(peer, 1)
        peer.send({"t": FT_LINK_CLOSE, "ns": NS, "link": 1})
        for attempt in range(3, 40):
            _open_link(peer, attempt)
            time.sleep(0.05)
            frames = _pings_back(peer, attempt)
            if not [f for f in frames if f["t"] == FT_LINK_END and f["link"] == attempt]:
                return
        pytest.fail("the closed link's place never came free")


def test_cancel_before_the_statement_ends_the_link_with_an_error(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        peer.send({"t": FT_LINK_CANCEL, "ns": NS, "link": 1})
        assert "cancelled" in _link_end(peer)["exception_message"]


def test_cancel_stops_a_statement_waiting_for_credit(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        _control(peer, plan_header(_plan()))
        _data_frames(peer, DEFAULT_BATCH_CREDITS)
        peer.send({"t": FT_LINK_CANCEL, "ns": NS, "link": 1})
        assert "cancelled" in _link_end(peer)["exception_message"]


def test_closing_a_starved_stream_lets_its_commit_through(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        _control(peer, plan_header(_plan(), ends_transaction=False))
        _data_frames(peer, DEFAULT_BATCH_CREDITS)
        peer.send({"t": FT_STREAM_END, "ns": NS, "link": 1, "stream": SLICE})
        assert _control_reply(peer) == "DONE"
        _control(peer, "COMMIT")
        assert _control_reply(peer) == "DONE"
        assert "exception_type" not in _link_end(peer)


def test_frames_after_a_link_ended_are_dropped(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        _control(peer, "ROLLBACK")
        assert _control_reply(peer) == "DONE"
        _link_end(peer)
        peer.send({"t": FT_STREAM_CREDIT, "ns": NS, "link": 1, "stream": SLICE, "n": 1})
        peer.send({"t": FT_STREAM_DATA, "ns": NS, "link": 1, "stream": 0, "fin": True}, stream_payload("COMMIT"))
        peer.send({"t": FT_STREAM_CREDIT, "ns": NS, "link": 77, "stream": SLICE, "n": 1})
        _pings_back(peer)


def test_an_idle_transaction_ends_when_the_timeout_is_set(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a', idle_txn_timeout_ms => 300") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        assert "idle" in _link_end(peer)["exception_message"]


def test_a_transaction_in_use_outlives_the_timeout(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE TABLE a.main.small AS SELECT 1::BIGINT AS i")
    plan = substrait_plan(substrait_read("main", "small", [("i", "i64")]), ["i"], slice_id=0)
    with _serving(con, ", 'a', idle_txn_timeout_ms => 500") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        for statement in range(6):
            time.sleep(0.2)
            statement_plan = plan.replace('"id": 0', f'"id": {statement}')
            _control(peer, plan_header(statement_plan, ends_transaction=False, slice_id=statement))
            assert _control_reply(peer) == "DONE"
        _control(peer, "COMMIT")
        assert _control_reply(peer) == "DONE"


def test_a_stream_parked_for_credit_ends_when_the_timeout_is_set(con: duckdb.DuckDBPyConnection) -> None:
    with _serving(con, ", 'a', idle_stream_timeout_ms => 300") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer)
        _control(peer, plan_header(_plan()))
        assert "idle" in _link_end(peer)["exception_message"]


def test_closing_the_socket_ends_open_links(con: duckdb.DuckDBPyConnection) -> None:
    # `_serving` fails if the serve has not ended ten seconds after the peer closes.
    with _serving(con, ", 'a'") as (peer, _):
        assert _open(peer, NS, "a")["t"] == FT_OPEN_ACK
        _open_link(peer, 1)
        _open_link(peer, 2)
        _control(peer, plan_header(_plan()), 2)
        _data_frames(peer, DEFAULT_BATCH_CREDITS, 2)
