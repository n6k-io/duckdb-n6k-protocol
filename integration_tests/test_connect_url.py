"""Connections the extension dials itself: `n6k_connect(url => …)` and `ATTACH 'n6k://…'`, which share one
connection per URL and token, held open by a lease per n6k_connect and per ATTACH.

Runs against the always-on test server on :8099.

Run:
    uv run pytest integration_tests/test_connect_url.py -v
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import List, Tuple

import duckdb
import pytest
from duckdb.sqltypes import UUID, VARCHAR

from _paths import EXT_PATH

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
N6K_URL = f"n6k://localhost:{SERVER_PORT}"
TIMEOUT_S = 5


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip("Test server not running on port 8099")


def _connect():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


def _ws_close_all():
    urllib.request.urlopen(
        urllib.request.Request(f"{SERVER_URL}/debug/ws/close_all", method="POST"),
        timeout=2,
    ).read()


class _Inbox:
    """What the extension handed the host UDFs, from its own thread."""

    def __init__(self) -> None:
        self.cv = threading.Condition()
        self.texts: List[Tuple[uuid.UUID, str]] = []
        self.closes: List[Tuple[uuid.UUID, str]] = []

    def on_text(self, conn_id: uuid.UUID, text: str) -> None:
        with self.cv:
            self.texts.append((conn_id, text))
            self.cv.notify_all()

    def on_close(self, conn_id: uuid.UUID, reason: str) -> None:
        with self.cv:
            self.closes.append((conn_id, reason))
            self.cv.notify_all()

    def wait(self, pred) -> None:
        with self.cv:
            assert self.cv.wait_for(pred, TIMEOUT_S), (self.texts, self.closes)


def _register_inbox(c) -> _Inbox:
    inbox = _Inbox()
    for name, fn in (("host_on_text", inbox.on_text), ("host_on_close", inbox.on_close)):
        c.create_function(name, fn, [UUID, VARCHAR], VARCHAR, null_handling="special", side_effects=True)
    return inbox


def _connect_url(c, url, extra=""):
    return c.execute(f"SELECT * FROM n6k_connect(url => '{url}'{extra})").fetchone()


def _attach(c, conn_id, alias, catalog=None):
    extra = f", CATALOG '{catalog}'" if catalog else ""
    c.execute(f"ATTACH '' AS {alias} (TYPE n6k, CONNECTION '{conn_id}'{extra})")


def test_two_attaches_share_one_dialed_connection():
    c = _connect()
    try:
        conn_id, caps = _connect_url(c, f"{N6K_URL}/tuned?catalogs=a,b")
        assert "crossing" in caps
        _attach(c, conn_id, "x1", "a")
        _attach(c, conn_id, "y1", "b")
        assert c.execute("SELECT catalog FROM x1.main.marker").fetchone()[0] == "a"
        assert c.execute("SELECT catalog FROM y1.main.marker").fetchone()[0] == "b"
        assert c.execute("SELECT count(*), sum(catalogs) FROM n6k_connections()").fetchone() == (1, 2)
        listed = c.execute("SELECT * FROM n6k_list_catalogs(connection => ?)", [conn_id]).fetchall()
        assert sorted(r[0] for r in listed) == ["a", "b"]
    finally:
        c.close()


def test_a_join_across_a_shared_connection_is_one_read():
    c = _connect()
    try:
        conn_id, _caps = _connect_url(c, f"{N6K_URL}/tuned?catalogs=a,b")
        _attach(c, conn_id, "x1", "a")
        _attach(c, conn_id, "y1", "b")
        sql = "SELECT x.catalog, y.catalog FROM x1.main.marker x JOIN y1.main.marker y ON x.catalog <> y.catalog"
        plan = "\n".join(r[1] for r in c.execute(f"EXPLAIN {sql}").fetchall())
        assert plan.count("Crossing Read") == 1
        assert c.execute(sql).fetchall() == [("a", "b")]
    finally:
        c.close()


def test_on_text_receives_server_text_and_send_text_reaches_the_server():
    c = _connect()
    inbox = _register_inbox(c)
    try:
        conn_id, caps = _connect_url(c, f"{N6K_URL}/rpc?catalog=db", ", on_text => 'host_on_text'")
        assert "jsonrpc" in caps
        inbox.wait(lambda: any('"welcome"' in text for _, text in inbox.texts))
        assert all(cid == conn_id for cid, _ in inbox.texts)

        request = {"jsonrpc": "2.0", "id": 1, "method": "echo", "params": {"x": 5}}
        c.execute("SELECT * FROM n6k_send_text(?, ?)", [conn_id, json.dumps(request)]).fetchall()
        inbox.wait(lambda: any(json.loads(text).get("id") == 1 for _, text in inbox.texts))
        reply = next(json.loads(text) for _, text in inbox.texts if json.loads(text).get("id") == 1)
        assert reply["result"] == 5
    finally:
        c.close()


def test_text_received_before_a_disconnect_still_reaches_on_text():
    c = _connect()
    inbox = _Inbox()
    release = threading.Event()

    def on_text(conn_id: uuid.UUID, text: str) -> None:
        inbox.on_text(conn_id, text)
        release.wait(TIMEOUT_S)

    c.create_function("held_on_text", on_text, [UUID, VARCHAR], VARCHAR, null_handling="special", side_effects=True)
    try:
        conn_id, _caps = _connect_url(c, f"{N6K_URL}/rpc?catalog=db", ", on_text => 'held_on_text'")
        # The worker is now held inside the first call, so the reply below queues behind it.
        inbox.wait(lambda: len(inbox.texts) == 1)
        request = {"jsonrpc": "2.0", "id": 1, "method": "echo", "params": {"x": 5}}
        c.execute("SELECT * FROM n6k_send_text(?, ?)", [conn_id, json.dumps(request)]).fetchall()
        time.sleep(0.5)
        c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        release.set()
        inbox.wait(lambda: any(json.loads(text).get("id") == 1 for _, text in inbox.texts))
    finally:
        release.set()
        c.close()


def test_without_on_text_the_offer_is_crossing_only():
    c = _connect()
    try:
        _conn_id, caps = _connect_url(c, f"{N6K_URL}/rpc?catalog=db")
        assert "jsonrpc" not in caps
    finally:
        c.close()


def test_a_misnamed_on_text_fails_the_connect():
    c = _connect()
    try:
        with pytest.raises(duckdb.Error, match="host callback"):
            _connect_url(c, f"{N6K_URL}/rpc?catalog=db", ", on_text => 'no_such_udf'")
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()


def test_on_close_fires_on_a_server_drop_and_the_next_query_redials():
    c = _connect()
    inbox = _register_inbox(c)
    try:
        conn_id, _caps = _connect_url(c, f"{N6K_URL}?catalog=db", ", on_close => 'host_on_close'")
        _attach(c, conn_id, "db")
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3

        _ws_close_all()
        inbox.wait(lambda: len(inbox.closes) == 1)
        assert inbox.closes[0][0] == conn_id
        assert c.execute("SELECT alive FROM n6k_connections()").fetchone()[0] is False

        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        assert c.execute("SELECT alive FROM n6k_connections()").fetchone()[0] is True
    finally:
        c.close()


def test_reconnect_brings_back_a_dropped_connection_with_no_catalogs():
    c = _connect()
    inbox = _register_inbox(c)
    try:
        conn_id, caps = _connect_url(c, f"{N6K_URL}?catalog=db", ", on_close => 'host_on_close'")
        _ws_close_all()
        inbox.wait(lambda: len(inbox.closes) == 1)
        assert c.execute("SELECT alive FROM n6k_connections()").fetchone()[0] is False

        assert c.execute("SELECT * FROM n6k_reconnect(?)", [str(conn_id)]).fetchone()[0] == caps
        assert c.execute("SELECT connection_id, alive FROM n6k_connections()").fetchall() == [(conn_id, True)]
    finally:
        c.close()


def test_reconnect_of_a_live_connection_redials_and_its_catalogs_reopen():
    c = _connect()
    inbox = _register_inbox(c)
    try:
        conn_id, _caps = _connect_url(c, f"{N6K_URL}?catalog=db", ", on_close => 'host_on_close'")
        _attach(c, conn_id, "db")
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3

        c.execute("SELECT * FROM n6k_reconnect(?)", [conn_id]).fetchall()
        assert c.execute("SELECT alive, catalogs FROM n6k_connections()").fetchone() == (True, 1)
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        # Asked for by the host, so not reported back to it.
        assert inbox.closes == []
    finally:
        c.close()


def test_send_text_on_a_disconnected_id_raises():
    c = _connect()
    try:
        conn_id, _caps = _connect_url(c, f"{N6K_URL}?catalog=db")
        c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        with pytest.raises(duckdb.Error, match="no open connection"):
            c.execute("SELECT * FROM n6k_send_text(?, '{}')", [conn_id]).fetchall()
    finally:
        c.close()


def test_url_attaches_share_one_leased_connection():
    c = _connect()
    try:
        c.execute(f"ATTACH '{N6K_URL}' AS a (TYPE n6k, CATALOG 'db')")
        c.execute(f"ATTACH '{N6K_URL}' AS b (TYPE n6k, CATALOG 'db')")
        assert c.execute("SELECT count(*), sum(catalogs), sum(leases) FROM n6k_connections()").fetchone() == (1, 2, 2)
        c.execute("DETACH a")
        assert c.execute("SELECT alive, catalogs, leases FROM n6k_connections()").fetchone() == (True, 1, 1)
        assert c.execute("SELECT count(*) FROM b.main.users").fetchone()[0] == 3
        c.execute("DETACH b")
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()


def _sessions_pushed_to(catalog: str) -> int:
    request = urllib.request.Request(
        f"{SERVER_URL}/debug/push_invalidate?catalog={catalog}&schemas=main", method="POST"
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        return int(json.load(response)["sent"])


def test_detach_closes_the_session_on_the_server():
    c = _connect()
    catalog = f"cs_{uuid.uuid4().hex[:8]}"
    try:
        c.execute(f"ATTACH '{N6K_URL}' AS keep (TYPE n6k, CATALOG 'db')")
        c.execute(f"ATTACH '{N6K_URL}' AS gone (TYPE n6k, CATALOG '{catalog}')")
        assert _sessions_pushed_to(catalog) == 1
        c.execute("DETACH gone")
        deadline = time.monotonic() + TIMEOUT_S
        while _sessions_pushed_to(catalog) != 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert _sessions_pushed_to(catalog) == 0
        assert c.execute("SELECT alive, leases FROM n6k_connections()").fetchone() == (True, 1)
    finally:
        c.close()


def test_a_different_token_gets_its_own_connection():
    c = _connect()
    try:
        c.execute(f"ATTACH '{N6K_URL}' AS a (TYPE n6k, CATALOG 'db', TOKEN 'x')")
        c.execute(f"ATTACH '{N6K_URL}' AS b (TYPE n6k, CATALOG 'db', TOKEN 'y')")
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 2
    finally:
        c.close()


def test_n6k_connect_and_a_url_attach_share_a_connection():
    c = _connect()
    try:
        conn_id, _caps = _connect_url(c, N6K_URL)
        c.execute(f"ATTACH '{N6K_URL}' AS db (TYPE n6k)")
        assert c.execute("SELECT connection_id, leases FROM n6k_connections()").fetchall() == [(conn_id, 2)]
        # Drops n6k_connect's lease only: the ATTACH still holds the connection open.
        c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        assert c.execute("SELECT alive, leases FROM n6k_connections()").fetchone() == (True, 1)
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        c.execute("DETACH db")
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()


def test_two_n6k_connects_to_one_url_lease_one_connection():
    c = _connect()
    try:
        first, _caps = _connect_url(c, N6K_URL)
        second, _caps = _connect_url(c, N6K_URL)
        assert first == second
        assert c.execute("SELECT leases FROM n6k_connections()").fetchone()[0] == 2
        c.execute("SELECT * FROM n6k_disconnect(?)", [first]).fetchall()
        assert c.execute("SELECT leases FROM n6k_connections()").fetchone()[0] == 1
        c.execute("SELECT * FROM n6k_disconnect(?)", [first]).fetchall()
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()


def test_n6k_disconnect_cannot_take_an_attachs_lease():
    c = _connect()
    try:
        conn_id, _caps = _connect_url(c, N6K_URL)
        c.execute(f"ATTACH '{N6K_URL}' AS db (TYPE n6k)")
        c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        with pytest.raises(duckdb.Error, match="no n6k_connect lease"):
            c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        assert c.execute("SELECT alive, leases FROM n6k_connections()").fetchone() == (True, 1)
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()


def test_n6k_disconnect_refuses_a_connection_only_an_attach_opened():
    c = _connect()
    try:
        c.execute(f"ATTACH '{N6K_URL}' AS db (TYPE n6k)")
        conn_id = c.execute("SELECT connection_id FROM n6k_connections()").fetchone()[0]
        with pytest.raises(duckdb.Error, match="no n6k_connect lease"):
            c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()


def test_a_second_n6k_connect_with_on_text_raises():
    c = _connect()
    _register_inbox(c)
    try:
        _connect_url(c, N6K_URL, ", on_text => 'host_on_text'")
        with pytest.raises(duckdb.Error, match="already open"):
            _connect_url(c, N6K_URL, ", on_text => 'host_on_text'")
        assert c.execute("SELECT leases FROM n6k_connections()").fetchone()[0] == 1
    finally:
        c.close()


def test_on_text_on_an_open_plain_connection_raises():
    c = _connect()
    _register_inbox(c)
    try:
        _connect_url(c, N6K_URL)
        with pytest.raises(duckdb.Error, match="already open"):
            _connect_url(c, N6K_URL, ", on_text => 'host_on_text'")
    finally:
        c.close()


def test_detach_leaves_an_n6k_connect_connection_open():
    c = _connect()
    try:
        conn_id, _caps = _connect_url(c, f"{N6K_URL}?catalog=db")
        _attach(c, conn_id, "db")
        c.execute("DETACH db")
        assert c.execute("SELECT connection_id, alive, catalogs FROM n6k_connections()").fetchall() == [
            (conn_id, True, 0)
        ]
    finally:
        c.close()


def test_n6k_connect_to_a_closed_port_raises():
    c = _connect()
    try:
        t0 = time.monotonic()
        with pytest.raises(duckdb.Error):
            _connect_url(c, "n6k://127.0.0.1:1?catalog=db")
        assert time.monotonic() - t0 < 10
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()
