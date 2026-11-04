"""A connection over a socket the host already connected (`n6k_connect(wsFd => …)`),
instead of letting the extension dial the server itself.

The host (here, this test) owns the WebSocket end-to-end:
  1. socketpair() -> (py_sock, ext_sock)
  2. open the real WebSocket to the server and let it complete the upgrade
  3. two daemon threads pump frames between ws <-> py_sock, stream-framed on the
     socket side (STREAM_FRAME_* in packages/python/src/n6k_protocol/protocol.py)
  4. `n6k_connect(wsFd => <fd>, token => ...)` sends the connection's one FT_HELLO
     and returns its connection_id
  5. ATTACH '' AS db (TYPE n6k, CONNECTION '<id>') sends only FT_OPEN

Runs against the always-on test server on :8099. We never spawn a server here.

Run:
    uv run pytest integration_tests/test_wsfd_attach.py -v
"""

import os
import socket
import struct
import threading
import time
import urllib.error
import urllib.request

import duckdb
import pytest
from websockets.sync.client import connect as ws_connect

from _paths import EXT_PATH

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
WS_URL = f"ws://localhost:{SERVER_PORT}/ws?catalog=db"
AUTH_WS_URL = f"ws://localhost:{SERVER_PORT}/auth/ws?catalog=db"
AUTH_TOKEN = "n6k-test-token"


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


def _recv_exactly(sock, n):
    """Read exactly n bytes; return None on EOF/error (peer closed)."""
    chunks = []
    got = 0
    while got < n:
        try:
            b = sock.recv(n - got)
        except OSError:
            return None
        if not b:
            return None
        chunks.append(b)
        got += len(b)
    return b"".join(chunks)


def _start_pump(ws, sock):
    """Pump stream-framed frames between the WebSocket and the raw socket.

    ws  -> sock: each WS message becomes one stream frame; text is JSONRPC, binary is N6K.
    sock -> ws : read one stream frame, send it as one WS message (JSONRPC as text).

    Returns the (ws->sock, sock->ws) threads so a test can join on the second one
    to observe the extension closing its end.
    """

    def ws_to_sock():
        try:
            for msg in ws:
                if isinstance(msg, str):
                    sock.sendall(struct.pack(">IB", len(msg.encode()), 0x01) + msg.encode())
                else:
                    sock.sendall(struct.pack(">IB", len(msg), 0x02) + msg)
        except Exception:
            pass
        finally:
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def sock_to_ws():
        try:
            while True:
                header = _recv_exactly(sock, 5)
                if header is None:
                    break
                n, frame_type = struct.unpack(">IB", header)
                body = _recv_exactly(sock, n)
                if body is None:
                    break
                ws.send(body.decode() if frame_type == 0x01 else body)
        except Exception:
            pass
        finally:
            try:
                ws.close()
            except Exception:
                pass

    t_out = threading.Thread(target=ws_to_sock, daemon=True)
    t_in = threading.Thread(target=sock_to_ws, daemon=True)
    t_out.start()
    t_in.start()
    return t_out, t_in


def _open_pumped(url=WS_URL):
    py_sock, ext_sock = socket.socketpair()
    ws = ws_connect(url)
    _t_out, t_in = _start_pump(ws, py_sock)
    return py_sock, ext_sock, ws, t_in


def _n6k_connect(c, fd, token=""):
    """The connection_id n6k_connect returns, as a string for ATTACH."""
    return str(c.execute("SELECT connection_id FROM n6k_connect(wsFd => ?, token => ?)", [fd, token]).fetchone()[0])


def _attach(c, conn_id, alias="db", catalog=None):
    extra = f", CATALOG '{catalog}'" if catalog else ""
    c.execute(f"ATTACH '' AS {alias} (TYPE n6k, CONNECTION '{conn_id}'{extra})")


def _connect_and_attach(c, alias="db"):
    """Open the WS, start the pump, connect, and ATTACH over the resulting connection.

    Returns (conn_id, py_sock, ws, sock_to_ws_thread). Closes ext_sock once the extension
    has dup()'d it, so the extension's copy alone keeps the endpoint open.
    """
    py_sock, ext_sock, ws, t_in = _open_pumped()
    conn_id = _n6k_connect(c, ext_sock.fileno())
    ext_sock.close()
    _attach(c, conn_id, alias)
    return conn_id, py_sock, ws, t_in


def test_n6k_connect_returns_an_id_and_the_granted_capabilities():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    try:
        conn_id, caps = c.execute("SELECT * FROM n6k_connect(wsFd => ?, token => '')", [ext_sock.fileno()]).fetchone()
        assert caps == ["crossing"]
        rows = c.execute("SELECT connection_id, alive, catalogs, leases FROM n6k_connections()").fetchall()
        assert rows == [(conn_id, True, 0, 1)]
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_connection_attaches_and_queries():
    c = _connect()
    _conn_id, py_sock, ws, _t_in = _connect_and_attach(c)
    try:
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        names = [r[0] for r in c.execute("SELECT name FROM db.main.users ORDER BY name").fetchall()]
        assert len(names) == 3
        assert c.execute("SELECT catalogs FROM n6k_connections()").fetchone()[0] == 1
    finally:
        c.close()
        ws.close()
        py_sock.close()


def test_the_last_lease_released_closes_the_connection():
    c = _connect()
    conn_id, py_sock, ws, t_in = _connect_and_attach(c)
    try:
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        c.execute("DETACH db")
        assert c.execute("SELECT alive, catalogs FROM n6k_connections()").fetchone() == (True, 0)
        _attach(c, conn_id)
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3

        # The ATTACH still holds a lease, so the connection outlives n6k_disconnect.
        c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        c.execute("DETACH db")
        # The extension's dup was the last owner of the endpoint, so the pump's reader
        # on py_sock hits EOF and the sock->ws thread exits.
        t_in.join(timeout=5)
        assert not t_in.is_alive(), "extension did not close its end on the last release"
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()
        ws.close()
        py_sock.close()


def test_closing_the_database_releases_the_socket():
    c = _connect()
    _conn_id, py_sock, ws, t_in = _connect_and_attach(c)
    try:
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        c.close()
        t_in.join(timeout=5)
        assert not t_in.is_alive(), "closing the database left the extension's end open"
    finally:
        ws.close()
        py_sock.close()


def test_connection_ids_are_per_database():
    c = _connect()
    other = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        with pytest.raises(duckdb.Error, match="no open connection"):
            _attach(other, conn_id)
        assert other.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()
        other.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_connection_mutually_exclusive_with_url():
    c = _connect()
    try:
        with pytest.raises(duckdb.Error, match="mutually exclusive"):
            c.execute(
                f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db "
                "(TYPE n6k, CONNECTION '00000000-0000-4000-8000-000000000001')"
            )
    finally:
        c.close()


def test_connection_with_an_inline_token_is_a_bind_error():
    c = _connect()
    try:
        with pytest.raises(duckdb.Error, match="cannot be combined"):
            c.execute("ATTACH '' AS db (TYPE n6k, CONNECTION '00000000-0000-4000-8000-000000000001', token 'x')")
    finally:
        c.close()


def test_connection_ignores_a_global_unscoped_secret():
    c = _connect()
    c.execute("CREATE SECRET g (TYPE n6k, TOKEN 'unused')")
    _conn_id, py_sock, ws, _t_in = _connect_and_attach(c)
    try:
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()
        ws.close()
        py_sock.close()


def test_n6k_connect_carries_the_token():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped(AUTH_WS_URL)
    try:
        _attach(c, _n6k_connect(c, ext_sock.fileno(), AUTH_TOKEN))
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_n6k_connect_with_a_bad_token_raises():
    c = _connect()
    py_sock, ext_sock, ws, t_in = _open_pumped(AUTH_WS_URL)
    try:
        with pytest.raises(duckdb.Error, match="unauthorized"):
            _n6k_connect(c, ext_sock.fileno(), "wrong-token")
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
        # ext_sock is still open here, so this EOF can only come from the extension's shutdown.
        t_in.join(timeout=5)
        assert not t_in.is_alive(), "host pump never saw EOF after the failed handshake"
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_n6k_connect_resolves_the_token_from_a_scoped_secret():
    c = _connect()
    c.execute(f"CREATE SECRET s (TYPE n6k, TOKEN '{AUTH_TOKEN}', SCOPE 'localhost:{SERVER_PORT}')")
    py_sock, ext_sock, ws, _t_in = _open_pumped(AUTH_WS_URL)
    try:
        conn_id = c.execute(
            "SELECT connection_id FROM n6k_connect(wsFd => ?, scope => ?)",
            [ext_sock.fileno(), f"n6k://localhost:{SERVER_PORT}"],
        ).fetchone()[0]
        _attach(c, conn_id)
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_n6k_connect_with_a_scope_no_secret_matches_is_anonymous():
    c = _connect()
    c.execute(f"CREATE SECRET s (TYPE n6k, TOKEN '{AUTH_TOKEN}', SCOPE 'otherhost:{SERVER_PORT}')")
    py_sock, ext_sock, ws, _t_in = _open_pumped(AUTH_WS_URL)
    try:
        with pytest.raises(duckdb.Error, match="unauthorized"):
            c.execute(
                "SELECT * FROM n6k_connect(wsFd => ?, scope => ?)", [ext_sock.fileno(), f"localhost:{SERVER_PORT}"]
            ).fetchall()
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_n6k_connect_inline_token_wins_over_a_scoped_secret():
    c = _connect()
    c.execute(f"CREATE SECRET s (TYPE n6k, TOKEN '{AUTH_TOKEN}', SCOPE 'localhost:{SERVER_PORT}')")
    py_sock, ext_sock, ws, _t_in = _open_pumped(AUTH_WS_URL)
    try:
        with pytest.raises(duckdb.Error, match="unauthorized"):
            c.execute(
                "SELECT * FROM n6k_connect(wsFd => ?, token => 'wrong-token', scope => ?)",
                [ext_sock.fileno(), f"localhost:{SERVER_PORT}"],
            ).fetchall()
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_a_corrupt_stream_gives_the_host_pump_eof():
    c = _connect()
    py_sock, ext_sock, ws, t_in = _open_pumped()
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        # A 5-byte stream header naming no frame type the extension knows.
        py_sock.sendall(b"\x00\x00\x00\x00\xff")
        # ext_sock is still open here, so this EOF can only come from the extension's shutdown.
        t_in.join(timeout=5)
        assert not t_in.is_alive(), "host pump never saw EOF after the reader gave up"
        assert c.execute("SELECT alive FROM n6k_connections()").fetchone()[0] is False
        c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        assert c.execute("SELECT count(*) FROM n6k_connections()").fetchone()[0] == 0
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_a_second_connect_on_the_same_socket_shares_it():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        assert _n6k_connect(c, ext_sock.fileno()) == conn_id
        assert c.execute("SELECT leases FROM n6k_connections()").fetchone()[0] == 2
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_n6k_connect_on_a_non_socket_fd_raises():
    c = _connect()
    r, w = os.pipe()
    try:
        with pytest.raises(duckdb.Error, match="is not an open socket"):
            _n6k_connect(c, r)
    finally:
        c.close()
        os.close(r)
        os.close(w)


def test_n6k_connect_times_out_on_a_silent_peer():
    c = _connect()
    silent, ext_sock = socket.socketpair()
    try:
        with pytest.raises(duckdb.Error, match="no reply within"):
            _n6k_connect(c, ext_sock.fileno())
    finally:
        c.close()
        ext_sock.close()
        silent.close()


def test_n6k_connect_waits_through_hello_pending():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped(f"ws://localhost:{SERVER_PORT}/ws?catalog=slowauth_7000")
    try:
        t0 = time.monotonic()
        _n6k_connect(c, ext_sock.fileno())
        assert time.monotonic() - t0 >= 7.0
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def _list_catalogs(c, conn_id):
    return [r[0] for r in c.execute("SELECT * FROM n6k_list_catalogs(connection => ?)", [conn_id]).fetchall()]


def test_list_catalogs_before_attach():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        assert _list_catalogs(c, conn_id) == ["db"]
        _attach(c, conn_id)
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_list_catalogs_after_attach():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        _attach(c, conn_id)
        assert _list_catalogs(c, conn_id) == ["db"]
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_list_catalogs_after_two_attaches():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped(f"ws://localhost:{SERVER_PORT}/tuned/ws?catalogs=a,b")
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        _attach(c, conn_id, "a")
        _attach(c, conn_id, "b")
        assert sorted(_list_catalogs(c, conn_id)) == ["a", "b"]
        assert c.execute("SELECT catalog FROM a.main.marker").fetchone()[0] == "a"
        assert c.execute("SELECT catalog FROM b.main.marker").fetchone()[0] == "b"
        assert c.execute("SELECT catalogs FROM n6k_connections()").fetchone()[0] == 2
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_list_catalogs_repeated():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        _attach(c, conn_id)
        assert _list_catalogs(c, conn_id) == ["db"]
        assert _list_catalogs(c, conn_id) == ["db"]
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_list_catalogs_after_disconnect_raises():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    try:
        conn_id = _n6k_connect(c, ext_sock.fileno())
        c.execute("SELECT * FROM n6k_disconnect(?)", [conn_id]).fetchall()
        with pytest.raises(duckdb.Error, match="no open connection"):
            _list_catalogs(c, conn_id)
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def test_a_dropped_socket_is_not_redialed():
    c = _connect()
    conn_id, py_sock, ws, _t_in = _connect_and_attach(c)
    try:
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
        py_sock.close()
        deadline = time.monotonic() + 5
        while c.execute("SELECT alive FROM n6k_connections()").fetchone()[0] and time.monotonic() < deadline:
            time.sleep(0.05)
        assert c.execute("SELECT alive FROM n6k_connections()").fetchone()[0] is False
        with pytest.raises(duckdb.Error, match=f"connection {conn_id}"):
            c.execute("SELECT count(*) FROM db.main.users").fetchone()
        with pytest.raises(duckdb.Error, match="cannot be redialed"):
            c.execute("SELECT * FROM n6k_reconnect(?)", [conn_id]).fetchall()
    finally:
        c.close()
        ws.close()


def test_n6k_connect_works_on_a_nonblocking_fd():
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped()
    ext_sock.setblocking(False)
    try:
        _attach(c, _n6k_connect(c, ext_sock.fileno()))
        assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()
