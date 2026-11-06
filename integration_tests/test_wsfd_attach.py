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


def _list_catalogs(c, conn_id):
    return [r[0] for r in c.execute("SELECT * FROM n6k_list_catalogs(connection => ?)", [conn_id]).fetchall()]
