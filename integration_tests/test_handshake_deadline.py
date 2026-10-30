"""`n6k_serve_fd` enforces its FT_HELLO deadline across a partially sent frame.

Run:
    uv run pytest integration_tests/test_handshake_deadline.py -v
"""

import os
import socket
import struct
import threading

import duckdb
import pytest

from _paths import REPO_ROOT

from n6k_protocol.engine import unpack_frame
from n6k_protocol.protocol import FT_HELLO_ERR, STREAM_FRAME_N6K

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

HANDSHAKE_TIMEOUT_S = 5
GRACE_S = 5


@pytest.mark.parametrize("partial", [b"\x00\x00", b"\x00\x00\x00\x10\x02\x81"], ids=["prefix", "body"])
def test_a_partial_hello_is_rejected_at_the_deadline(partial: bytes) -> None:
    if not os.path.exists(SERVER_EXT_PATH):
        pytest.skip(f"Extension not built: {SERVER_EXT_PATH}")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.load_extension(SERVER_EXT_PATH)
    peer, served = socket.socketpair()
    session_id = con.execute("SELECT session_id FROM n6k_serve_create_session(?)", [served.fileno()]).fetchone()[0]

    def serve() -> None:
        try:
            con.execute("CALL n6k_serve_fd(?)", [session_id]).fetchall()
        except BaseException:
            pass

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    peer.sendall(partial)
    worker.join(HANDSHAKE_TIMEOUT_S + GRACE_S)
    hung = worker.is_alive()
    peer.settimeout(GRACE_S)
    reply = b"" if hung else peer.makefile("rb").read()
    peer.close()
    worker.join(GRACE_S)
    served.close()

    assert not hung, "n6k_serve_fd blocked past its deadline on a partial FT_HELLO"
    n, frame_type = struct.unpack(">IB", reply[:5])
    assert frame_type == STREAM_FRAME_N6K
    assert unpack_frame(reply[5 : 5 + n])[0]["t"] == FT_HELLO_ERR
