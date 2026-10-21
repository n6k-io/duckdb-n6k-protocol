"""`n6k_handshake` enforces its FT_HELLO deadline across a partially sent frame.

Run:
    uv run pytest integration_tests/test_handshake_deadline.py -v
"""

import os
import socket
import threading

import duckdb
import pytest

from _paths import REPO_ROOT

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
    outcome: list[BaseException | None] = []

    def handshake() -> None:
        try:
            con.execute(f"SELECT * FROM n6k_handshake({served.fileno()})").fetchall()
            outcome.append(None)
        except BaseException as e:
            outcome.append(e)

    worker = threading.Thread(target=handshake, daemon=True)
    worker.start()
    peer.sendall(partial)
    worker.join(HANDSHAKE_TIMEOUT_S + GRACE_S)
    hung = worker.is_alive()
    peer.close()
    worker.join(GRACE_S)
    served.close()

    assert not hung, "n6k_handshake blocked past its deadline on a partial FT_HELLO"
    assert outcome and outcome[0] is not None
