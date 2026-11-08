"""A server that has the client extension loaded too, as a host serving and attaching from one process does.

The server runs in a subprocess, so a crash fails the test instead of the run.

Run:
    uv run pytest integration_tests/test_server_with_client_loaded.py -v
"""

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator

import duckdb
import pytest

from _paths import EXT_PATH, REPO_ROOT

SERVER_EXT = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")

pytestmark = pytest.mark.skipif(not os.path.exists(SERVER_EXT), reason=f"Extension not built: {SERVER_EXT}")

_SERVE = """
import sys, duckdb
client_ext, server_ext, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.load_extension(client_ext)
con.load_extension(server_ext)
con.execute("ATTACH ':memory:' AS r")
con.execute(f"CALL n6k_serve_http('127.0.0.1', {port}, 'r')")
"""


@pytest.fixture
def served() -> Iterator[tuple[duckdb.DuckDBPyConnection, subprocess.Popen[str]]]:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = subprocess.Popen(
        [sys.executable, "-c", _SERVE, EXT_PATH, SERVER_EXT, str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 30
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            break
        except OSError:
            assert server.poll() is None, "the server exited before it listened"
            assert time.time() < deadline, "the server never listened"
            time.sleep(0.05)
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    c.execute(f"ATTACH 'n6k://127.0.0.1:{port}' AS r (TYPE n6k)")
    yield c, server
    c.close()
    server.kill()
    server.wait()


def test_a_refused_insert_leaves_the_server_running(
    served: tuple[duckdb.DuckDBPyConnection, subprocess.Popen[str]],
) -> None:
    c, server = served
    c.execute("CREATE TABLE r.t_c (i INT CHECK (i > 0))")
    with pytest.raises(duckdb.ConstraintException):
        c.execute("INSERT INTO r.t_c VALUES (-1)")
    assert c.execute("SELECT count(*) FROM r.t_c").fetchall() == [(0,)]
    assert server.poll() is None, "the server exited"
