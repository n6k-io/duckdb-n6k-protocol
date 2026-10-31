"""`CALL enable_logging('WS')` records one row per n6k frame, on the client and on the server.

uv run pytest integration_tests/test_ws_logging.py -v
"""

from __future__ import annotations

import os
import socket
import threading
import time
from collections.abc import Iterator
from typing import List, Optional

import duckdb
import pytest

from _paths import EXT_PATH, REPO_ROOT

SERVER_EXT = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")
TIMEOUT = 10
ROWS = 100_000

pytestmark = [
    pytest.mark.skipif(not os.path.exists(SERVER_EXT), reason=f"not built: {SERVER_EXT} (run `make release`)"),
    pytest.mark.skipif(not os.path.exists(EXT_PATH), reason=f"not built: {EXT_PATH} (run `make release`)"),
]

WS_ROWS = """
    SELECT direction, frame_type, req_id, ns, bytes
    FROM duckdb_logs_parsed('WS')
"""


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


class Served:
    def __init__(self, con: duckdb.DuckDBPyConnection, port: int) -> None:
        self.con = con
        self.port = port


@pytest.fixture()
def served() -> Iterator[Served]:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.load_extension(SERVER_EXT)
    con.execute("CALL enable_logging('WS', storage = 'memory')")
    con.execute("ATTACH ':memory:' AS db")
    con.execute(f"CREATE TABLE db.a AS SELECT range::BIGINT AS i FROM range({ROWS})")
    port = _free_port()
    errors: List[BaseException] = []

    def serve() -> None:
        try:
            con.cursor().execute(f"CALL n6k_serve_http('127.0.0.1', {port}, 'db')")
        except BaseException as e:  # noqa: B036
            errors.append(e)

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    try:
        deadline = time.time() + TIMEOUT
        while not _port_open(port):
            assert t.is_alive(), errors
            assert time.time() < deadline, "n6k_serve_http never started listening"
            time.sleep(0.05)
        yield Served(con, port)
    finally:
        con.interrupt()
        t.join(TIMEOUT)


def _client(port: int, log: bool) -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    if log:
        c.execute("CALL enable_logging('WS', storage = 'memory')")
    c.execute(f"ATTACH 'n6k://127.0.0.1:{port}' AS r (TYPE n6k, CATALOG 'db')")
    return c


def _rows(con: duckdb.DuckDBPyConnection) -> List[tuple[str, Optional[str], Optional[int], Optional[int], int]]:
    return con.cursor().execute(WS_ROWS).fetchall()


def test_client_and_server_log_each_frame(served: Served) -> None:
    client = _client(served.port, log=True)
    assert client.execute("SELECT sum(i) FROM r.main.a").fetchone() == (ROWS * (ROWS - 1) // 2,)

    client_rows = _rows(client)
    sent = {(r[1], r[2], r[3]) for r in client_rows if r[0] == "send"}
    recv = [r for r in client_rows if r[0] == "recv"]
    assert any(t == "REQ" for t, _, _ in sent), client_rows
    assert any(r[1] == "HELLO_ACK" for r in recv), client_rows
    assert any(r[1] == "OPEN_ACK" for r in recv), client_rows
    chunks = [r for r in recv if r[1] == "RESP_CHUNK"]
    assert chunks and all(r[4] > 0 for r in chunks), client_rows

    server_rows = _rows(served.con)
    server_recv = {(r[1], r[2], r[3]) for r in server_rows if r[0] == "recv"}
    server_sent = [r for r in server_rows if r[0] == "send"]
    client_reqs = {(t, req_id, ns) for t, req_id, ns in sent if t == "REQ"}
    assert client_reqs <= server_recv, (client_reqs, server_recv)
    server_chunk_bytes = sum(r[4] for r in server_sent if r[1] == "RESP_CHUNK")
    assert server_chunk_bytes == sum(r[4] for r in chunks)
    client.close()


def test_nothing_logged_unless_enabled(served: Served) -> None:
    client = _client(served.port, log=False)
    client.execute("SELECT sum(i) FROM r.main.a").fetchone()
    assert client.execute("SELECT count(*) FROM duckdb_logs WHERE type = 'WS'").fetchone() == (0,)
    client.close()
