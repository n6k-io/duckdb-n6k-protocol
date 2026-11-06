"""OP_CREATE_SCHEMA refuses a malformed request and a catalog that grants no create.

Run:
    uv run pytest integration_tests/test_create_schema_wire.py -v
"""

import os
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from _paths import EXT_PATH, REPO_ROOT
from _targets import TARGETS, Target, connect

from n6k_protocol.protocol import OP_CREATE_SCHEMA

SERVER_EXT = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")


def _client() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


@contextmanager
def _serving(attach_sql: str, catalog: str) -> Iterator[int]:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.load_extension(SERVER_EXT)
    con.execute(attach_sql)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    def serve() -> None:
        try:
            con.cursor().execute(f"CALL n6k_serve_http('127.0.0.1', {port}, '{catalog}')")
        except duckdb.InterruptException:
            pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    deadline = time.time() + 30
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            break
        except OSError:
            assert thread.is_alive(), "n6k_serve_http exited"
            assert time.time() < deadline, "n6k_serve_http never started listening"
            time.sleep(0.05)
    try:
        yield port
    finally:
        con.interrupt()
        thread.join(30)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.parametrize(
    "args",
    [
        {"schema": "", "on_conflict": "error"},
        {"schema": "never_created", "on_conflict": "replace"},
        {"schema": "never_created", "on_conflict": ""},
    ],
    ids=["no-schema", "replace", "no-on-conflict"],
)
async def test_malformed_create_schema_is_refused(target: Target, args: dict) -> None:
    async with connect(target) as (client, _hello):
        err = await client.error(OP_CREATE_SCHEMA, **args)
        assert err["exception_type"] == "InvalidInputException"


def test_bridge_catalog_refuses(server: int) -> None:
    conn = _client()
    # The `bridge_db` alias gets a bridge-backed catalog, whose schemas grant no create.
    conn.execute(f"ATTACH 'n6k://localhost:{server}' AS bridge_db (TYPE n6k)")
    with pytest.raises(duckdb.PermissionException, match="does not allow CREATE SCHEMA"):
        conn.execute("CREATE SCHEMA bridge_db.refused")
    conn.close()


def test_read_only_catalog_refuses(tmp_path: Path) -> None:
    path = str(tmp_path / "ro.duckdb")
    duckdb.connect(path).close()
    with _serving(f"ATTACH '{path}' AS ro (READ_ONLY)", "ro") as port:
        conn = _client()
        conn.execute(f"ATTACH 'n6k://localhost:{port}' AS r (TYPE n6k, CATALOG 'ro')")
        with pytest.raises(duckdb.PermissionException, match="does not allow CREATE SCHEMA"):
            conn.execute("CREATE SCHEMA r.refused")
        conn.close()


def test_multi_statement_execute_creates_schema_then_table() -> None:
    with _serving("ATTACH ':memory:' AS app", "app") as port:
        conn = _client()
        conn.execute(f"ATTACH 'n6k://localhost:{port}' AS r (TYPE n6k, CATALOG 'app')")
        conn.execute("CREATE SCHEMA r.s1; CREATE TABLE r.s1.x (i INT);")
        assert conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name = 's1'").fetchall() == [("x",)]
        conn.close()
