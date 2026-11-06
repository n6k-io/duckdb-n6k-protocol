"""OP_HAS_FUNCTION: whether a server has a function taking exactly these argument types.

Run:
    uv run pytest integration_tests/test_has_function.py -v
"""

import os
import socket
import threading
from collections.abc import Iterator

import duckdb
import pytest

from _paths import EXT_PATH, REPO_ROOT
from _targets import TARGETS, Target, connect

from n6k_protocol.protocol import FT_RESP_END, OP_HAS_FUNCTION

SERVER_EXT_PATH = os.path.join(REPO_ROOT, "build", "release", "extension", "n6k_server", "n6k_server.duckdb_extension")


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
@pytest.mark.parametrize(
    "name, args, exists",
    [
        ("upper", ["VARCHAR"], True),
        ("UPPER", ["VARCHAR"], True),
        ("upper", ["INTEGER"], False),
        ("sqrt", ["DOUBLE"], True),
        ("sqrt", ["INTEGER"], False),
        ("abs", ["DECIMAL(10,2)"], True),
        ("abs", ["VARCHAR"], False),
        ("sum", ["INTEGER"], True),
        ("count_star", [], True),
        ("concat", ["VARCHAR", "VARCHAR", "VARCHAR"], True),
        ("list_contains", ["INTEGER[]", "INTEGER"], True),
        ("struct_extract", ["STRUCT(a INTEGER, b VARCHAR)", "VARCHAR"], True),
        ("struct_extract", ["STRUCT(a INTEGER, b VARCHAR)"], False),
        ("struct_pack", ["INTEGER", "VARCHAR"], True),
        ("no_such_function", [], False),
        ("read_csv", ["VARCHAR"], False),
        ("upper", ["NOT_A_TYPE"], False),
    ],
)
async def test_answer(target: Target, name: str, args: list[str], exists: bool) -> None:
    async with connect(target) as (client, _hello):
        frames = await client.request(OP_HAS_FUNCTION, name=name, args=args)
        end = [h for h, _ in frames if h["t"] == FT_RESP_END][0]
        assert end["exists"] is exists


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_an_empty_name_is_refused(target: Target) -> None:
    async with connect(target) as (client, _hello):
        err = await client.error(OP_HAS_FUNCTION, name="", args=[])
        assert err["exception_type"] == "InvalidInputException"


@pytest.fixture
def served() -> Iterator[tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyConnection]]:
    if not os.path.exists(SERVER_EXT_PATH):
        pytest.skip(f"Extension not built: {SERVER_EXT_PATH}")
    server = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    server.load_extension(SERVER_EXT_PATH)
    server.execute("ATTACH ':memory:' AS a")
    server.execute("CREATE TABLE a.main.t AS SELECT 1::BIGINT AS i")
    client_end, server_end = socket.socketpair()
    session_id = server.execute("SELECT session_id FROM n6k_serve_create_session(?)", [server_end.fileno()]).fetchone()[
        0
    ]

    def serve() -> None:
        server.execute(f"CALL n6k_serve_fd('{session_id}'::UUID, 'a')").fetchall()

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()

    client = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    client.load_extension("httpfs")
    client.load_extension(EXT_PATH)
    connection_id = client.execute(
        "SELECT connection_id FROM n6k_connect(wsFd => ?, token => '')", [client_end.fileno()]
    ).fetchone()[0]
    client.execute(f"ATTACH '' AS a (TYPE n6k, CONNECTION '{connection_id}')")
    yield client, server.cursor()
    client.close()
    server_end.shutdown(socket.SHUT_RDWR)
    worker.join(timeout=10)
    client_end.close()
    server_end.close()


def test_answers_are_cached_until_invalidated(
    served: tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyConnection],
) -> None:
    client, server = served
    client.create_function("shared_name", lambda x: x + 1, ["BIGINT"], "BIGINT")
    sql = "SELECT shared_name(i) FROM a.main.t"
    assert client.execute(sql).fetchall() == [(2,)]

    server.create_function("shared_name", lambda x: x + 1000, ["BIGINT"], "BIGINT")
    assert client.execute(sql).fetchall() == [(2,)]

    client.execute("SELECT * FROM n6k_invalidate_cache('a', 'main')").fetchall()
    assert client.execute(sql).fetchall() == [(1001,)]
