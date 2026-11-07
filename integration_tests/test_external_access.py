"""With `enable_external_access = false`, n6k_client opens no new connections."""

from __future__ import annotations

import os
import socket
import threading
import time
from typing import Iterator

import duckdb
import pytest

from _paths import EXT_PATH

pytestmark = pytest.mark.skipif(not os.path.exists(EXT_PATH), reason=f"not built: {EXT_PATH} (run `make release`)")

ERROR = "external access is disabled"


class Listener:
    def __init__(self) -> None:
        self.accepted = 0
        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen()
        self.port = int(self._sock.getsockname()[1])
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            self.accepted += 1
            conn.close()

    def close(self) -> None:
        self._sock.close()


@pytest.fixture()
def listener() -> Iterator[Listener]:
    instance = Listener()
    yield instance
    instance.close()


def _connect() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true", "allow_persistent_secrets": "false"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


def _lock(c: duckdb.DuckDBPyConnection) -> None:
    c.execute("SET enable_external_access = false")


def _assert_refused(c: duckdb.DuckDBPyConnection, sql: str, listener: Listener) -> None:
    with pytest.raises(duckdb.PermissionException, match=ERROR):
        c.execute(sql).fetchall()
    time.sleep(0.2)
    assert listener.accepted == 0


def test_attach_with_a_secret_is_refused(listener: Listener) -> None:
    c = _connect()
    c.execute("CREATE SECRET s (TYPE n6k, TOKEN 'tok')")
    _lock(c)
    _assert_refused(c, f"ATTACH 'n6k://127.0.0.1:{listener.port}' AS x (TYPE n6k)", listener)


def test_n6k_login_is_refused(listener: Listener) -> None:
    c = _connect()
    _lock(c)
    _assert_refused(c, f"SELECT * FROM n6k_login('http://127.0.0.1:{listener.port}')", listener)


def test_refresh_grant_mint_is_refused(listener: Listener) -> None:
    c = _connect()
    c.execute(
        "CREATE SECRET r (TYPE n6k_refresh, REFRESH_TOKEN 'rt', CLIENT_ID 'id', "
        f"TOKEN_ENDPOINT 'http://127.0.0.1:{listener.port}/token')"
    )
    _lock(c)
    _assert_refused(c, f"ATTACH 'n6k://127.0.0.1:{listener.port}' AS x (TYPE n6k)", listener)


def test_token_exchange_mint_is_refused(listener: Listener) -> None:
    c = _connect()
    c.execute("CREATE SECRET r (TYPE n6k_refresh, SUBJECT_TOKEN 'st', " f"ISSUER 'http://127.0.0.1:{listener.port}')")
    _lock(c)
    _assert_refused(c, f"ATTACH 'n6k://127.0.0.1:{listener.port}' AS x (TYPE n6k)", listener)


def test_an_open_connection_keeps_working_and_cannot_reconnect(server: int) -> None:
    c = _connect()
    row = c.execute(f"SELECT connection_id FROM n6k_connect(url => 'n6k://localhost:{server}')").fetchone()
    assert row is not None
    connection_id = row[0]
    c.execute(f"ATTACH '' AS db (TYPE n6k, CONNECTION '{connection_id}')")
    _lock(c)

    def users() -> int:
        count = c.execute("SELECT count(*) FROM db.main.users").fetchone()
        assert count is not None
        return int(count[0])

    assert users() == 3
    with pytest.raises(duckdb.PermissionException, match=ERROR):
        c.execute(f"SELECT * FROM n6k_reconnect('{connection_id}')").fetchall()
    alive = c.execute("SELECT alive FROM n6k_connections()").fetchone()
    assert alive == (True,)
    assert users() == 3
