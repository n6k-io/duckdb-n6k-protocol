"""Transport-drop hardening.

Covers:
  - Client-side DETACH destroys the WsClient and in-flight requests surface
    as clean DuckDB IOException (no hang, no crash).
  - Server-side forced close (/debug/ws/close_all) is transparently recovered:
    the next request reconnects and succeeds (no hang, no crash).

Run:
    uv run pytest integration_tests/test_ws_drop_native.py -v
"""

import os
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip(f"test server not running on {SERVER_URL}")


@pytest.fixture
def con():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.execute(f"LOAD '{EXT_PATH}'")
    yield c
    try:
        c.execute("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id >= 10000')").fetchone()
    except Exception:
        pass


def _ws_close_all():
    urllib.request.urlopen(
        urllib.request.Request(f"{SERVER_URL}/debug/ws/close_all", method="POST"),
        timeout=2,
    ).read()


def test_detach_clears_strict_client_and_re_attach_works(con):
    con.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    rows = con.execute("SELECT count(*) FROM db.main.users").fetchone()
    assert rows[0] >= 3

    con.execute("DETACH db")

    # Re-ATTACH same URL; must open a fresh WS (new HELLO_ACK, new session).
    con.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    rows = con.execute("SELECT count(*) FROM db.main.users").fetchone()
    assert rows[0] >= 3


def test_server_force_close_auto_reconnects(con):
    """Kill every live connection server-side, then run a query: it must
    transparently reconnect and succeed — no hang, no segfault, and no manual
    DETACH/re-ATTACH. Native shares wasm's per-attach reconnect (see the
    wasm-side reconnect.test.ts); a dropped socket is re-established lazily on
    the next request."""
    con.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    # Open a live session first, so the force-close drops an established socket.
    assert con.execute("SELECT count(*) FROM db.main.users").fetchone()[0] >= 3

    # Trigger server-side force-close of our session.
    _ws_close_all()

    # The next scan re-establishes the dropped socket and returns data cleanly.
    rows = con.execute("SELECT count(*) FROM db.main.users").fetchone()
    assert rows[0] >= 3
