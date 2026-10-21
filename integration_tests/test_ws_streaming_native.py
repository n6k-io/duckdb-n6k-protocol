"""SCAN and QUERY stream over the WS with credit flow control.

Requires the test server on :8099.

Run:
    uv run pytest integration_tests/test_ws_streaming_native.py -v
"""

import json
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
    c.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    yield c
    # cleanup rows inserted for streaming tests
    try:
        c.execute("SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id >= 10000')").fetchone()
    except Exception:
        pass


def _reset():
    urllib.request.urlopen(urllib.request.Request(f"{SERVER_URL}/debug/counts/reset", method="POST"), timeout=2).read()


def _counts() -> dict:
    with urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2) as r:
        return json.loads(r.read())


def _seed(con, n: int):
    values = ", ".join(f"({10000+i}, 's{i}', {i})" for i in range(n))
    con.execute(f"INSERT INTO db.main.users VALUES {values}")


# ─────────────────────────────────────────────────────────────────────────────


def test_scan_streams_and_no_http_hit(con):
    _seed(con, 25)
    _reset()
    rows = con.execute("SELECT id FROM db.main.users WHERE id >= 10000 ORDER BY id").fetchall()
    assert len(rows) == 25
    counts = _counts()
    scan_hits = sum(v for k, v in counts.get("http", {}).items() if k.startswith("POST /main/users/scan"))
    assert scan_hits == 0, counts


def test_query_streams_and_no_http_hit(con):
    _seed(con, 25)
    _reset()
    rows = con.execute(
        "SELECT * FROM n6k_catalog_query('db', 'SELECT id FROM db.main.users WHERE id >= 10000 ORDER BY id')"
    ).fetchall()
    assert len(rows) == 25
    counts = _counts()
    assert counts.get("http", {}).get("POST /query", 0) == 0, counts


def test_credit_backpressure_observable(con):
    """With ≫ DEFAULT_BATCH_CREDITS chunks, the server must pause
    at least once waiting on CREDIT refill from the client."""
    _reset()
    # Enough rows to fill many vectors, so the credit window is certainly exhausted:
    # the reactor streams a whole DuckDB vector per chunk, so row count, not batch
    # size, is what makes backpressure observable.
    rows = con.execute("SELECT n FROM db.n6k_testing_stream_counter(40000)").fetchall()
    assert len(rows) == 40000
    counts = _counts()
    pauses = counts.get("ws", {}).get("credit_pauses", 0)
    assert pauses >= 1, counts


def test_full_drain_does_not_send_spurious_cancel(con):
    """A stream read to completion must not fire a CANCEL on release."""
    _seed(con, 10)
    _reset()
    rows = con.execute("SELECT id FROM db.main.users WHERE id >= 10000 ORDER BY id").fetchall()
    assert len(rows) == 10
    counts = _counts()
    assert counts.get("ws", {}).get("cancels", 0) == 0, counts
