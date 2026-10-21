"""A catalog whose backend, or whose authentication, takes longer than the client's connect
timeout still ATTACHes: the test server's `slowopen_<ms>` / `slowopen_fail_<ms>` catalogs answer OPEN
with FT_OPEN_PENDING until their attach finishes, and `slowauth_<ms>` answers HELLO with
FT_HELLO_PENDING until authenticate returns.

Run:
    uv run pytest integration_tests/test_open_pending.py -v
"""

import os
import time
from typing import Any

import duckdb
import pytest
import websockets

from _paths import EXT_PATH
from _targets import SERVER_PORT, SERVER_URL, hello, server_is_up

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_HELLO,
    FT_HELLO_ACK,
    FT_HELLO_PENDING,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_OPEN_PENDING,
    FT_PING,
    N6K_PROTOCOL_VERSION,
    OPEN_PENDING_INTERVAL_MS,
    OPEN_PENDING_STALL_MS,
)

pytestmark = [
    pytest.mark.skipif(not os.path.exists(EXT_PATH), reason=f"Extension not built: {EXT_PATH}"),
    pytest.mark.skipif(not server_is_up(), reason="test server not running on :8099"),
]

# Past the client's connect timeout, so only OPEN_PENDING can keep the ATTACH alive.
SLOW_MS = 7000
N6K_URL = f"n6k://localhost:{SERVER_PORT}"


@pytest.fixture
def con() -> Any:
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.execute(f"LOAD '{EXT_PATH}'")
    yield c
    c.close()


def test_attach_waits_out_a_slow_open_and_then_serves(con: Any) -> None:
    alias = f"slowopen_{SLOW_MS}"
    t0 = time.monotonic()
    con.execute(f"ATTACH '{N6K_URL}' AS {alias} (TYPE n6k)")
    assert time.monotonic() - t0 >= SLOW_MS / 1000
    rows = con.execute(f"SELECT count(*) FROM {alias}.main.users").fetchone()
    assert rows[0] >= 3


@pytest.mark.parametrize("delay_ms", [1500, SLOW_MS])
def test_a_failed_slow_open_fails_the_attach_with_the_backend_reason(con: Any, delay_ms: int) -> None:
    with pytest.raises(duckdb.Error, match="slowopen backend failed"):
        con.execute(f"ATTACH '{N6K_URL}' AS slowopen_fail_{delay_ms} (TYPE n6k)")


@pytest.mark.asyncio
async def test_open_pending_repeats_at_the_advertised_cadence_until_ack() -> None:
    catalog = "slowopen_5000"
    url = SERVER_URL.replace("http://", "ws://") + f"/ws?catalog={catalog}"
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        await ws.send(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": catalog}))
        t0 = time.monotonic()
        beats: list[float] = []
        while True:
            header, _ = unpack_frame(await ws.recv())
            if header["t"] == FT_PING:
                continue
            if header["t"] != FT_OPEN_PENDING:
                break
            assert header["ns"] == 1
            assert header["message"] == f"starting {catalog}"
            beats.append(time.monotonic() - t0)
        assert header["t"] == FT_OPEN_ACK
        assert beats[0] < 1.0
        assert len(beats) >= 3
        gaps = [b - a for a, b in zip(beats, beats[1:])]
        assert all(0.75 * OPEN_PENDING_INTERVAL_MS / 1000 <= g < OPEN_PENDING_STALL_MS / 1000 for g in gaps), gaps


def test_attach_waits_out_a_slow_authenticate(con: Any) -> None:
    alias = f"slowauth_{SLOW_MS}"
    t0 = time.monotonic()
    con.execute(f"ATTACH '{N6K_URL}' AS {alias} (TYPE n6k)")
    assert time.monotonic() - t0 >= SLOW_MS / 1000
    rows = con.execute(f"SELECT count(*) FROM {alias}.main.users").fetchone()
    assert rows[0] >= 3


@pytest.mark.asyncio
async def test_hello_pending_repeats_at_the_advertised_cadence_until_hello_ack() -> None:
    catalog = "slowauth_6000"
    url = SERVER_URL.replace("http://", "ws://") + f"/ws?catalog={catalog}"
    async with websockets.connect(url) as ws:
        await ws.send(pack_frame({"t": FT_HELLO, "token": "", "protocol_version": N6K_PROTOCOL_VERSION}))
        t0 = time.monotonic()
        beats: list[float] = []
        while True:
            header, _ = unpack_frame(await ws.recv())
            if header["t"] != FT_HELLO_PENDING:
                break
            assert header["message"] == "authenticating"
            beats.append(time.monotonic() - t0)
        assert header["t"] == FT_HELLO_ACK
        assert beats[0] < 5.0
        assert len(beats) >= 2
        gaps = [b - a for a, b in zip(beats, beats[1:])]
        assert all(0.75 * OPEN_PENDING_INTERVAL_MS / 1000 <= g < OPEN_PENDING_STALL_MS / 1000 for g in gaps), gaps
