"""FT_HELLO against the token-protected `/auth` mount on the running :8099 test server.

Run:
    uv run pytest integration_tests/test_ws_handshake_auth.py -v
"""

import asyncio
import urllib.error
import urllib.request

import msgpack
import pytest
import websockets

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    APP_HELLO_CAPS,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_REQ,
    HELLO_CAPS,
    N6K_PROTOCOL_VERSION,
    OP_SCHEMA_LIST,
    OPEN_CAPS,
)

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
AUTH_TOKEN = "n6k-test-token"
WS_URL = f"ws://127.0.0.1:{SERVER_PORT}/auth/ws?catalog=db"


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip("Test server not running on port 8099")


async def _recv(ws) -> dict:
    raw = await ws.recv()
    assert not isinstance(raw, str), f"expected binary frame, got text: {raw!r}"
    header, _ = unpack_frame(raw)
    return header


async def _expect_hello_err_then_close(ws) -> dict:
    header = await _recv(ws)
    assert header["t"] == FT_HELLO_ERR, header
    assert "ns" not in header
    assert header["exception_message"]
    with pytest.raises(websockets.exceptions.ConnectionClosed):
        await ws.recv()
    return header


@pytest.mark.asyncio
async def test_valid_token_gets_hello_ack_then_open_ack():
    async with websockets.connect(WS_URL) as ws:
        await ws.send(pack_frame({"t": FT_HELLO, "token": AUTH_TOKEN, "protocol_version": N6K_PROTOCOL_VERSION}))
        ack = await _recv(ws)
        assert ack == {"t": FT_HELLO_ACK, "protocol_version": N6K_PROTOCOL_VERSION, "capabilities": []}
        await ws.send(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "db"}))
        opened = await _recv(ws)
        assert opened["t"] == FT_OPEN_ACK
        assert opened["ns"] == 1
        assert opened["capabilities"] == []


@pytest.mark.asyncio
async def test_hello_capabilities_are_the_intersection():
    async with websockets.connect(WS_URL) as ws:
        hello = {
            "t": FT_HELLO,
            "token": AUTH_TOKEN,
            "protocol_version": N6K_PROTOCOL_VERSION,
            "capabilities": ["foo", *HELLO_CAPS],
        }
        await ws.send(pack_frame(hello))
        ack = await _recv(ws)
        assert ack["t"] == FT_HELLO_ACK
        assert ack["capabilities"] == [c for c in HELLO_CAPS if c not in APP_HELLO_CAPS]


@pytest.mark.asyncio
async def test_open_capabilities_are_the_intersection():
    async with websockets.connect(WS_URL) as ws:
        await ws.send(pack_frame({"t": FT_HELLO, "token": AUTH_TOKEN, "protocol_version": N6K_PROTOCOL_VERSION}))
        assert (await _recv(ws))["t"] == FT_HELLO_ACK
        await ws.send(pack_frame({"t": FT_OPEN, "ns": 1, "catalog": "db", "capabilities": ["foo", *OPEN_CAPS]}))
        opened = await _recv(ws)
        assert opened["t"] == FT_OPEN_ACK
        assert opened["capabilities"] == list(OPEN_CAPS)


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [N6K_PROTOCOL_VERSION - 1, None], ids=["older", "missing"])
async def test_mismatched_protocol_version_gets_hello_err_then_close(version):
    hello = {"t": FT_HELLO, "token": AUTH_TOKEN}
    if version is not None:
        hello["protocol_version"] = version
    async with websockets.connect(WS_URL) as ws:
        # Raw msgpack: pack_frame would refuse the header this test sends.
        await ws.send(msgpack.packb(hello, use_bin_type=True))
        err = await _expect_hello_err_then_close(ws)
        assert "protocol_version" in err["exception_message"]


@pytest.mark.asyncio
async def test_bad_token_gets_hello_err_then_close():
    for _ in range(50):
        async with websockets.connect(WS_URL) as ws:
            await ws.send(pack_frame({"t": FT_HELLO, "token": "wrong-token", "protocol_version": N6K_PROTOCOL_VERSION}))
            err = await _expect_hello_err_then_close(ws)
            assert "unauthorized" in err["exception_message"]


@pytest.mark.asyncio
async def test_non_hello_first_frame_gets_hello_err_then_close():
    async with websockets.connect(WS_URL) as ws:
        await ws.send(pack_frame({"t": FT_REQ, "id": 1, "op": OP_SCHEMA_LIST, "ns": 1}))
        err = await _expect_hello_err_then_close(ws)
        assert "first frame must be FT_HELLO" in err["exception_message"]


@pytest.mark.asyncio
async def test_silence_gets_hello_err_then_close():
    async with websockets.connect(WS_URL) as ws:
        err = await asyncio.wait_for(_expect_hello_err_then_close(ws), timeout=10)
        assert "no FT_HELLO" in err["exception_message"]


@pytest.mark.asyncio
async def test_second_hello_gets_hello_err_then_close():
    async with websockets.connect(WS_URL) as ws:
        await ws.send(pack_frame({"t": FT_HELLO, "token": AUTH_TOKEN, "protocol_version": N6K_PROTOCOL_VERSION}))
        assert (await _recv(ws))["t"] == FT_HELLO_ACK
        await ws.send(pack_frame({"t": FT_HELLO, "token": AUTH_TOKEN, "protocol_version": N6K_PROTOCOL_VERSION}))
        err = await _expect_hello_err_then_close(ws)
        assert "already received" in err["exception_message"]
