"""Exercises the msgpack framing on /ws in the test server.

Speaks the wire protocol directly with the `websockets` client library against
the already-running test server on :8099 (root mount, no auth). Each WS
connection gets its own freshly seeded in-memory catalog, so tests stay isolated
without spawning a server. No DuckDB extension required.

Run:
    uv run pytest integration_tests/test_ws_protocol.py -v
"""

import asyncio
import io
import urllib.error
import urllib.request
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import websockets

from _targets import hello, open_session, scan_plan

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (  # noqa: E402
    FT_CANCEL,
    FT_CREDIT,
    FT_ERR,
    FT_HELLO_ACK,
    FT_OPEN_ACK,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
    N6K_PROTOCOL_VERSION,
    OP_CATALOG_LIST,
    OP_SCHEMA_LIST,
    OP_EXEC,
    OP_INSERT,
    OP_QUERY,
    OP_READ_PLAN,
    OP_RPC_SCALAR,
    OP_RPC_TABLE,
    OP_TABLE_SCHEMA,
    OP_TABLES_LIST,
)

# A frame is `(header map, body bytes)` — see n6k_protocol.engine.
Frame = "tuple[dict[str, Any], bytes]"


@pytest.fixture(scope="module")
def server():
    # The always-running test server on :8099 (root mount, no auth).
    port = 8099
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip("Test server not running on port 8099")
    yield port


class V2Client:
    """Thin helper that frames/unframes and tracks req_ids."""

    def __init__(self, ws, ns: int = 1):
        self.ws = ws
        self.ns = ns
        self._next_req = 1

    def next_req(self) -> int:
        r = self._next_req
        self._next_req += 1
        return r

    async def send_req(self, op: int, *, req_id: int | None = None, body: bytes = b"", **args: Any) -> int:
        r = req_id if req_id is not None else self.next_req()
        scope = {} if op == OP_CATALOG_LIST else {"ns": self.ns}
        await self.ws.send(pack_frame({"t": FT_REQ, "id": r, "op": op, **scope, **args}, body))
        return r

    async def send_credit(self, req_id: int, n: int):
        await self.ws.send(pack_frame({"t": FT_CREDIT, "id": req_id, "n": n, "ns": self.ns}))

    async def send_cancel(self, req_id: int):
        await self.ws.send(pack_frame({"t": FT_CANCEL, "id": req_id, "ns": self.ns}))

    async def recv(self) -> tuple[dict[str, Any], bytes]:
        raw = await self.ws.recv()
        if isinstance(raw, str):
            raise AssertionError(f"expected binary frame, got text: {raw!r}")
        header, body = unpack_frame(raw)
        return header, bytes(body)

    async def drain_until_end(self, req_id: int, timeout: float = 5.0) -> list[tuple[dict[str, Any], bytes]]:
        frames: list[tuple[dict[str, Any], bytes]] = []
        async with asyncio.timeout(timeout):
            while True:
                header, body = await self.recv()
                frames.append((header, body))
                # RESP_END / ERR are the terminal frame types for this req.
                if header.get("id") == req_id and header["t"] in (FT_RESP_END, FT_ERR):
                    return frames


async def _connect(port: int) -> tuple[websockets.WebSocketClientProtocol, V2Client, dict]:
    ws = await websockets.connect(f"ws://127.0.0.1:{port}/ws?catalog=db")
    ack = await hello(ws)
    assert ack["t"] == FT_HELLO_ACK, ack
    assert ack["protocol_version"] == N6K_PROTOCOL_VERSION
    opened = await open_session(ws, 1, "db")
    assert opened["t"] == FT_OPEN_ACK, opened
    return ws, V2Client(ws), opened


# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_open_ack(server):
    ws, _, opened = await _connect(server)
    try:
        assert opened["default_batch_credits"] >= 1
        assert opened["max_concurrent_reqs"] >= 1
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_schema_list(server):
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_SCHEMA_LIST)
        frames = await client.drain_until_end(req)
        chunk = [h for h, _ in frames if h["t"] == FT_RESP_CHUNK][0]
        assert "main" in chunk["schemas"]
        assert "test_schema" in chunk["schemas"]
        # Ordering is part of the op's contract — see src/common/n6k_catalog_list.cpp.
        assert chunk["schemas"] == sorted(chunk["schemas"])
        assert any(h["t"] == FT_RESP_END for h, _ in frames)
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_catalog_list(server):
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_CATALOG_LIST)
        frames = await client.drain_until_end(req)
        chunk = [h for h, _ in frames if h["t"] == FT_RESP_CHUNK][0]
        assert isinstance(chunk["catalogs"], list)
        assert len(chunk["catalogs"]) > 0
        assert any(h["t"] == FT_RESP_END for h, _ in frames)
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_tables_list(server):
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_TABLES_LIST)
        frames = await client.drain_until_end(req)
        chunk = [h for h, _ in frames if h["t"] == FT_RESP_CHUNK][0]
        names = {t["name"] for t in chunk["tables"]}
        assert {"users", "products"}.issubset(names)
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_table_schema(server):
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_TABLE_SCHEMA, schema="main", table="users")
        frames = await client.drain_until_end(req)
        schema_frames = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA]
        assert len(schema_frames) == 1  # RESP_SCHEMA always carries the Arrow schema body
        reader = ipc.open_stream(io.BytesIO(schema_frames[0]))
        assert {f.name for f in reader.schema} == {"id", "name", "age"}
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_exec_and_query(server):
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_EXEC, sql="UPDATE db.main.users SET age = age + 1 WHERE id = 1")
        frames = await client.drain_until_end(req)
        end = [h for h, _ in frames if h["t"] == FT_RESP_END][0]
        assert end["rowcount"] >= 0

        req = await client.send_req(OP_QUERY, sql="SELECT id, name FROM db.main.users ORDER BY id", _batch_rows=1)
        frames = await client.drain_until_end(req)
        assert sum(1 for h, _ in frames if h["t"] == FT_RESP_SCHEMA) == 1
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        assert len(chunks) >= 2, "batch_rows=1 with 3 rows should yield ≥2 chunks"
        schema_bytes = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        buf = io.BytesIO(schema_bytes + b"".join(chunks))
        table = ipc.open_stream(buf).read_all()
        assert set(table.column_names) == {"id", "name"}
        assert table.num_rows == 3
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_read_plan_streaming(server):
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(
            OP_READ_PLAN,
            plan=scan_plan("main", "users", ["id", "age"]),
            _batch_rows=1,
        )
        frames = await client.drain_until_end(req)
        assert sum(1 for h, _ in frames if h["t"] == FT_RESP_SCHEMA) == 1
        assert sum(1 for h, _ in frames if h["t"] == FT_RESP_CHUNK) >= 2
        assert sum(1 for h, _ in frames if h["t"] == FT_RESP_END) == 1
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_insert_and_readback(server):
    ws, client, _ = await _connect(server)
    try:
        arrow_table = pa.table(
            {
                "id": pa.array([99], type=pa.int32()),
                "name": pa.array(["Gizmo"], type=pa.utf8()),
                "price": pa.array([1.25], type=pa.float64()),
            }
        )
        sink = io.BytesIO()
        w = ipc.new_stream(sink, arrow_table.schema)
        w.write_table(arrow_table)
        w.close()
        # INSERT carries metadata in the header and the Arrow stream as the body.
        req = await client.send_req(OP_INSERT, body=sink.getvalue(), schema="test_schema", table="products")
        frames = await client.drain_until_end(req)
        end = [h for h, _ in frames if h["t"] == FT_RESP_END][0]
        assert end["rowcount"] == 1

        req = await client.send_req(OP_QUERY, sql="SELECT id FROM db.test_schema.products WHERE id = 99")
        frames = await client.drain_until_end(req)
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        schema_bytes = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        buf = io.BytesIO(schema_bytes + b"".join(chunks))
        table = ipc.open_stream(buf).read_all()
        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [99]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_flow_control_pauses_producer(server):
    """Server must stop sending RESP_CHUNK when credits are exhausted.

    We issue a scan whose batch count exceeds DEFAULT_BATCH_CREDITS (8) and
    refuse to send any CREDIT frames. After a short window, the server
    should have sent exactly default_batch_credits RESP_CHUNK frames and
    nothing further (no RESP_END).
    """
    ws, client, opened = await _connect(server)
    budget = opened["default_batch_credits"]
    try:
        # Enough rows to fill many vectors, so the stream outruns the credit window on the
        # server's own chunk boundaries and still ends — the tail of this test drains to
        # RESP_END.
        #
        # Deliberately NOT `budget + 3` rows with `_batch_rows=1`: that couples the test to
        # the server honouring the hint. What is under test is the credit limit, not how rows
        # are packed, so the pressure comes from real volume instead. `_batch_rows` is covered
        # on its own in the streaming tests above.
        scan_req = await client.send_req(OP_QUERY, sql="SELECT * FROM n6k_testing_stream_counter(40000)")

        got_chunks = 0
        got_schema = False
        async with asyncio.timeout(5):
            while got_chunks < budget or not got_schema:
                header, _ = await client.recv()
                assert header.get("id") == scan_req
                if header["t"] == FT_RESP_SCHEMA:
                    got_schema = True
                elif header["t"] == FT_RESP_CHUNK:
                    got_chunks += 1

        # No more frames should arrive within 0.5s without CREDIT.
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(0.5):
                await client.recv()

        # Now refill credits and expect the stream to finish.
        await client.send_credit(scan_req, budget * 2)
        remaining = await client.drain_until_end(scan_req, timeout=5)
        assert any(h["t"] == FT_RESP_END for h, _ in remaining)
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_cancel_mid_stream(server):
    """CANCEL mid-stream must yield RESP_END{cancelled:true}."""
    ws, client, _hello = await _connect(server)
    try:
        # An endless source, so the stream is certainly still running when CANCEL lands.
        # A small result would finish on its own first and end normally, which is what
        # this test must not mistake for a cancellation.
        scan_req = await client.send_req(OP_QUERY, sql="SELECT * FROM n6k_testing_stream_counter(0)")

        # Drain schema + at least one chunk, then cancel.
        got_chunk = False
        async with asyncio.timeout(5):
            while not got_chunk:
                header, _ = await client.recv()
                assert header.get("id") == scan_req
                if header["t"] == FT_RESP_CHUNK:
                    got_chunk = True

        await client.send_cancel(scan_req)
        # Drain until end; may still see some in-flight chunks first.
        end_header = None
        async with asyncio.timeout(5):
            while end_header is None:
                header, _ = await client.recv()
                if header["t"] in (FT_RESP_END, FT_ERR) and header.get("id") == scan_req:
                    end_header = header
        assert end_header.get("cancelled") is True
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_socket_drop_server_cleans_up(server):
    """Close client mid-scan; server must not hang or leak the task.

    We cannot observe server internals here, but we can reconnect and
    issue another request successfully — if the server were stuck the
    handler pool or event loop would misbehave.
    """
    ws, client, _ = await _connect(server)
    try:
        await client.send_req(OP_QUERY, sql="SELECT id FROM db.main.users ORDER BY id", _batch_rows=1)
        await client.recv()
    finally:
        await ws.close()

    ws2, client2, _ = await _connect(server)
    try:
        req = await client2.send_req(OP_SCHEMA_LIST)
        await client2.drain_until_end(req)
    finally:
        await ws2.close()


# ── Streaming RPC (an RPC whose handler yields batches, incl. unbounded) ──────


@pytest.mark.asyncio
async def test_streaming_rpc_bounded_streams_and_completes(server):
    """A bounded streaming RPC delivers its rows as RESP_CHUNKs and ends normally
    (not cancelled). Proves the RPC op flows through the streaming path."""
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_RPC_SCALAR, function="stream_counter", args=[3])
        frames = await client.drain_until_end(req)

        assert sum(1 for h, _ in frames if h["t"] == FT_RESP_SCHEMA) == 1
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        assert len(chunks) >= 1
        end = [h for h, _ in frames if h["t"] == FT_RESP_END][0]
        assert not end.get("cancelled")

        schema_bytes = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        table = ipc.open_stream(io.BytesIO(schema_bytes + b"".join(chunks))).read_all()
        assert table.column("n").to_pylist() == [0, 1, 2]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_streaming_rpc_unbounded_backpressure_and_cancel(server):
    """An *unbounded* streaming RPC: yields forever, so it delivers exactly the
    credit window then parks on backpressure; granting credit resumes it; CANCEL
    terminates it with RESP_END{cancelled:true}."""
    ws, client, opened = await _connect(server)
    budget = opened["default_batch_credits"]
    try:
        # args=[0] → unbounded counter.
        req = await client.send_req(OP_RPC_SCALAR, function="stream_counter", args=[0])

        header, _ = await client.recv()
        assert header["t"] == FT_RESP_SCHEMA and header.get("id") == req

        # Incremental delivery: an infinite generator produces the full credit
        # window without ever finishing.
        got = 0
        async with asyncio.timeout(5):
            while got < budget:
                header, _ = await client.recv()
                assert header.get("id") == req
                if header["t"] == FT_RESP_CHUNK:
                    got += 1
        assert got == budget

        # Backpressure: with credits exhausted, no further chunk arrives.
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(0.5):
                await client.recv()

        # Granting credit resumes the producer.
        await client.send_credit(req, 3)
        more = 0
        async with asyncio.timeout(5):
            while more < 3:
                header, _ = await client.recv()
                if header["t"] == FT_RESP_CHUNK and header.get("id") == req:
                    more += 1
        assert more == 3

        # Cancel terminates the unbounded stream.
        await client.send_cancel(req)
        end_header = None
        async with asyncio.timeout(5):
            while end_header is None:
                header, _ = await client.recv()
                if header["t"] in (FT_RESP_END, FT_ERR) and header.get("id") == req:
                    end_header = header
        assert end_header["t"] == FT_RESP_END
        assert end_header.get("cancelled") is True
    finally:
        await ws.close()


# ── Host-backed RPC (a Python generator, not a macro) ─────────────────────────
#
# These call the `py_*` targets the test server registers through
# `virtual_catalog.rpc_stream`. The point of each is that no catalog entry exists for
# the name — the reactor resolves it to a generator in the server process.


@pytest.mark.asyncio
async def test_host_rpc_finite_returns_its_table(server):
    """A finite host RPC: one Arrow table, schema first, ends normally."""
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_RPC_SCALAR, function="py_echo", args=["hello"])
        frames = await client.drain_until_end(req)

        end = [h for h, _ in frames if h["t"] == FT_RESP_END][0]
        assert not end.get("cancelled"), f"unexpected end: {end}"
        schema_bytes = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        table = ipc.open_stream(io.BytesIO(schema_bytes + b"".join(chunks))).read_all()
        assert table.column("arg0").to_pylist() == ["hello"]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_host_rpc_streams_one_chunk_per_yield(server):
    """A bounded generator delivers one RESP_CHUNK per yield — the whole result
    is never materialized on the server."""
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_RPC_SCALAR, function="py_ticker", args=[3])
        frames = await client.drain_until_end(req)

        assert sum(1 for h, _ in frames if h["t"] == FT_RESP_SCHEMA) == 1
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        # One per yield, plus the trailing end-of-stream marker.
        assert len(chunks) >= 3
        schema_bytes = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        table = ipc.open_stream(io.BytesIO(schema_bytes + b"".join(chunks))).read_all()
        assert table.column("seq").to_pylist() == [0, 1, 2]
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_host_rpc_pushed_rows_arrive_as_a_table(server):
    """OP_RPC_TABLE: the Arrow body reaches the Python function as a pa.Table,
    without being staged through a temp view first."""
    ws, client, _ = await _connect(server)
    try:
        sink = pa.BufferOutputStream()
        rows = pa.table({"id": pa.array([4, 5, 6], pa.int64())})
        with ipc.new_stream(sink, rows.schema) as writer:
            writer.write_table(rows)

        req = await client.send_req(OP_RPC_TABLE, function="py_sum_table", body=sink.getvalue().to_pybytes())
        frames = await client.drain_until_end(req)

        schema_bytes = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        table = ipc.open_stream(io.BytesIO(schema_bytes + b"".join(chunks))).read_all()
        assert table.column("total").to_pylist() == [15]
    finally:
        await ws.close()


async def _host_rpc_closes(client) -> int:
    """How many times a `py_ticker` generator has run its own teardown."""
    req = await client.send_req(OP_RPC_SCALAR, function="py_ticker_closes")
    frames = await client.drain_until_end(req)
    schema_bytes = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
    chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
    table = ipc.open_stream(io.BytesIO(schema_bytes + b"".join(chunks))).read_all()
    return table.column("closes").to_pylist()[0]


@pytest.mark.asyncio
async def test_host_rpc_unbounded_backpressure_cancel_and_teardown(server):
    """An endless generator parks on credit, resumes when granted, and on CANCEL
    ends with RESP_END{cancelled} *after* its own `finally` has run — which is
    how a real subscription unsubscribes rather than leaking."""
    ws, client, opened = await _connect(server)
    budget = opened["default_batch_credits"]
    try:
        before = await _host_rpc_closes(client)

        # count=0 → unbounded; 20 ms between ticks so delivery is observably paced.
        req = await client.send_req(OP_RPC_SCALAR, function="py_ticker", args=[0, 20])

        header, _ = await client.recv()
        assert header["t"] == FT_RESP_SCHEMA and header.get("id") == req

        got = 0
        async with asyncio.timeout(10):
            while got < budget:
                header, _ = await client.recv()
                assert header.get("id") == req
                if header["t"] == FT_RESP_CHUNK:
                    got += 1
        assert got == budget

        # Credits exhausted: the generator is parked, so nothing more arrives.
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(0.5):
                await client.recv()

        await client.send_credit(req, 2)
        more = 0
        async with asyncio.timeout(10):
            while more < 2:
                header, _ = await client.recv()
                if header["t"] == FT_RESP_CHUNK and header.get("id") == req:
                    more += 1

        await client.send_cancel(req)
        end_header = None
        async with asyncio.timeout(10):
            while end_header is None:
                header, _ = await client.recv()
                if header["t"] in (FT_RESP_END, FT_ERR) and header.get("id") == req:
                    end_header = header
        assert end_header["t"] == FT_RESP_END
        assert end_header.get("cancelled") is True

        # The reactor closes the generator before it sends END, so this needs no wait.
        assert await _host_rpc_closes(client) == before + 1
    finally:
        await ws.close()


@pytest.mark.asyncio
async def test_host_rpc_unknown_name_is_an_error(server):
    """A name that is neither registered nor in the catalog still fails as a
    binder error, not as a hung request."""
    ws, client, _ = await _connect(server)
    try:
        req = await client.send_req(OP_RPC_SCALAR, function="py_not_a_thing")
        frames = await client.drain_until_end(req)
        assert frames[-1][0]["t"] == FT_ERR
    finally:
        await ws.close()
