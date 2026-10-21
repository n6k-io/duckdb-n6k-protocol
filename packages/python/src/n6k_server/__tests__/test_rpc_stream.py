"""Tests for host-backed RPC (`n6k_server.rpc_stream`).

Most of these drive the three UDFs from SQL exactly as the reactor does —
`open` → `next`* → `close` — because that call sequence *is* the contract
between the extension and this process, and running it here pins it without a
socket. The registration half needs the `virtual_catalog_provider` extension and is marked.

Queries run through `asyncio.to_thread`, matching the reactor: the generator is
driven on the event loop from a DuckDB worker thread, and calling it from the
loop thread would deadlock.
"""

import asyncio
from typing import Any, AsyncIterator, List, Optional

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from n6k_server.extension import local_extension_path
from n6k_server.rpc_stream import (
    ArrowFrameEncoder,
    HostStreamUdfs,
    RpcRegistry,
    StreamingRpc,
    register_rpc_streams,
)

CONN_CONFIG: dict[str, Any] = {"allow_unsigned_extensions": "true"}
TICK = pa.schema([("seq", pa.int64())])


def _scope(registry: RpcRegistry, loop: asyncio.AbstractEventLoop) -> tuple[duckdb.DuckDBPyConnection, HostStreamUdfs]:
    """A scope with its UDFs live, but no extension and no registration.

    Built directly rather than through `register_rpc_streams` so the driving
    sequence can be tested against a plain DuckDB.
    """
    con = duckdb.connect(config=CONN_CONFIG)
    scope = HostStreamUdfs(con, "unittest", loop)
    scope.registry = registry
    scope.create_functions()
    return con, scope


def _drive(
    con: duckdb.DuckDBPyConnection,
    scope: HostStreamUdfs,
    function: str,
    args_json: str = "[]",
    body: Optional[bytes] = None,
    stop_after: Optional[int] = None,
) -> tuple[list[bytes], int]:
    """Run the reactor's loop against `function`; returns (ipc messages, chunks).

    `stop_after` cuts the stream short the way a CANCEL does — close is still
    called, which is the point of stopping early.
    """

    def one(sql: str, params: list[Any]) -> Any:
        row = con.execute(sql, params).fetchone()
        assert row is not None
        return row[0]

    open_udf, next_udf, close_udf = scope.udfs
    messages = [one(f"SELECT {open_udf}(?, ?, ?, ?)", ["h1", function, args_json, body])]
    chunks = 0
    while stop_after is None or chunks < stop_after:
        batch = one(f"SELECT {next_udf}(?)", ["h1"])
        if batch is None:
            break
        messages.append(batch)
        chunks += 1
    trailing = one(f"SELECT {close_udf}(?)", ["h1"])
    if trailing:
        messages.append(trailing)
    return messages, chunks


def _read(messages: List[bytes]) -> pa.Table:
    return ipc.open_stream(b"".join(messages)).read_all()


def test_arrow_frame_encoder_splits_one_stream_across_messages():
    enc = ArrowFrameEncoder(TICK)
    parts = [enc.take_schema_message()]
    parts.append(enc.encode_batch(pa.record_batch({"seq": [1]}, schema=TICK)))
    parts.append(enc.encode_batch(pa.record_batch({"seq": [2]}, schema=TICK)))
    parts.append(enc.close_and_take_trailing_bytes())
    # Every part is its own IPC message, and concatenated they are one stream.
    assert all(parts[:3])
    assert _read(parts).column("seq").to_pylist() == [1, 2]


@pytest.mark.asyncio
async def test_finite_rpc_sends_a_table_and_ends():
    def echo(scalars: List[Any], table: Optional[pa.Table]) -> pa.Table:
        return pa.table({"seq": [int(scalars[0])]}, schema=TICK)

    con, scope = _scope({"echo": (echo, TICK)}, asyncio.get_running_loop())
    messages, chunks = await asyncio.to_thread(_drive, con, scope, "echo", "[7]")
    assert chunks == 1
    assert _read(messages).column("seq").to_pylist() == [7]


@pytest.mark.asyncio
async def test_streaming_rpc_yields_one_chunk_per_yield():
    async def ticker(scalars: List[Any], table: Optional[pa.Table]) -> AsyncIterator[pa.RecordBatch]:
        for seq in range(int(scalars[0])):
            yield pa.record_batch({"seq": [seq]}, schema=TICK)

    con, scope = _scope({"ticker": (StreamingRpc(ticker), TICK)}, asyncio.get_running_loop())
    messages, chunks = await asyncio.to_thread(_drive, con, scope, "ticker", "[3]")
    assert chunks == 3
    assert _read(messages).column("seq").to_pylist() == [0, 1, 2]


@pytest.mark.asyncio
async def test_empty_yield_is_not_end_of_stream():
    """A yield carrying no rows must not truncate the stream — the generator has
    not finished, it just had nothing this tick."""

    async def sparse(scalars: List[Any], table: Optional[pa.Table]) -> AsyncIterator[pa.Table]:
        yield pa.table({"seq": []}, schema=TICK)
        yield pa.record_batch({"seq": [42]}, schema=TICK)

    con, scope = _scope({"sparse": (StreamingRpc(sparse), TICK)}, asyncio.get_running_loop())
    messages, chunks = await asyncio.to_thread(_drive, con, scope, "sparse")
    assert chunks == 1
    assert _read(messages).column("seq").to_pylist() == [42]


@pytest.mark.asyncio
async def test_unbounded_stream_stops_at_close_and_runs_teardown():
    """Closing an endless generator early runs its `finally` — this is what a
    CANCEL does, and it is how a real subscription unsubscribes."""
    closed = asyncio.Event()

    async def forever(scalars: List[Any], table: Optional[pa.Table]) -> AsyncIterator[pa.RecordBatch]:
        seq = 0
        try:
            while True:
                yield pa.record_batch({"seq": [seq]}, schema=TICK)
                seq += 1
        finally:
            closed.set()

    con, scope = _scope({"forever": (StreamingRpc(forever), TICK)}, asyncio.get_running_loop())
    messages, chunks = await asyncio.to_thread(_drive, con, scope, "forever", stop_after=4)
    assert chunks == 4
    assert closed.is_set()
    assert _read(messages).column("seq").to_pylist() == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_pushed_rows_arrive_as_a_table():
    """The OP_RPC_TABLE body reaches the function as `table`, untouched."""

    def total(scalars: List[Any], table: Optional[pa.Table]) -> pa.Table:
        assert table is not None
        return pa.table({"seq": [sum(table.column(0).to_pylist())]}, schema=TICK)

    body = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, body.schema) as writer:
        writer.write_table(body)

    con, scope = _scope({"total": (total, TICK)}, asyncio.get_running_loop())
    messages, _ = await asyncio.to_thread(_drive, con, scope, "total", "[]", sink.getvalue().to_pybytes())
    assert _read(messages).column("seq").to_pylist() == [6]


@pytest.mark.asyncio
async def test_generator_error_surfaces_as_a_query_error():
    async def boom(scalars: List[Any], table: Optional[pa.Table]) -> AsyncIterator[pa.RecordBatch]:
        yield pa.record_batch({"seq": [0]}, schema=TICK)
        raise ValueError("upstream is down")

    con, scope = _scope({"boom": (StreamingRpc(boom), TICK)}, asyncio.get_running_loop())
    with pytest.raises(duckdb.Error, match="upstream is down"):
        await asyncio.to_thread(_drive, con, scope, "boom")


@pytest.mark.asyncio
async def test_unknown_function_is_an_error():
    con, scope = _scope({}, asyncio.get_running_loop())
    with pytest.raises(duckdb.Error, match="no host RPC named"):
        await asyncio.to_thread(_drive, con, scope, "nope")


@pytest.mark.asyncio
async def test_register_rpc_streams_creates_a_callable_table_function():
    """The point of the catalog form: a host RPC is ordinary SQL on the connection
    that registered it, with no server and no reactor in the picture."""
    from n6k_server.extension import VIRTUAL_CATALOG_PROVIDER, load_virtual_catalog_provider

    def echo(scalars: List[Any], table: Optional[pa.Table]) -> pa.Table:
        return pa.table({"seq": [7, 8]}, schema=TICK)

    if local_extension_path(VIRTUAL_CATALOG_PROVIDER) is None:
        pytest.skip("virtual_catalog_provider extension not built")
    con = duckdb.connect(config=CONN_CONFIG)
    load_virtual_catalog_provider(con)
    con.execute("ATTACH ':memory:' AS db")
    handle = await asyncio.to_thread(
        register_rpc_streams,
        con,
        catalog="db",
        registry={"echo": (echo, TICK)},
        main_loop=asyncio.get_running_loop(),
    )

    # The generator runs on this loop, so the query has to be driven off it.
    rows = await asyncio.to_thread(lambda: con.execute("SELECT * FROM db.main.echo()").fetchall())
    assert rows == [(7,), (8,)]

    await asyncio.to_thread(handle.unregister_and_close_streams)
    with pytest.raises(duckdb.CatalogException):
        con.execute("SELECT * FROM db.main.echo()").fetchall()
    await asyncio.to_thread(handle.unregister_and_close_streams)  # idempotent


@pytest.mark.asyncio
async def test_unregister_tears_down_a_still_open_stream():
    """Shutting the reactor down interrupts its workers, so a stream can outlive
    the request that opened it. Scope teardown must still close it — from the
    loop thread, where blocking on the generator would deadlock."""
    closed = asyncio.Event()

    async def forever(scalars: List[Any], table: Optional[pa.Table]) -> AsyncIterator[pa.RecordBatch]:
        try:
            while True:
                yield pa.record_batch({"seq": [0]}, schema=TICK)
        finally:
            closed.set()

    con, scope = _scope({"forever": (StreamingRpc(forever), TICK)}, asyncio.get_running_loop())
    open_udf = scope.udfs[0]
    await asyncio.to_thread(con.execute, f"SELECT {open_udf}(?, ?, ?, ?)", ["h1", "forever", "[]", None])
    await asyncio.to_thread(con.execute, f"SELECT {scope.udfs[1]}(?)", ["h1"])

    # No extension loaded here, so there is nothing to drop; exercise the teardown directly.
    for stream in list(scope._live.values()):
        stream.close()
    # Scheduled on this loop rather than run inline, so it lands on the next turn.
    await asyncio.sleep(0)
    assert closed.is_set()
