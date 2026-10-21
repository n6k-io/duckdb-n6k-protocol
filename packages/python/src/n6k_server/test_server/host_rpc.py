"""Host-backed RPC targets for the test server.

These are Python functions, not macros: they exercise the path in
`n6k_server.rpc_stream` where an RPC name resolves to something living in this
process rather than in the served DuckDB. `py_ticker` is the case that has no
catalog form at all — it paces itself in wall-clock time and, given a count of
zero, never ends.
"""

import asyncio
from typing import Any, AsyncIterator, List, Optional

import duckdb
import pyarrow as pa

from n6k_server.rpc_stream import HostStreamUdfs, RpcRegistry, StreamingRpc, register_rpc_streams

ECHO_SCHEMA = pa.schema([("arg0", pa.string())])
TICK_SCHEMA = pa.schema([("seq", pa.int64())])
TOTAL_SCHEMA = pa.schema([("total", pa.int64())])
CLOSES_SCHEMA = pa.schema([("closes", pa.int64())])

# How many times a `py_ticker` generator has run its own teardown. Process-global on purpose: a
# test cancels a stream on one connection and reads the count from another, which is the only way
# to observe that cancelling actually reached the generator instead of just dropping its output.
_ticker_closes = 0


def _py_echo(scalars: List[Any], table: Optional[pa.Table]) -> pa.Table:
    """Finite host RPC: one table, sent and ended."""
    return pa.table({"arg0": [str(scalars[0]) if scalars else ""]}, schema=ECHO_SCHEMA)


def _py_sum_table(scalars: List[Any], table: Optional[pa.Table]) -> pa.Table:
    """Finite host RPC over pushed rows — the OP_RPC_TABLE body arrives as a `pa.Table`."""
    total = 0
    if table is not None and table.num_columns:
        total = sum(v for v in table.column(0).to_pylist() if v is not None)
    return pa.table({"total": [int(total)]}, schema=TOTAL_SCHEMA)


def _py_ticker_closes(scalars: List[Any], table: Optional[pa.Table]) -> pa.Table:
    return pa.table({"closes": [_ticker_closes]}, schema=CLOSES_SCHEMA)


async def _py_ticker(scalars: List[Any], table: Optional[pa.Table]) -> AsyncIterator[pa.RecordBatch]:
    """Streaming host RPC: `py_ticker(count[, interval_ms])`, unbounded when count <= 0.

    One row per yield, so each interval produces one observable chunk — the same
    shape as the native `slow_ticker`, but driven by an asyncio sleep in this
    process.
    """
    global _ticker_closes
    count = int(scalars[0]) if scalars else 0
    interval_s = (float(scalars[1]) / 1000.0) if len(scalars) > 1 and scalars[1] is not None else 0.0
    seq = 0
    try:
        while count <= 0 or seq < count:
            yield pa.record_batch({"seq": [seq]}, schema=TICK_SCHEMA)
            seq += 1
            if interval_s > 0:
                await asyncio.sleep(interval_s)
    finally:
        _ticker_closes += 1


HOST_RPCS: RpcRegistry = {
    "py_echo": (_py_echo, ECHO_SCHEMA),
    "py_sum_table": (_py_sum_table, TOTAL_SCHEMA),
    "py_ticker": (StreamingRpc(_py_ticker), TICK_SCHEMA),
    "py_ticker_closes": (_py_ticker_closes, CLOSES_SCHEMA),
}


def register_host_rpcs(
    con: duckdb.DuckDBPyConnection, catalog: str, main_loop: Optional[asyncio.AbstractEventLoop] = None
) -> HostStreamUdfs:
    """Bind `HOST_RPCS` to `catalog` on `con`. Call before serving `con`."""
    return register_rpc_streams(con, catalog=catalog, registry=HOST_RPCS, main_loop=main_loop)
