"""Host-backed RPC: serve `@rpc`-style Python functions, including generators.

The C++ server resolves an RPC name against the served DuckDB, so an RPC has to
be a catalog entry. A Python generator cannot be a table macro, and DuckDB's
Python API cannot register a table function at all — `create_function` is
scalar-only.

So this module registers three scalar UDFs (open/next/close, speaking Arrow IPC)
and asks the extension to build a table function over them, via
`vcatprovider_create_stream_function`. The result is an ordinary catalog entry:
the reactor resolves it like any other table function and drives the generator one
Arrow batch per `RESP_CHUNK`, paced by the client's credit and closed on cancel,
and `SELECT * FROM <catalog>.main.<name>(...)` works locally with no server.

## Registering

```python
import pyarrow as pa
from n6k_server.rpc_stream import StreamingRpc, register_rpc_streams

TICK = pa.schema([("seq", pa.int64()), ("value", pa.float64())])


async def random_walk(scalars, table):
    value = 100.0
    for seq in range(scalars[0]):
        value += random.uniform(-1.0, 1.0)
        yield pa.record_batch({"seq": [seq], "value": [value]}, schema=TICK)
        await asyncio.sleep(0.1)


def connect(ws):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_vcatprovider(con)
    con.execute("ATTACH ':memory:' AS db")
    register_rpc_streams(con, catalog="db", registry={"random_walk": (StreamingRpc(random_walk), TICK)})
    return con
```

`registry` is `{name: (callable, pa.Schema)}`. The callable takes
`(scalars, table)` — the RPC's positional arguments, and the rows a
`RPC_TABLE` call pushed (`None` for `RPC_SCALAR`) — and is either:

- wrapped in `StreamingRpc`, returning an async iterator of `pa.RecordBatch`
  (or `pa.Table`), which may never end; or
- a plain callable returning one `pa.Table`, which is sent and ended.

The schema is declared, not inferred, because the wire sends `RESP_SCHEMA`
before the first batch — and a generator that has not yielded yet cannot say
what its columns are.

## Where the code runs

Generators are driven on `main_loop` (the loop running at registration), so
they see the same per-connection state as everything else the host serves —
one RPC can change what another is streaming mid-flight. Each batch costs one
thread hop from a DuckDB worker into that loop, so yield **batches**, not rows.

A finite callable runs directly on the worker thread, which is already off the
loop.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import struct
import uuid
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, Tuple, Union

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
from duckdb.func import FunctionNullHandling

from n6k_server.extension import load_vcatprovider

log = logging.getLogger(__name__)

_VARCHAR = duckdb.sqltype("VARCHAR")
_BLOB = duckdb.sqltype("BLOB")
_SPECIAL = FunctionNullHandling.SPECIAL

# Where the created table functions land. The wire carries only a function name, never a schema, so
# the C++ side resolves RPC in `main` (request_handlers.cpp RPC_SCHEMA) — this must match it.
RPC_SCHEMA = "main"

# A finite RPC: (scalars, table) -> one fully materialized pa.Table.
FiniteRpcFn = Callable[[List[Any], Optional[pa.Table]], pa.Table]
# A streaming RPC: (scalars, table) -> an async iterator of record batches, possibly unbounded.
StreamingRpcFn = Callable[[List[Any], Optional[pa.Table]], AsyncIterator[Any]]


@dataclass(frozen=True)
class StreamingRpc:
    """Registry marker wrapping a streaming RPC callable.

    Wrapping the callable rather than sniffing its return type keeps the two
    cases unambiguous at dispatch: a function that returns an async iterator
    and one that returns a table are indistinguishable until called, and
    calling a subscription to find out is not an option.
    """

    fn: StreamingRpcFn


RpcEntryFn = Union[FiniteRpcFn, StreamingRpc]
RpcRegistry = Dict[str, Tuple[RpcEntryFn, pa.Schema]]


def _drop_leading_message(data: bytes) -> bytes:
    """Strip one encapsulated IPC message from the front of `data`.

    Framing is: the 0xFFFFFFFF continuation marker, a little-endian metadata
    length, then that many (already padded) metadata bytes. A schema message
    carries no body buffers, so that is the whole message.
    """
    if len(data) < 8 or data[:4] != b"\xff\xff\xff\xff":
        raise ValueError("expected an encapsulated Arrow IPC schema message at the start of the stream")
    (metadata_len,) = struct.unpack("<I", data[4:8])
    return data[8 + metadata_len :]


class ArrowFrameEncoder:
    """A pyarrow IPC stream writer that hands back one IPC message per call.

    The response is a single Arrow stream split across frames — schema message
    first, then one record-batch message per chunk — so the batches must come
    from one writer, not from one stream per batch.
    """

    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema
        self._sink = io.BytesIO()
        self._writer = ipc.new_stream(self._sink, schema)
        self._cursor = self._sink.tell()
        self._closed = False
        self._drop_schema = False

    def _take_new_bytes(self) -> bytes:
        data = self._sink.getvalue()[self._cursor :]
        self._cursor = self._sink.tell()
        if self._drop_schema and data:
            self._drop_schema = False
            data = _drop_leading_message(data)
        return data

    def take_schema_message(self) -> bytes:
        data = self._take_new_bytes()
        if data:
            return data
        # pyarrow buffers the schema until the first write (it wrote it eagerly in older
        # versions, which the branch above still covers). The schema message has to go out
        # first regardless — it is a frame of its own — so take it from the schema and drop
        # the writer's identical copy when it finally appears.
        self._drop_schema = True
        return bytes(self._schema.serialize().to_pybytes())

    def encode_batch(self, batch: pa.RecordBatch) -> bytes:
        self._writer.write_batch(batch)
        return self._take_new_bytes()

    def close_and_take_trailing_bytes(self) -> bytes:
        if not self._closed:
            self._closed = True
            self._writer.close()
        return self._take_new_bytes()


class _Done:
    """Sentinel for an exhausted async iterator.

    `StopAsyncIteration` is not raised across `run_coroutine_threadsafe`
    unchanged in every Python — asyncio gives the Stop* exceptions special
    treatment when they cross a future — so exhaustion is reported as a value.
    """


_DONE = _Done()


async def _anext_or_done(iterator: AsyncIterator[Any]) -> Any:
    try:
        return await iterator.__anext__()
    except StopAsyncIteration:
        return _DONE


def _run_async(coro: Any, loop: asyncio.AbstractEventLoop) -> Any:
    """Bridge an async call to a sync context. Called from DuckDB worker
    threads — never from the event-loop thread (would deadlock)."""
    return asyncio.run_coroutine_threadsafe(coro, loop).result()


def _as_batches(value: Any) -> List[pa.RecordBatch]:
    """One yielded value as record batches. A table becomes its batches; an
    empty one becomes none at all, which is not the end of the stream."""
    if isinstance(value, pa.RecordBatch):
        return [value]
    if isinstance(value, pa.Table):
        return list(value.to_batches())
    raise TypeError(f"streaming RPC must yield pa.RecordBatch or pa.Table, got {type(value).__name__}")


class _LiveStream:
    """One in-flight call: where its batches come from, and the encoder framing them."""

    def __init__(
        self,
        encoder: ArrowFrameEncoder,
        loop: asyncio.AbstractEventLoop,
        *,
        aiterator: Optional[AsyncIterator[Any]] = None,
        batches: Optional[Iterator[pa.RecordBatch]] = None,
    ) -> None:
        self.encoder = encoder
        self._loop = loop
        self._aiterator = aiterator
        self._batches = batches
        self._pending: List[pa.RecordBatch] = []
        # Holds the teardown task when close() runs on the loop thread, so it is not collected
        # mid-flight.
        self._task: Optional["asyncio.Task[None]"] = None

    def next_batch(self) -> Optional[pa.RecordBatch]:
        """The next batch, or None once the source is exhausted."""
        while True:
            if self._pending:
                return self._pending.pop(0)
            if self._batches is not None:
                return next(self._batches, None)
            assert self._aiterator is not None
            value = _run_async(_anext_or_done(self._aiterator), self._loop)
            if value is _DONE:
                return None
            # A yield that carried no rows is not the end: pull again rather than reporting
            # exhaustion, which would truncate the stream at the first empty tick.
            self._pending = _as_batches(value)

    def close(self) -> None:
        """Run the source's own teardown. A generator's `finally` unsubscribes
        here, on cancel or error, rather than whenever GC gets to it."""
        if self._aiterator is None:
            return
        aclose = getattr(self._aiterator, "aclose", None)
        if aclose is None:
            return
        try:
            running: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self._loop:
            # Called from the loop itself — connection teardown, not the reactor. Waiting on a
            # future only this loop can complete would deadlock, so hand it back to the loop.
            self._task = self._loop.create_task(aclose())
            return
        _run_async(aclose(), self._loop)


class HostStreamUdfs:
    """A connection's open/next/close UDFs and the catalog entries built on them.

    Returned by `register_rpc_streams`; the caller owns it and calls
    `unregister_and_close_streams()`.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, udf_suffix: str, loop: asyncio.AbstractEventLoop) -> None:
        self.con = con
        self.loop = loop
        self.registry: RpcRegistry = {}
        # (catalog, name) per created table function, so teardown can drop exactly those.
        self.created: List[Tuple[str, str]] = []
        # create_function registers into the DATABASE catalog, not the connection, and a duplicate
        # name raises. Two connections sharing a DatabaseInstance would collide on a fixed name, so
        # the names carry a per-`register_rpc_streams` suffix. It is only uniqueness -- nothing looks it up.
        self.udfs = (
            f"__n6k_rpc_open_{udf_suffix}",
            f"__n6k_rpc_next_{udf_suffix}",
            f"__n6k_rpc_close_{udf_suffix}",
        )
        self._live: Dict[str, _LiveStream] = {}

    # ── the three UDFs the reactor calls ─────────────────────────────────────

    def _open(self, handle: str, function: str, args_json: str, input_ipc: Optional[bytes]) -> bytes:
        entry = self.registry.get(function)
        if entry is None:
            raise KeyError(f"no host RPC named {function!r} is registered on this connection")
        fn, schema = entry
        scalars = json.loads(args_json) if args_json else []
        table = ipc.open_stream(input_ipc).read_all() if input_ipc else None

        encoder = ArrowFrameEncoder(schema)
        if isinstance(fn, StreamingRpc):
            source = fn.fn(scalars, table)
            iterator = source.__aiter__() if hasattr(source, "__aiter__") else source
            self._live[handle] = _LiveStream(encoder, self.loop, aiterator=iterator)
        else:
            result = fn(scalars, table)
            if not isinstance(result, pa.Table):
                raise TypeError(f"RPC {function!r} must return pa.Table, got {type(result).__name__}")
            self._live[handle] = _LiveStream(encoder, self.loop, batches=iter(result.to_batches()))
        return encoder.take_schema_message()

    def _next(self, handle: str) -> Optional[bytes]:
        stream = self._live.get(handle)
        if stream is None:
            raise KeyError(f"RPC stream {handle!r} is not open")
        batch = stream.next_batch()
        # NULL is the only end-of-stream signal the reactor gets.
        return None if batch is None else stream.encoder.encode_batch(batch)

    def _close(self, handle: str) -> bytes:
        stream = self._live.pop(handle, None)
        if stream is None:
            raise KeyError(f"RPC stream {handle!r} is not open")
        try:
            stream.close()
        finally:
            trailing = stream.encoder.close_and_take_trailing_bytes()
        return trailing

    # ── lifecycle ────────────────────────────────────────────────────────────

    def create_functions(self) -> None:
        open_udf, next_udf, close_udf = self.udfs
        # SPECIAL null handling on all three: `open` takes a NULL body for a scalar call, and `next`
        # answers NULL at end of stream. Under DEFAULT, DuckDB filters NULL-carrying rows out of the
        # input and rejects a NULL return outright, so neither would work.
        self.con.create_function(
            open_udf,
            self._open,
            [_VARCHAR, _VARCHAR, _VARCHAR, _BLOB],
            _BLOB,
            null_handling=_SPECIAL,
            side_effects=True,
        )
        self.con.create_function(next_udf, self._next, [_VARCHAR], _BLOB, null_handling=_SPECIAL, side_effects=True)
        self.con.create_function(close_udf, self._close, [_VARCHAR], _BLOB, null_handling=_SPECIAL, side_effects=True)

    def unregister_and_close_streams(self) -> None:
        """Drop the catalog entries and UDFs this registration created, and tear down
        any generator still open on them.

        Idempotent, and optional: closing the connection drops them anyway, and the
        reactor closes each generator itself (RpcStreamCloser runs from its destructor).
        Use it to un-register without closing the connection.
        """
        if self.created:
            # Same reason as the create side: committed explicitly or the drop is discarded.
            self.con.begin()
            try:
                for catalog, name in self.created:
                    self.con.execute("SELECT vcatprovider_drop_stream_function(?, ?, ?)", [catalog, RPC_SCHEMA, name])
            except Exception:
                self.con.rollback()
                raise
            self.con.commit()
        self.created = []
        for name in self.udfs:
            try:
                self.con.remove_function(name)
            except duckdb.InvalidInputException:
                pass  # already gone
        # A stream can still be open here: shutting the reactor down interrupts its workers, so a
        # worker mid-stream may never reach its own close. Dropping these without tearing them
        # down would strand whatever the generator holds.
        for handle, stream in list(self._live.items()):
            try:
                stream.close()
            except Exception:  # noqa: BLE001 - one stuck generator must not block the rest
                log.exception("host RPC stream %s failed to close", handle)
        self.registry = {}
        self._live.clear()


def register_rpc_streams(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
    registry: RpcRegistry,
    main_loop: Optional[asyncio.AbstractEventLoop] = None,
) -> HostStreamUdfs:
    """Register `registry`'s functions as table functions in `catalog`.main on `con`.

    Each becomes an ordinary catalog entry, so `SELECT * FROM <catalog>.main.<name>(...)`
    works on this connection and the reactor resolves it the same way it resolves any
    other table function. Nothing needs to be tracked afterwards; the returned handle is
    only for a host that wants to un-register without closing the connection.

    Generators run on `main_loop` (default: the loop running at call time).
    That loop must own any loop-bound resource they touch, and must not make
    synchronous DuckDB calls against `con` while it is served.
    """
    if main_loop is None:
        main_loop = asyncio.get_running_loop()

    load_vcatprovider(con)
    handle = HostStreamUdfs(con, uuid.uuid4().hex, main_loop)
    handle.create_functions()

    open_udf, next_udf, close_udf = handle.udfs
    # Explicit transaction: the catalog entry is created from inside a scalar function, and DuckDB
    # plans a SELECT as read-only, so under auto-commit the new entry is discarded when the
    # statement ends. Committing it ourselves is what makes it stick.
    con.begin()
    try:
        for name, entry in registry.items():
            fn, schema = entry
            if not isinstance(schema, pa.Schema):
                raise TypeError(f"RPC {name!r} needs a pa.Schema, got {type(schema).__name__}")
            handle.registry[name] = (fn, schema)
            con.execute(
                "SELECT vcatprovider_create_stream_function(?, ?, ?, ?, ?, ?)",
                [catalog, RPC_SCHEMA, name, open_udf, next_udf, close_udf],
            )
            handle.created.append((catalog, name))
    except Exception:
        con.rollback()
        raise
    con.commit()
    return handle
