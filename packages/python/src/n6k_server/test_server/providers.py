"""Provider fixtures served by the test server's `slowdb` catalog."""

import asyncio
from typing import Optional

import pyarrow as pa

from n6k_protocol.filters import Filters
from n6k_server.provider import Provider, TableNotFound

# Catalog name that routes to the providers below. A client that attaches
# `n6k://host` AS slowdb (or with `catalog 'slowdb'`) lands on them.
SLOW_CATALOG = "slowdb"
SLOW_SCAN_SECONDS = 0.5
_SLOW_SCHEMA = pa.schema([("id", pa.int64()), ("value", pa.utf8())])


class SlowProvider(Provider):
    """A virtual table whose scan is deliberately slow, for profiling WS
    concurrency. The delay is `await asyncio.sleep` — provider.scan runs on the
    event loop (marshaled there via run_coroutine_threadsafe), so N concurrent
    scans over the multiplexed WebSocket overlap to ~one delay, not N×. A
    blocking sleep here would instead stall the loop and serialize them."""

    async def list_tables(self) -> list[str]:
        return ["slow"]

    async def schema(self, name: str) -> pa.Schema:
        if name != "slow":
            raise TableNotFound(name)
        return _SLOW_SCHEMA

    async def scan(self, name: str, columns: Optional[list[str]], filters: Filters) -> pa.Table:
        if name != "slow":
            raise TableNotFound(name)
        await asyncio.sleep(SLOW_SCAN_SECONDS)
        table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]}, schema=_SLOW_SCHEMA)
        return table.select(columns) if columns is not None else table


class CategoricalProvider(Provider):
    """Serves a dictionary-encoded (pandas-Categorical-shaped) column, on the `cats`
    schema of the slow catalog. Proves the schema exchange and scan of a dictionary
    field survive the full served path instead of tearing down the socket."""

    _TABLE = "cats"

    async def list_tables(self) -> list[str]:
        return [self._TABLE]

    async def schema(self, name: str) -> pa.Schema:
        if name != self._TABLE:
            raise TableNotFound(name)
        return pa.schema([("id", pa.int64()), ("cat", pa.dictionary(pa.int8(), pa.utf8()))])

    async def scan(self, name: str, columns: Optional[list[str]], filters: Filters) -> pa.Table:
        if name != self._TABLE:
            raise TableNotFound(name)
        cat = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0], type=pa.int8()), pa.array(["x", "y"]))
        table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64()), "cat": cat})
        return table.select(columns) if columns is not None else table
