"""Server targets for the wire-conformance tests, and the raw client that drives them.

Every n6k server implementation must answer the same frames the same way. These tests
prove that by speaking the protocol directly — `websockets` + `pack_frame`/`unpack_frame`,
no DuckDB extension and no client-side code — so they are implementation-neutral by
construction and can be pointed at anything that serves the wire.

`TARGETS` is the list of those implementations, each mounted at a different path on the
one test server (`uv run test-server`). Adding a new server means adding one `Target`
here; every test parameterized over `TARGETS` then covers it. That is the point of this
module: the alternative is what the suite grew into first, where each file hard-coded
its own mount paths, its own port probe, and its own copy of the request client.
"""

import io
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional

import pyarrow as pa
import pyarrow.ipc as ipc
import websockets

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_ERR,
    FT_HELLO,
    FT_HELLO_ACK,
    FT_OPEN,
    FT_OPEN_ACK,
    FT_REQ,
    FT_RESP_CHUNK,
    FT_RESP_END,
    FT_RESP_SCHEMA,
    N6K_PROTOCOL_VERSION,
    OP_CATALOG_LIST,
    OP_READ_PLAN,
)

SERVER_HOST = os.environ.get("N6K_TEST_HOST", "127.0.0.1")
SERVER_PORT = int(os.environ.get("N6K_TEST_PORT", "8099"))
SERVER_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"

# The catalog every target seeds from fixtures.sql, and therefore the one the
# cross-implementation comparisons address.
FIXTURE_CATALOG = "db"


@dataclass(frozen=True)
class Target:
    """One server implementation, addressed by the path it is mounted at."""

    id: str
    mount: str

    def ws_url(self, catalog: Optional[str] = FIXTURE_CATALOG, **query: str) -> str:
        url = f"ws://{SERVER_HOST}:{SERVER_PORT}{self.mount}/ws"
        params = dict(query)
        if catalog is not None:
            params.setdefault("catalog", catalog)
        if params:
            url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
        return url

    def __str__(self) -> str:
        return self.id


# The root mount, reached through the shipped `register()` adapter — the only implementation.
# `TARGETS` is a list of one on purpose: the parametrization is what makes adding the next server a
# one-line change here rather than an edit to every test that must then cover it.
REGISTER = Target(
    id="register",
    mount="",
)

TARGETS = [REGISTER]

# Not a target: the same reactor behind the same pump, mounted separately only because the
# conformance suite needs per-connection knobs the shipped adapter deliberately has no room
# for (`?catalogs=`, `?ping_interval_ms=`, `?require_token=`). Tests that exercise those use
# it directly; nothing about the wire differs, so parametrizing over it would prove nothing.
TUNED = Target(
    id="tuned",
    mount="/tuned",
)


# Substrait JSON plan builders. A READ_PLAN is the only read op qualified against the
# session's catalog (QUERY is raw SQL), so it is what the conformance tests scan with.

# The fixture tables the scans below address, as (column, substrait type) pairs.
FIXTURE_TABLES: dict[str, list[tuple[str, str]]] = {
    "users": [("id", "i32"), ("name", "string"), ("age", "i32")],
    "marker": [("catalog", "string")],
    "side_made": [("i", "i32")],
    "nope": [("id", "i32")],
}


def substrait_nullable(kind: str) -> dict[str, Any]:
    return {kind: {"nullability": "NULLABILITY_NULLABLE"}}


def substrait_field(i: int) -> dict[str, Any]:
    return {"selection": {"directReference": {"structField": {"field": i}}, "rootReference": {}}}


def substrait_read(
    schema: str, table: str, columns: list[tuple[str, str]], fields: Optional[list[int]] = None
) -> dict[str, Any]:
    rel: dict[str, Any] = {
        "read": {
            "baseSchema": {
                "names": [name for name, _ in columns],
                "struct": {
                    "types": [substrait_nullable(kind) for _, kind in columns],
                    "nullability": "NULLABILITY_REQUIRED",
                },
            },
            "namedTable": {"names": [schema, table]},
        }
    }
    if fields is not None:
        rel["read"]["projection"] = {"select": {"structItems": [{"field": f} for f in fields]}}
    return rel


def substrait_plan(rel: dict[str, Any], names: list[str], functions: list[str] = ()) -> str:
    return json.dumps(
        {
            "version": {"minorNumber": 98, "producer": "test"},
            "extensionUris": [{"extensionUriAnchor": 1, "uri": "https://n6k.io/substrait/duckdb.yaml"}],
            "extensions": [
                {"extensionFunction": {"extensionUriReference": 1, "functionAnchor": i + 1, "name": name}}
                for i, name in enumerate(functions)
            ],
            "relations": [{"root": {"input": rel, "names": names}}],
        }
    )


def scan_plan(schema: str, table: str, columns: Optional[list[str]] = None) -> str:
    """A READ_PLAN body scanning `schema.table` (a FIXTURE_TABLES entry), optionally
    projected to `columns`."""
    all_columns = FIXTURE_TABLES[table]
    names = [name for name, _ in all_columns]
    fields = None if columns is None else [names.index(c) for c in columns]
    return substrait_plan(substrait_read(schema, table, all_columns, fields), columns or names)


def server_is_up(timeout: float = 2.0) -> bool:
    """True if the test server answers on its debug endpoint."""
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=timeout)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        return False
    return True


class WireClient:
    """A raw n6k protocol client: one request id space, frames in and out.

    Wraps an open WebSocket whose session `ns` is already OPENed. Request ids are allocated
    monotonically from 1, matching the client contract (id 0 is reserved for the
    session-close CANCEL).
    """

    def __init__(self, ws: Any, ns: int = 1) -> None:
        self.ws = ws
        self.ns = ns
        self._next_id = 1

    async def send(self, op: int, body: bytes = b"", **args: Any) -> int:
        """Send one REQ. `body` is the raw payload appended after the header — Arrow IPC
        for the ops that push rows (INSERT, RPC_TABLE), empty for everything else."""
        req_id = self._next_id
        self._next_id += 1
        scope = {} if op == OP_CATALOG_LIST else {"ns": self.ns}
        await self.ws.send(pack_frame({"t": FT_REQ, "id": req_id, "op": op, **scope, **args}, body))
        return req_id

    async def drain(self, req_id: int) -> list[tuple[dict[str, Any], bytes]]:
        """Collect frames until the terminal one for `req_id` (RESP_END or ERR)."""
        frames: list[tuple[dict[str, Any], bytes]] = []
        while True:
            header, body = unpack_frame(await self.ws.recv())
            frames.append((header, bytes(body)))
            if header.get("id") == req_id and header["t"] in (FT_RESP_END, FT_ERR):
                return frames

    async def request(self, op: int, body: bytes = b"", **args: Any) -> list[tuple[dict[str, Any], bytes]]:
        return await self.drain(await self.send(op, body, **args))

    async def table(self, op: int, body: bytes = b"", **args: Any) -> pa.Table:
        """Run a streaming op and reassemble its Arrow IPC stream.

        Concatenating the RESP_SCHEMA body with every RESP_CHUNK body yields a valid
        IPC stream regardless of where the server put its message boundaries — which
        is exactly the framing latitude the protocol grants (see the Arrow message
        framing section of docs/n6k-network-protocol.md).
        """
        frames = await self.request(op, body, **args)
        errs = [h for h, _ in frames if h["t"] == FT_ERR]
        assert not errs, f"unexpected ERR frame: {errs}"
        schema = [b for h, b in frames if h["t"] == FT_RESP_SCHEMA][0]
        chunks = [b for h, b in frames if h["t"] == FT_RESP_CHUNK]
        return ipc.open_stream(io.BytesIO(schema + b"".join(chunks))).read_all()

    async def error(self, op: int, body: bytes = b"", **args: Any) -> dict[str, Any]:
        """Run an op expected to fail and return the ERR header."""
        frames = await self.request(op, body, **args)
        errs = [h for h, _ in frames if h["t"] == FT_ERR]
        assert errs, f"expected an ERR frame, got {[h['t'] for h, _ in frames]}"
        return errs[0]

    async def scan(self, schema: str, table: str, columns: Optional[list[str]] = None, **args: Any) -> pa.Table:
        """Scan a fixture table through the session's catalog via READ_PLAN."""
        return await self.table(OP_READ_PLAN, plan=scan_plan(schema, table, columns), **args)

    async def scan_error(self, schema: str, table: str, **args: Any) -> dict[str, Any]:
        return await self.error(OP_READ_PLAN, plan=scan_plan(schema, table), **args)


async def hello(ws: Any, token: str = "") -> dict[str, Any]:
    await ws.send(pack_frame({"t": FT_HELLO, "token": token, "protocol_version": N6K_PROTOCOL_VERSION}))
    header, _ = unpack_frame(await ws.recv())
    return dict(header)


async def open_session(ws: Any, ns: int, catalog: str) -> dict[str, Any]:
    await ws.send(pack_frame({"t": FT_OPEN, "ns": ns, "catalog": catalog}))
    header, _ = unpack_frame(await ws.recv())
    return dict(header)


class connect:
    """Async context manager yielding `(WireClient, open_ack_header)` for a target:
    FT_HELLO with `token`, then FT_OPEN of `catalog` as ns 1."""

    def __init__(self, target: Target, catalog: Optional[str] = FIXTURE_CATALOG, token: str = "", **query: str) -> None:
        self._url = target.ws_url(catalog, **query)
        self._catalog = catalog or FIXTURE_CATALOG
        self._token = token
        self._ws: Any = None

    async def __aenter__(self) -> tuple[WireClient, dict[str, Any]]:
        self._ws = await websockets.connect(self._url)
        ack = await hello(self._ws, self._token)
        assert ack["t"] == FT_HELLO_ACK, ack
        opened = await open_session(self._ws, 1, self._catalog)
        assert opened["t"] == FT_OPEN_ACK, opened
        return WireClient(self._ws), opened

    async def __aexit__(self, *exc: Any) -> None:
        await self._ws.close()
