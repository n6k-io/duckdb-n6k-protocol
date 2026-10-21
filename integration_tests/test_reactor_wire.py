"""Wire coverage for the C++ reactor served in-process via `n6k_serve_fd`.

This is the regression fence for what `n6k_server.pump` provides — socketpair,
`CALL n6k_serve_fd(...)` on a worker thread, byte pump — and for the frames the
reactor answers on the other side of it.

The shared-op tests are parameterized over `TARGETS`, which is one entry today;
the rest address `TUNED` directly, because they need per-connection knobs the
shipped `register()` adapter deliberately does not expose (`?catalogs=`,
`?ping_interval_ms=`, `?require_token=`).

Run:
    uv run pytest integration_tests/test_reactor_wire.py -v
"""

import asyncio
import io
import json
import urllib.parse
import urllib.request
from typing import Any, Optional

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import websockets

from _targets import (
    FIXTURE_CATALOG,
    SERVER_URL,
    TARGETS,
    TUNED,
    Target,
    WireClient,
    connect,
    hello,
    open_session,
    server_is_up,
)

from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import (
    FT_CANCEL,
    FT_CREDIT,
    FT_ERR,
    FT_HELLO_ACK,
    FT_HELLO_ERR,
    FT_OPEN_ACK,
    FT_OPEN_ERR,
    FT_PING,
    FT_PONG,
    FT_PUSH,
    FT_RESP_CHUNK,
    FT_RESP_END,
    OP_CATALOG_INVALIDATED,
    OP_CATALOG_LIST,
    OP_SCHEMA_LIST,
    OP_EXEC,
    OP_QUERY,
    OP_RPC_SCALAR,
    OP_RPC_TABLE,
    OP_TABLE_SCHEMA,
    OP_TABLES_LIST,
)

pytestmark = pytest.mark.skipif(not server_is_up(), reason="test server not running on :8099")


def arrow_body(table: pa.Table) -> bytes:
    """Serialize a table as the Arrow IPC stream an OP_RPC_TABLE body carries."""
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


async def chunk_key(client: WireClient, op: int, key: str, **args: Any) -> list[Any]:
    """Read a list-valued key off a msgpack RESP_CHUNK.

    CATALOG_LIST and TABLES_LIST answer with a msgpack chunk carrying `schemas` /
    `tables` in the frame header, not with an Arrow stream — so `WireClient.table`
    does not apply to them.
    """
    frames = await client.request(op, **args)
    errs = [h for h, _ in frames if h["t"] == FT_ERR]
    assert not errs, f"unexpected ERR frame: {errs}"
    for header, _ in frames:
        if key in header:
            return list(header[key])
    raise AssertionError(f"no {key!r} in {[dict(h) for h, _ in frames]}")


# ── Shared ops: every target must answer these identically ──────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_handshake_advertises_the_same_contract(target: Target) -> None:
    async with connect(target) as (_client, opened):
        assert opened["t"] == FT_OPEN_ACK
        assert opened["max_concurrent_reqs"] == 64
        assert opened["default_batch_credits"] == 8


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_schema_list_reports_main(target: Target) -> None:
    async with connect(target) as (client, _hello):
        assert "main" in await chunk_key(client, OP_SCHEMA_LIST, "schemas")


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_catalog_list_reports_served_catalogs(target: Target) -> None:
    async with connect(target) as (client, _hello):
        catalogs = await chunk_key(client, OP_CATALOG_LIST, "catalogs")
        assert isinstance(catalogs, list)
        assert len(catalogs) > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_tables_list_reports_the_fixture_tables(target: Target) -> None:
    async with connect(target) as (client, _hello):
        names = {t["name"] for t in await chunk_key(client, OP_TABLES_LIST, "tables")}
        assert {"users", "products"} <= names


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_table_schema_is_columns_without_rows(target: Target) -> None:
    async with connect(target) as (client, _hello):
        schema = await client.table(OP_TABLE_SCHEMA, schema="main", table="users")
        assert schema.num_rows == 0
        assert schema.schema.names == ["id", "name", "age"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_scan_streams_the_fixture_rows(target: Target) -> None:
    async with connect(target) as (client, _hello):
        rows = await client.scan("main", "users")
        assert rows.num_rows == 3
        assert rows.schema.names == ["id", "name", "age"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_scan_projects_columns(target: Target) -> None:
    async with connect(target) as (client, _hello):
        rows = await client.scan("main", "users", columns=["id"])
        assert rows.schema.names == ["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_query_streams_passthrough_sql(target: Target) -> None:
    async with connect(target) as (client, _hello):
        assert (await client.table(OP_QUERY, sql="SELECT 42 AS n")).column("n").to_pylist() == [42]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", TARGETS, ids=str)
async def test_scanning_a_missing_table_errors(target: Target) -> None:
    async with connect(target) as (client, _hello):
        assert await client.scan_error("main", "nope")


# ── RPC as DuckDB table-function dispatch ───────────────────────────────────


@pytest.mark.asyncio
async def test_rpc_calls_a_table_macro_in_the_served_catalog() -> None:
    async with connect(TUNED) as (client, _hello):
        rows = await client.table(OP_RPC_SCALAR, function="doubler", args=[21])
        assert rows.column("doubled").to_pylist() == [42]


@pytest.mark.asyncio
async def test_rpc_arg_types_survive_the_round_trip() -> None:
    """Each wire arg type must render as the matching SQL literal, not as text."""
    async with connect(TUNED) as (client, _hello):
        rows = await client.table(OP_RPC_SCALAR, function="echo", args=["hello", 7])
        assert rows.column("arg0").to_pylist() == ["hello"]
        assert rows.column("arg1").to_pylist() == [7]


@pytest.mark.asyncio
async def test_rpc_resolves_by_arity() -> None:
    async with connect(TUNED) as (client, _hello):
        one = await client.table(OP_RPC_SCALAR, function="echo", args=["only"])
        assert one.schema.names == ["arg0"]


@pytest.mark.asyncio
async def test_rpc_accepts_a_struct_argument() -> None:
    """A STRUCT arg arrives as a JSON object; RenderSqlValue rejects those on purpose,
    so RPC args go through RenderRpcArgValue instead."""
    async with connect(TUNED) as (client, _hello):
        rows = await client.table(OP_RPC_SCALAR, function="struct_arg", args=[{"x": 1, "y": 2}])
        assert rows.column("x").to_pylist() == [1]
        assert rows.column("y").to_pylist() == [2]


@pytest.mark.asyncio
async def test_rpc_can_reach_a_builtin_table_function() -> None:
    """An unqualified name falls back to the system catalog, which is how a function the
    host registered on the instance (rather than in the served catalog) is reached."""
    async with connect(TUNED) as (client, _hello):
        rows = await client.table(OP_RPC_SCALAR, function="range", args=[3])
        assert rows.num_rows == 3


@pytest.mark.asyncio
async def test_rpc_on_an_unknown_function_errors() -> None:
    async with connect(TUNED) as (client, _hello):
        err = await client.error(OP_RPC_SCALAR, function="no_such_function", args=[])
        assert "no_such_function" in err["exception_message"]


# A REQ_RPC_SCALAR with no `function` is not covered here: `function` is required by the generated
# header schema, so `pack_frame` refuses to encode one and no conforming client can send it. The
# server still guards against it, for a peer that does not validate.


@pytest.mark.asyncio
async def test_rpc_table_receives_the_pushed_rows() -> None:
    """OP_RPC_TABLE ships a table of rows in the request body alongside the call."""
    async with connect(TUNED) as (client, _hello):
        pushed = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
        rows = await client.table(OP_RPC_TABLE, arrow_body(pushed), function="sum_table")
        assert rows.column("total").to_pylist() == [6]


@pytest.mark.asyncio
async def test_rpc_table_input_does_not_leak_between_requests() -> None:
    """The staged view is per-worker, so a second call must not see the first's rows."""
    async with connect(TUNED) as (client, _hello):
        first = await client.table(
            OP_RPC_TABLE, arrow_body(pa.table({"id": pa.array([5], type=pa.int64())})), function="sum_table"
        )
        assert first.column("total").to_pylist() == [5]

        second = await client.table(
            OP_RPC_TABLE, arrow_body(pa.table({"id": pa.array([1, 1], type=pa.int64())})), function="sum_table"
        )
        assert second.column("total").to_pylist() == [2]


@pytest.mark.asyncio
async def test_rpc_table_carries_scalar_args_too() -> None:
    async with connect(TUNED) as (client, _hello):
        pushed = pa.table({"id": pa.array([1, 2], type=pa.int64())})
        err = await client.error(OP_RPC_TABLE, arrow_body(pushed), function="no_such_table_fn", args=[1])
        assert "no_such_table_fn" in err["exception_message"]


@pytest.mark.asyncio
async def test_rpc_table_uses_a_subquery_for_a_declared_table_parameter() -> None:
    """A native in-out function declares TABLE, so it gets a sub-select, not a view name.

    The server picks the calling convention from the target's own signature, so this
    and `sum_table` (a macro, which cannot declare one) must both work unchanged.
    """
    async with connect(TUNED) as (client, _hello):
        pushed = pa.table({"id": pa.array([10, 20, 30], type=pa.int64())})
        rows = await client.table(OP_RPC_TABLE, arrow_body(pushed), function="n6k_testing_sum_table")
        assert rows.column("total").to_pylist() == [60]


@pytest.mark.asyncio
async def test_unbounded_rpc_is_paced_by_credits_and_ends_on_cancel() -> None:
    """A subscription-shaped RPC that never ends must park on credit, resume on refill,
    and terminate with RESP_END{cancelled} — not buffer without limit."""
    async with connect(TUNED) as (client, hello):
        budget = hello["default_batch_credits"]
        # No inter-row delay: DuckDB accumulates a table function's output toward a full vector
        # before handing a chunk to the result, so a per-row sleep paces the CHUNK rate by
        # ~2048x and starves this test rather than slowing it. Credit pacing is what is under
        # test here, and it does not need the rows to be slow.
        req_id = await client.send(OP_RPC_SCALAR, function="n6k_testing_stream_counter", args=[0])

        chunks = 0
        while chunks < budget:
            header, _ = unpack_frame(await asyncio.wait_for(client.ws.recv(), timeout=10.0))
            if header["t"] == FT_RESP_CHUNK:
                chunks += 1
        assert chunks == budget

        # Budget spent: the stream must now be parked rather than still arriving.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.ws.recv(), timeout=1.5)

        await client.ws.send(pack_frame({"t": FT_CREDIT, "id": req_id, "n": 2, "ns": client.ns}))
        for _ in range(2):
            header, _ = unpack_frame(await asyncio.wait_for(client.ws.recv(), timeout=10.0))
            assert header["t"] == FT_RESP_CHUNK

        await client.ws.send(pack_frame({"t": FT_CANCEL, "id": req_id, "ns": client.ns}))
        while True:
            header, _ = unpack_frame(await asyncio.wait_for(client.ws.recv(), timeout=10.0))
            if header["t"] == FT_RESP_END:
                assert header.get("cancelled") is True
                break


@pytest.mark.asyncio
async def test_bounded_rpc_stream_completes() -> None:
    async with connect(TUNED) as (client, _hello):
        rows = await client.table(OP_RPC_SCALAR, function="n6k_testing_stream_counter", args=[3])
        assert rows.column("n").to_pylist() == [0, 1, 2]


@pytest.mark.asyncio
async def test_rpc_streams_a_large_result_under_credits() -> None:
    """RPC uses the same StreamResult path as READ_PLAN, so backpressure applies unchanged."""
    async with connect(TUNED) as (client, _hello):
        rows = await client.table(OP_RPC_SCALAR, function="range", args=[5000])
        assert rows.num_rows == 5000


# ── Auth: the host's authenticate step sees the HELLO token ─────────────────


@pytest.mark.asyncio
async def test_authorized_hello_opens_a_session() -> None:
    async with connect(TUNED, token="letmein", require_token="letmein") as (client, _opened):
        assert (await client.scan("main", "users")).num_rows == 3


@pytest.mark.asyncio
async def test_a_bad_token_is_refused_with_the_hosts_reason_and_the_socket_ends() -> None:
    url = TUNED.ws_url(catalog=None, require_token="letmein")
    async with websockets.connect(url) as ws:
        header = await hello(ws, "wrong")
        assert header["t"] == FT_HELLO_ERR
        assert "bad token" in header["exception_message"]

        with pytest.raises(websockets.exceptions.ConnectionClosed):
            async with asyncio.timeout(10):
                while True:
                    await ws.recv()


# ── Server-initiated invalidation ───────────────────────────────────────────


def _push_invalidate(catalog: str = "db", schemas: str = "main") -> dict[str, Any]:
    """Ask the server to broadcast an invalidation to every session serving `catalog`."""
    url = f"{SERVER_URL}/debug/push_invalidate?catalog={catalog}&schemas={schemas}"
    request = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(request, timeout=5) as response:
        return dict(json.loads(response.read()))


@pytest.mark.asyncio
async def test_invalidation_reaches_an_attached_client() -> None:
    """A catalog change has to reach clients that already cached its schema.

    The client drops its cached tables on this frame, so without it a table created
    after ATTACH stays invisible until reconnect.
    """
    async with connect(TUNED) as (client, _hello):
        result = await asyncio.to_thread(_push_invalidate)
        assert result["sent"] >= 1, result

        header, _ = unpack_frame(await asyncio.wait_for(client.ws.recv(), timeout=5.0))
        assert header["t"] == FT_PUSH
        assert header["op"] == OP_CATALOG_INVALIDATED
        assert header["schemas"] == ["main"]
        assert "id" not in header, "a push answers no request, so it carries no req_id"


@pytest.mark.asyncio
async def test_invalidation_is_stamped_with_the_right_session() -> None:
    """Two catalogs on one socket: only the session bound to the named one hears it."""
    url = TUNED.ws_url(catalog=None, catalogs="db,other")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        for ns, catalog in ((1, "db"), (2, "other")):
            assert (await open_session(ws, ns, catalog))["t"] == FT_OPEN_ACK

        result = await asyncio.to_thread(_push_invalidate, "other", "main")
        assert result["sent"] >= 1, result

        header, _ = unpack_frame(await asyncio.wait_for(ws.recv(), timeout=5.0))
        assert header["t"] == FT_PUSH
        assert header["ns"] == 2, "stamped for the session serving `other`, not `db`"


@pytest.mark.asyncio
async def test_invalidation_with_no_live_session_is_a_no_op() -> None:
    """Named catalog nobody serves, rather than waiting for the whole server to be idle.

    "No live session" cannot be asserted globally from inside the suite: the run shares
    one process, so client connections opened by earlier files are still alive and still
    holding their sessions open — correctly. Scoping the question to a catalog nothing
    is attached to asks the same thing and does not depend on test order.
    """
    result = await asyncio.to_thread(_push_invalidate, "nothing_is_attached_here", "main")
    assert result == {"handlers": 0, "sent": 0}


# ── The /debug control plane the conformance suite needs ────────────────────


def _debug(path: str, params: Optional[dict[str, str]] = None, method: str = "GET") -> dict[str, Any]:
    url = f"{SERVER_URL}/debug/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=5) as response:
        return dict(json.loads(response.read()))


@pytest.mark.asyncio
async def test_server_exec_bypasses_the_client() -> None:
    """Reaching the database without going through the protocol.

    This is what makes cache-invalidation tests meaningful: if the harness had to use
    the client to change server state, the client would refresh its own cache on the
    way and there would be nothing stale left to test.
    """
    async with connect(TUNED) as (client, _hello):
        # Absent beforehand, so the change below is demonstrably this call's doing.
        assert await client.scan_error("main", "side_made")

        result = await asyncio.to_thread(
            _debug,
            "server_exec",
            {"catalog": "db", "sql": "CREATE TABLE db.main.side_made (i INTEGER)"},
            "POST",
        )
        assert result["handlers"] >= 1

        rows = await asyncio.to_thread(
            _debug, "server_query", {"catalog": "db", "sql": "SELECT count(*) FROM db.main.side_made"}
        )
        assert rows["rows"] == [[0]]

        # Reachable over the wire now — the server really did change.
        #
        # Note this does NOT show the client's cache going stale: a raw WireClient keeps no
        # catalog cache, so its scan always asks the server. The staleness that makes
        # server_exec worth having is only visible to a client that caches, which is what
        # test_attached_native.py::test_invalidation_makes_a_new_table_visible covers.
        assert (await client.scan("main", "side_made")).num_rows == 0


@pytest.mark.asyncio
async def test_counts_report_credit_pauses() -> None:
    """Backpressure leaves no trace on the wire, so it is asserted through counters.

    A stream that paced correctly and one that never filled its window look identical
    from outside; only the server knows it had to stop and wait.
    """
    async with connect(TUNED) as (client, hello):
        before = (await asyncio.to_thread(_debug, "counts"))["ws"]["credit_pauses"]

        # Far more rows than the credit window, so the stream must park at least once.
        req_id = await client.send(OP_RPC_SCALAR, function="n6k_testing_stream_counter", args=[0])
        for _ in range(hello["default_batch_credits"]):
            while True:
                header, _ = unpack_frame(await asyncio.wait_for(client.ws.recv(), timeout=10.0))
                if header["t"] == FT_RESP_CHUNK:
                    break

        after = (await asyncio.to_thread(_debug, "counts"))["ws"]["credit_pauses"]
        assert after > before, f"{before} -> {after}"

        await client.ws.send(pack_frame({"t": FT_CANCEL, "id": req_id, "ns": client.ns}))
        while True:
            header, _ = unpack_frame(await asyncio.wait_for(client.ws.recv(), timeout=10.0))
            if header["t"] == FT_RESP_END:
                break

        cancels = (await asyncio.to_thread(_debug, "counts"))["ws"]["cancels"]
        assert cancels >= 1


@pytest.mark.asyncio
async def test_close_all_drops_live_sockets() -> None:
    async with connect(TUNED) as (client, _hello):
        result = await asyncio.to_thread(_debug, "ws/close_all", None, "POST")
        assert result["closed"] >= 1

        with pytest.raises(websockets.exceptions.ConnectionClosed):
            async with asyncio.timeout(10):
                while True:
                    await client.ws.recv()


# ── Tuned-only: what the per-connection knobs make testable ─────────────────


@pytest.mark.asyncio
async def test_each_connection_gets_its_own_database_instance() -> None:
    """A write on one connection must not be visible to the next.

    The mount opens a fresh `duckdb.connect()` per WebSocket, so this is the
    per-connection isolation it claims — and the thing that would quietly break
    if it ever started sharing one instance across sessions.
    """
    async with connect(TUNED) as (client, _hello):
        await client.request(OP_EXEC, sql="CREATE TABLE main.leak_probe (i INTEGER)")

    async with connect(TUNED) as (client, _hello):
        names = {t["name"] for t in await chunk_key(client, OP_TABLES_LIST, "tables")}
        assert "leak_probe" not in names


@pytest.mark.asyncio
async def test_marker_row_names_the_serving_catalog() -> None:
    async with connect(TUNED) as (client, _hello):
        marker = await client.scan("main", "marker")
        assert marker.column("catalog").to_pylist() == [FIXTURE_CATALOG]


@pytest.mark.asyncio
async def test_multi_catalog_routes_sessions_by_ns() -> None:
    """Two catalogs on one socket: each `ns` reads only its own.

    The marker table is what makes a mis-route visible in the data rather than as
    a missing table — both catalogs carry the same fixture schema.
    """
    url = TUNED.ws_url(catalog=None, catalogs="db,other")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        for ns, catalog in ((1, "db"), (2, "other")):
            opened = await open_session(ws, ns, catalog)
            assert opened["t"] == FT_OPEN_ACK and opened["ns"] == ns

        client = WireClient(ws)
        for ns, catalog in ((1, "db"), (2, "other")):
            marker = await client.scan("main", "marker", ns=ns)
            assert marker.column("catalog").to_pylist() == [catalog]


@pytest.mark.asyncio
async def test_closing_one_session_leaves_its_sibling_serving() -> None:
    url = TUNED.ws_url(catalog=None, catalogs="db,other")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        for ns, catalog in ((1, "db"), (2, "other")):
            assert (await open_session(ws, ns, catalog))["t"] == FT_OPEN_ACK

        await ws.send(pack_frame({"t": FT_CANCEL, "id": 0, "ns": 1}))
        client = WireClient(ws)
        assert await client.scan_error("main", "marker", ns=1)
        marker = await client.scan("main", "marker", ns=2)
        assert marker.column("catalog").to_pylist() == ["other"]


@pytest.mark.asyncio
async def test_a_request_on_an_unopened_ns_errors() -> None:
    url = TUNED.ws_url(catalog=None)
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        err = await WireClient(ws, ns=5).scan_error("main", "users")
        assert "send FT_OPEN first" in err["exception_message"]


@pytest.mark.asyncio
async def test_unserved_catalog_is_refused() -> None:
    url = TUNED.ws_url(catalog=None, catalogs="db,other")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        header = await open_session(ws, 1, "nope")
        assert header["t"] == FT_OPEN_ERR
        assert header["ns"] == 1


@pytest.mark.asyncio
async def test_memory_catalog_is_not_served() -> None:
    url = TUNED.ws_url(catalog=None)
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        assert (await open_session(ws, 1, "memory"))["t"] == FT_OPEN_ERR


@pytest.mark.asyncio
async def test_duplicate_ns_is_refused() -> None:
    url = TUNED.ws_url(catalog=None)
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        assert (await open_session(ws, 1, "db"))["t"] == FT_OPEN_ACK
        header = await open_session(ws, 1, "db")
        assert header["t"] == FT_OPEN_ERR
        assert "duplicate" in header["exception_message"]


@pytest.mark.asyncio
async def test_zero_ns_is_refused() -> None:
    url = TUNED.ws_url(catalog=None)
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        header = await open_session(ws, 0, "db")
        assert header["t"] == FT_OPEN_ERR


@pytest.mark.asyncio
async def test_server_sends_keepalive_pings() -> None:
    """An idle connection must keep emitting traffic, or proxies reap it.

    Neither client initiates a ping, so if the server does not either, an idle
    n6k connection is silent on the wire until it is dropped.
    """
    url = TUNED.ws_url(ping_interval_ms="200")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK

        pings = []
        while len(pings) < 3:
            header, _ = unpack_frame(await asyncio.wait_for(ws.recv(), timeout=5.0))
            if header["t"] == FT_PING:
                pings.append(header)

        assert [p["id"] for p in pings] == [1, 2, 3], "ping ids are monotonic from 1, no gaps"
        assert all("ns" not in p for p in pings), "keepalive is connection-scoped, never ns-stamped"


@pytest.mark.asyncio
async def test_keepalive_can_be_disabled() -> None:
    url = TUNED.ws_url(ping_interval_ms="0")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(ws.recv(), timeout=1.0)


@pytest.mark.asyncio
async def test_pong_is_accepted_silently() -> None:
    """Answering our keepalive must not look like an unknown frame, or draw a reply."""
    url = TUNED.ws_url(ping_interval_ms="0")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        await ws.send(pack_frame({"t": FT_PONG, "id": 1}))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(ws.recv(), timeout=1.0)

        assert (await open_session(ws, 1, "db"))["t"] == FT_OPEN_ACK
        client = WireClient(ws)
        assert (await client.table(OP_QUERY, sql="SELECT 1 AS n")).column("n").to_pylist() == [1]


@pytest.mark.asyncio
async def test_client_ping_is_answered_with_pong() -> None:
    url = TUNED.ws_url(ping_interval_ms="0")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        await ws.send(pack_frame({"t": FT_PING, "id": 77}))
        header, _ = unpack_frame(await asyncio.wait_for(ws.recv(), timeout=5.0))
        assert header["t"] == FT_PONG
        assert header["id"] == 77


@pytest.mark.asyncio
async def test_an_idle_open_session_is_left_alone() -> None:
    url = TUNED.ws_url(ping_interval_ms="0")
    async with websockets.connect(url) as ws:
        assert (await hello(ws))["t"] == FT_HELLO_ACK
        assert (await open_session(ws, 1, "db"))["t"] == FT_OPEN_ACK
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(ws.recv(), timeout=7.0)
        client = WireClient(ws)
        assert (await client.table(OP_QUERY, sql="SELECT 1 AS n")).column("n").to_pylist() == [1]
