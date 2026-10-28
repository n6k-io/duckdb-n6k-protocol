"""The FastAPI app: wires auth middleware, per-connection observability,
and the WebSocket mounts the conformance suite targets.

Run via `python -m n6k_server.test_server --port 8099`.
"""

import asyncio
import logging
import os
import re
import shutil
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Callable, Optional

import duckdb
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from n6k_server.context import current
from n6k_server.jsonrpc import JsonRpc, JsonRpcError
from n6k_server.extension import load_vcatprovider, load_n6k, load_n6k_server, load_n6k_testing
from n6k_server.provider import register_provider
from n6k_server.pump import LiveServe, PendingCatalog, ServedCatalog, WsReject
from n6k_server.test_server.fixtures import fixture_statements, rpc_fixture_statements
from n6k_server.test_server.host_rpc import register_host_rpcs
from n6k_server.server_fastapi.register import register, serve_ws
from n6k_server.test_server.auth import http_present_token
from n6k_server.test_server.oauth import mount_oauth_as
from n6k_server.test_server.providers import (
    SLOW_CATALOG,
    CategoricalProvider,
    SlowProvider,
)
from n6k_server.test_server.seeding import (
    BRIDGE_CATALOG,
    SQLITE_CATALOG,
    seed_bridge,
    seed_memory,
    seed_sqlite,
)

logger = logging.getLogger("n6k_server.test_server")

app = FastAPI()

# Always-on auth modes selected by URL prefix, so tests pick a mode by attaching
# to n6k://host:port/<mode> instead of spinning up a server per mode:
#   (root)  open / no auth        ws://…/ws
#   /auth   static bearer token   ws://…/auth/ws   (token == AUTH_TOKEN)
#   /oauth  OAuth EdDSA JWT       ws://…/oauth/ws  (minted by the mounted AS)
# The OAuth Authorization Server (discovery/JWKS/token/device) is mounted here
# too and backs the /oauth verifier.
AUTH_TOKEN = "n6k-test-token"
IDENT_CATALOGS = {"tok-a": "a", "tok-b": "b"}
oauth_server = mount_oauth_as(app)


# ── Test-only observability ─────────────────────────────────────────────────


@dataclass
class _Live:
    """A connection currently being served, and a way to reach its data.

    `side` is a `cursor()` — an independent connection on the same database, taken
    before the serving CALL claimed the original. It is the only way in while a
    connection is live: the served connection blocks inside the CALL for the whole
    session, and duckdb serializes per connection, so reusing it would deadlock.

    Test scaffolding. A real consumer keeps a registry only if it wants one; the
    framework does not hand out either the socket or a side connection.
    """

    ws: WebSocket
    side: duckdb.DuckDBPyConnection
    catalogs: list[str]
    handle: Optional[LiveServe] = None


_live: dict[int, _Live] = {}


@dataclass
class _SocketState:
    temp_dir: Optional[str] = None
    bridge_source: Optional[duckdb.DuckDBPyConnection] = None


_sockets: dict[int, _SocketState] = {}
_bridge_builds = 0

_http_counts: dict[str, int] = {}


# Never auth-gated: test hooks (/debug/*) and the public OAuth AS surface
# (discovery, JWKS) plus the token/device endpoints clients fetch before they
# hold a token. NB /oauth/ is NOT a blanket open prefix — /oauth/* data
# routes require a JWT; only the two AS endpoints below are open.
_OPEN_PREFIXES = ("/debug/", "/.well-known/", "/jwks")
_OPEN_EXACT = ("/oauth/token", "/oauth/device")


def _prefix_auth_mode(path: str) -> Optional[str]:
    if path.startswith("/auth/"):
        return "token"
    if path.startswith("/oauth/"):
        return "jwt"
    return None


@app.middleware("http")
async def _http_auth_and_count(request: Request, call_next: Callable[..., Any]) -> Any:
    path = request.url.path
    is_open = path in _OPEN_EXACT or any(path.startswith(p) for p in _OPEN_PREFIXES)
    if not is_open:
        mode = _prefix_auth_mode(path)
        if mode == "token":
            if http_present_token(request) != AUTH_TOKEN:
                return JSONResponse({"detail": "unauthorized"}, status_code=401)
        elif mode == "jwt":
            token = http_present_token(request)
            if not token or not oauth_server.verify(token):
                return JSONResponse({"detail": "unauthorized"}, status_code=401)
    key = f"{request.method} {request.url.path}"
    _http_counts[key] = _http_counts.get(key, 0) + 1
    logger.info("HTTP %s %s", request.method, request.url.path)
    return await call_next(request)


@app.get("/debug/counts")
def debug_counts() -> dict[str, dict[str, int]]:
    """HTTP request counts, plus flow-control counters read out of the C++ reactor."""
    totals = {"credit_pauses": 0, "cancels": 0, "connections": 0, "requests_handled": 0}
    for live in list(_live.values()):
        try:
            row = live.side.execute("SELECT * FROM n6k_serve_stats()").fetchone()
        except duckdb.Error:
            continue
        if row:
            totals["connections"] += int(row[0])
            totals["requests_handled"] += int(row[1])
            totals["credit_pauses"] += int(row[2])
            totals["cancels"] += int(row[3])
    return {"http": dict(_http_counts), "ws": totals}


@app.get("/debug/bridge_builds")
def debug_bridge_builds() -> dict[str, int]:
    return {"builds": _bridge_builds}


@app.post("/debug/counts/reset")
def debug_counts_reset() -> dict[str, bool]:
    # Only the HTTP tally is resettable: the reactor's counters live for a connection's
    # lifetime and a new connection starts them at zero anyway.
    _http_counts.clear()
    return {"ok": True}


def _sessions_for(catalog: str) -> list[_Live]:
    """Live sockets serving `catalog`. One socket may serve several catalogs (whatever
    `authenticate` returned), so this matches membership rather than equality.

    An entry is added by `authenticate` (a rejected socket never gets one). "Live" is
    approximate by construction. An entry is removed by the `finally` of the `connect`
    generator (or of `/tuned/ws`), which resumes only *after* `serve_and_close_connection`
    has already closed the served connection — so between those two moments an entry is
    still here with a `side` cursor onto a closed database. Callers must tolerate that;
    `_with_side` does.
    """
    return [live for live in _live.values() if catalog in live.catalogs]


async def _with_side(sessions: list[_Live], work: Callable[[duckdb.DuckDBPyConnection], Any]) -> list[Any]:
    """Run `work` on each session's side connection, skipping the ones already torn down.

    A session whose connection closed between `_sessions_for` and here is finished, not
    broken: it has no client left to hear a PUSH and nothing to run SQL against. Letting
    that surface as a 500 turns an ordinary teardown race into a test failure.
    """
    results: list[Any] = []
    for live in sessions:
        try:
            results.append(await asyncio.to_thread(work, live.side))
        except duckdb.Error:
            continue
    return results


@app.post("/debug/server_exec")
async def debug_server_exec(catalog: str, sql: str) -> dict[str, int]:
    """Run SQL against every live session serving `catalog`, bypassing the client.

    Goes in through the side connection, so nothing the client caches is refreshed as
    a side effect — which is what makes it usable for testing PUSH-driven invalidation.
    """
    ran = await _with_side(_sessions_for(catalog), lambda side: side.execute(sql))
    return {"handlers": len(ran)}


@app.get("/debug/server_query")
async def debug_server_query(catalog: str, sql: str) -> dict[str, Any]:
    def _fetch(side: duckdb.DuckDBPyConnection) -> list[Any]:
        return [list(r) for r in side.execute(sql).fetchall()]

    sessions = _sessions_for(catalog)
    # Only the first session is queried — they all serve the same catalog, so one answer
    # is the answer — but which one is "first" may be a corpse, hence the walk.
    for live in sessions:
        try:
            return {"handlers": len(sessions), "rows": await asyncio.to_thread(_fetch, live.side)}
        except duckdb.Error:
            continue
    return {"handlers": len(sessions), "rows": []}


@app.post("/debug/push_invalidate")
async def debug_push_invalidate(catalog: str, schemas: str) -> dict[str, int]:
    """Tell every client attached to `catalog` that those schemas changed.

    Frame construction belongs to the server, so this asks for the PUSH via SQL.
    """
    names = [s for s in schemas.split(",") if s]
    args = ", ".join(f"'{n}'" for n in [catalog] + names)
    sql = f"CALL n6k_serve_push_invalidate({args})"

    def _call(side: duckdb.DuckDBPyConnection) -> int:
        row = side.execute(sql).fetchone()
        return int(row[0]) if row else 0

    counts = await _with_side(_sessions_for(catalog), _call)
    return {"handlers": len(counts), "sent": sum(counts)}


@app.post("/debug/catalog/add")
async def debug_catalog_add(on: str, catalog: str) -> dict[str, int]:
    """Seed `catalog` and serve it on every live /tuned connection serving `on`."""
    if not catalog.replace("_", "").isalnum():
        raise ValueError(f"invalid catalog name {catalog!r}")

    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        # On the loop thread: register_host_rpcs binds the running loop.
        _seed_tuned(cur, [catalog])

    added = 0
    for live in _sessions_for(on):
        if live.handle is None or live.handle.closed or catalog in live.catalogs:
            continue
        try:
            await live.handle.add_catalog(PendingCatalog(catalog, attach))
        except (duckdb.Error, RuntimeError):
            continue
        live.catalogs.append(catalog)
        added += 1
    return {"added": added}


@app.post("/debug/catalog/remove")
async def debug_catalog_remove(catalog: str) -> dict[str, int]:
    """Stop serving `catalog` on every live /tuned connection serving it."""
    removed = 0
    for live in _sessions_for(catalog):
        if live.handle is None or live.handle.closed:
            continue
        try:
            await live.handle.remove_catalog(catalog)
        except (duckdb.Error, RuntimeError):
            continue
        live.catalogs.remove(catalog)
        removed += 1
    return {"removed": removed}


@app.post("/debug/ws/close_all")
async def debug_ws_close_all() -> dict[str, int]:
    sessions = list(_live.values())
    for live in sessions:
        try:
            await live.ws.close(code=1011, reason="forced close")
        except RuntimeError:
            pass
    return {"closed": len(sessions)}


# ── Handlers ────────────────────────────────────────────────────────────────


def _open_con(catalog: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    if catalog == SLOW_CATALOG:
        load_vcatprovider(con)
    load_n6k(con)
    load_n6k_server(con)
    if catalog != SQLITE_CATALOG:
        # rpc_fixture_statements() wraps n6k_testing_stream_counter / _sum_table in
        # macros, and the conformance suite RPCs those table functions by name.
        load_n6k_testing(con)
    return con


def _seed_into(
    con: duckdb.DuckDBPyConnection, catalog: str, temp_dir: Optional[str]
) -> Optional[duckdb.DuckDBPyConnection]:
    if catalog == SLOW_CATALOG:
        # Provider-backed: `slowdb.main.slow` routes into SlowProvider, whose scan
        # sleeps on the event loop. This is the case that requires in-process serving —
        # the provider's rows come from Python callbacks marshalled to this loop, so a
        # subprocess could never serve it.
        con.execute(f"ATTACH ':memory:' AS \"{catalog}\" (TYPE vcatprovider)")
        register_provider(con, catalog=catalog, schema_name="main", provider=SlowProvider())
        register_provider(con, catalog=catalog, schema_name="cats", provider=CategoricalProvider())
        return None
    if catalog == BRIDGE_CATALOG:
        # Bridge-backed: n6k_table_permissions routes to the bridge's own collector, so
        # tables_list reports the bridge's verbs and primary keys.
        return seed_bridge(con, catalog)
    if catalog == SQLITE_CATALOG:
        assert temp_dir is not None
        seed_sqlite(con, catalog, os.path.join(temp_dir, f"{catalog}.sqlite"))
        return None
    seed_memory(con, catalog)
    for statement in rpc_fixture_statements(catalog):
        con.execute(statement)
    return None


# `slowopen_<ms>` attaches <ms> after the socket is served, so OPENs wait on FT_OPEN_PENDING;
# `slowopen_fail_<ms>` fails at <ms> instead.
_SLOWOPEN = re.compile(r"^slowopen_(fail_)?(\d+)$")
_SLOWOPEN_MAX_MS = 120_000
# `slowauth_<ms>` takes <ms> to authenticate, so the client waits on FT_HELLO_PENDING.
_SLOWAUTH = re.compile(r"^slowauth_(\d+)$")


def _slowopen(catalog: str) -> Optional[PendingCatalog]:
    match = _SLOWOPEN.match(catalog)
    if match is None:
        return None
    delay_ms = min(int(match[2]), _SLOWOPEN_MAX_MS)
    fail = match[1] is not None

    async def attach(cur: duckdb.DuckDBPyConnection) -> None:
        await asyncio.sleep(delay_ms / 1000)
        if fail:
            raise RuntimeError(f"slowopen backend failed after {delay_ms}ms")
        await asyncio.to_thread(seed_memory, cur, catalog)

    return PendingCatalog(catalog, attach, message=f"starting {catalog}")


def _credential_ok(mode: str, token: str) -> bool:
    return oauth_server.verify(token) if mode == "jwt" else token == AUTH_TOKEN


def _record_bridge(ws: WebSocket, con: duckdb.DuckDBPyConnection, catalogs: list[str]) -> list[str]:
    global _bridge_builds
    _bridge_builds += 1
    # The side cursor must be taken before the serving CALL claims `con` — after that
    # this is the only way in, and duckdb serializes per connection.
    _live[id(ws)] = _Live(ws=ws, side=con.cursor(), catalogs=catalogs)
    return catalogs


async def _open_db(ws: WebSocket, **_params: str) -> AsyncIterator[duckdb.DuckDBPyConnection]:
    catalog = ws.query_params.get("catalog")
    if not catalog:
        raise WsReject(code=4400, reason="missing required catalog (?catalog= query param)")
    key = id(ws)
    state = _SocketState()
    _sockets[key] = state
    try:
        yield _open_con(catalog)
    finally:
        _sockets.pop(key, None)
        _live.pop(key, None)
        if state.bridge_source is not None:
            state.bridge_source.close()
        if state.temp_dir:
            shutil.rmtree(state.temp_dir, ignore_errors=True)


async def _authenticate(
    ws: WebSocket, con: duckdb.DuckDBPyConnection, token: str, mode: Optional[str] = None, **_params: str
) -> list[ServedCatalog]:
    if mode is not None and not (token and _credential_ok(mode, token)):
        raise WsReject(code=4401, reason="unauthorized")
    catalog = ws.query_params["catalog"]
    if slowauth := _SLOWAUTH.match(catalog):
        await asyncio.sleep(min(int(slowauth[1]), _SLOWOPEN_MAX_MS) / 1000)
    slowopen = _slowopen(catalog)
    if slowopen is not None:
        _record_bridge(ws, con, [catalog])
        return [slowopen]
    state = _sockets[id(ws)]
    if catalog == SQLITE_CATALOG:
        state.temp_dir = tempfile.mkdtemp(prefix="n6k_sqlite_")
    state.bridge_source = _seed_into(con, catalog, state.temp_dir)
    # Registered here rather than in `_seed_into`: the generators run on this loop, and only an
    # async caller has one to give them. Off the loop thread, because creating the entry in a
    # provider catalog lists that provider's tables, and the provider answers on this loop.
    # A sqlite catalog holds tables only; the RPC entries cannot be created in it.
    if catalog != SQLITE_CATALOG:
        loop = asyncio.get_running_loop()
        await asyncio.to_thread(register_host_rpcs, con, catalog, loop)
    return list(_record_bridge(ws, con, [catalog]))


register(app, prefix="", connect=_open_db, authenticate=_authenticate)

register(
    app,
    prefix="/auth",
    connect=_open_db,
    authenticate=lambda ws, con, token, **p: _authenticate(ws, con, token, "token", **p),
)

register(
    app,
    prefix="/oauth",
    connect=_open_db,
    authenticate=lambda ws, con, token, **p: _authenticate(ws, con, token, "jwt", **p),
)


async def _open_ident_db(ws: WebSocket, **_params: str) -> AsyncIterator[duckdb.DuckDBPyConnection]:
    try:
        yield _open_con("db")
    finally:
        _live.pop(id(ws), None)


def _authenticate_ident(ws: WebSocket, con: duckdb.DuckDBPyConnection, token: str, **_params: str) -> list[str]:
    catalog = IDENT_CATALOGS.get(token)
    if catalog is None:
        raise WsReject(code=4401, reason="unknown identity")
    seed_memory(con, catalog)
    return _record_bridge(ws, con, [catalog])


register(app, prefix="/ident", connect=_open_ident_db, authenticate=_authenticate_ident)


rpc_api = JsonRpc()


@rpc_api.method()
def echo(x: Any) -> Any:
    return x


@rpc_api.method()
def fail() -> None:
    raise JsonRpcError(1234, "boom", {"k": 1})


@rpc_api.method()
async def ping_me(method: str) -> None:
    await current().notify(method, {"ok": True})


async def _welcome(ws: WebSocket, handle: LiveServe, **_: str) -> None:
    await handle.connection.notify("welcome", {"ok": True})


register(app, prefix="/rpc", connect=_open_db, authenticate=_authenticate, on_serve=_welcome, jsonrpc=rpc_api)


# ── /tuned: the same reactor, with per-connection knobs ─────────────────────
#
# Per-connection knobs `register()` has no room for, so this route calls `serve_ws`
# itself:
#
#     ?catalogs=db,other     serve several catalogs on one socket (multiplexed by `ns`)
#     ?ping_interval_ms=200  override the keepalive, which is otherwise process-wide
#                            ($N6K_PING_INTERVAL) and far too slow to assert on
#     ?require_token=secret  the token the client's FT_HELLO must carry
#
# It is still a pure byte pump — no protocol logic — and registers into the same
# `_live` map as every other mount, so the `/debug/*` control plane above reaches it.


def _parse_catalogs(raw: Optional[str]) -> list[str]:
    """Catalog names from `?catalogs=`, defaulting to the fixture catalog.

    Interpolated as SQL identifiers when the prelude runs, so anything but a bare
    alphanumeric name is refused here rather than passed along.
    """
    names = [n.strip() for n in (raw or "db").split(",") if n.strip()]
    for name in names:
        if not name.replace("_", "").isalnum():
            raise ValueError(f"invalid catalog name {name!r}")
    if len(set(names)) != len(names):
        raise ValueError("duplicate catalog name")
    return names or ["db"]


def _parse_ping_interval_ms(raw: Optional[str]) -> Optional[int]:
    """`?ping_interval_ms=` as an int, or None when the query param is absent."""
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"invalid ping_interval_ms {raw!r}")
    if value < 0:
        raise ValueError("ping_interval_ms must not be negative")
    return value


def _seed_tuned(con: duckdb.DuckDBPyConnection, catalogs: list[str]) -> None:
    """Attach, seed and RPC-enable every named catalog on `con`.

    The tables come from the same `fixtures.sql` the other mounts use, so a client
    that works against those works here unchanged. On top of it each catalog gets a
    `main.marker` row naming itself — a cheap way to see, in the data, whether a
    session ever reads the wrong catalog.
    """
    for name in catalogs:
        con.execute(f"ATTACH ':memory:' AS {name}")
        for statement in fixture_statements(name):
            con.execute(statement)
        con.execute(f"CREATE TABLE {name}.main.marker (catalog VARCHAR)")
        con.execute(f"INSERT INTO {name}.main.marker VALUES ('{name}')")
        for statement in rpc_fixture_statements(name):
            con.execute(statement)
        # Every served catalog gets them, so a session can call a host RPC on either.
        register_host_rpcs(con, name)


@app.websocket("/tuned/ws")
async def _tuned_ws(ws: WebSocket) -> None:
    await ws.accept()
    try:
        catalogs = _parse_catalogs(ws.query_params.get("catalogs"))
        ping_interval_ms = _parse_ping_interval_ms(ws.query_params.get("ping_interval_ms"))
    except ValueError as exc:
        await ws.close(code=4400, reason=str(exc))
        return

    require_token = ws.query_params.get("require_token")

    def connect(ws: WebSocket) -> duckdb.DuckDBPyConnection:
        return _open_con("db")

    def authenticate(ws: WebSocket, con: duckdb.DuckDBPyConnection, token: str) -> list[str]:
        if require_token is not None and token != require_token:
            raise WsReject(code=4401, reason=f"bad token for catalogs {catalogs!r}")
        _seed_tuned(con, catalogs)
        return _record_bridge(ws, con, catalogs)

    def on_serve(ws: WebSocket, handle: LiveServe) -> None:
        live = _live.get(id(ws))
        if live is not None:
            live.handle = handle

    try:
        await serve_ws(ws, connect, authenticate, {}, ping_interval_ms=ping_interval_ms, on_serve=on_serve)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001 - surface the reason, don't hang the client
        logger.exception("tuned ws failed")
        try:
            await ws.close(code=1011, reason=str(exc)[:120])
        except RuntimeError:
            pass
    finally:
        _live.pop(id(ws), None)


# CORS so a cross-origin-isolated browser page (served from an ephemeral port by
# the browser test harness) can reach this server (the WS upgrade and /debug).
# Added last → outermost middleware → handles the OPTIONS preflight before the
# auth/count middleware. A successful CORS response also satisfies the page's
# COEP: require-corp.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
