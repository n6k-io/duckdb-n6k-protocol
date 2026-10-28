# 0.11.4

Breaking, for downstream:

- Needs the `duckdb-virtual-catalog` build with the `vcat` names: extensions `vcatbridge` / `vcatprovider` (`TYPE` too), SQL functions `vcatbridge_*` / `vcatprovider_*`, `vcatbridge_lockdown`.
- `load_virtual_catalog_bridge` / `load_virtual_catalog_provider` → `load_vcatbridge` / `load_vcatprovider`; `VIRTUAL_CATALOG_BRIDGE` / `VIRTUAL_CATALOG_PROVIDER` → `VCATBRIDGE` / `VCATPROVIDER`.

# 0.11.3

Summary (install the `[mcp]` extra; needs the extension built from the same commit):

- `register(..., jsonrpc=JsonRpc(), mcp=MCPServer(...))`. Write plain functions with `@api.method()` / `@server.tool()`.
- Return `Authorized(catalogs, identity=user)` from `authenticate`. In handlers, `current().identity` and `current().params` give the user and URL params.
- Push with `await current().notify(method, params)`. Keep the `current()` object to push later.
- Raise `JsonRpcError(code, msg)` for errors. With `register(..., max_text_inflight=N)`, over N calls at once a JSON-RPC request gets -32000 "server busy"; no limit by default. MCP requests are capped per connection by `max_mcp_inflight` (default 8, None for no limit); past it an MCP request gets "server busy".
- `register(..., resolve=fn)` loads a catalog the first time a client opens it.
- `live.connect_upstream(ws)` + `live.add_upstream(up)` serve another n6k server's catalogs on a connection.
- `LazyCatalog(name)` lists a catalog to clients before `resolve` serves it.
- Details: `packages/python/docs/api.md`.

Breaking, for downstream:

- Requires the extension built from this commit (`n6k_serve_fd` takes `app_capabilities`).
- `authenticate` returning `[]` logs in with no catalogs (text only, or catalogs added later via `on_serve` or `resolve`). Refuse by raising `WsReject` (or any exception).
- `n6k_serve_socket` / `n6k_serve_fd` / `n6k_serve_http` with no catalog names serve none. Pass `'*'` alone for every attached catalog; `'*'` with names is a `BinderException`. (n6kd `server/ws.go`: an empty catalog list now serves nothing.)
- `n6k_connect` returns `(connected, capabilities)`; `capabilities` is the `HELLO_ACK` list. `SELECT connected FROM n6k_connect(...)` is unaffected.
- `bridge(..., permissions=...)` is removed. Pass `policies=[Policy(...)]`.
- `Policy` is `(table, setting, value)`, one `bridge_set_policy` call each; `primary_keys=` is removed. Needs the virtual_catalog_bridge build with `bridge_set_policy`.

  | Old | New |
  |---|---|
  | `Policy(t, "insert", "true", check=c)` | `Policy(t, "insert", c)` |
  | `Policy(t, "update", u, check=c)` | `Policy(t, "update", {"using": u, "check": c})` |
  | `primary_keys={t: cols}` | `Policy(t, "primary_key", list(cols))` |
- `n6k_connect` and the browser client's `registerWebsocket` offer the `jsonrpc` and `mcp` HELLO capabilities; `ATTACH 'n6k://...'` does not.
- `n6k_serve_inflight` returns `(holding BOOLEAN, running BIGINT)` instead of a count that is NULL until the pause applies, and raises on a catalog that is not paused.
- `ping_interval_ms` must be NULL (env default), 0 (disabled) or positive; a negative value raises (`ValueError` from `register`, `BinderException` from `n6k_serve_fd`). It used to mean the env default.
- `n6k_serve_socket` no longer dials `N6K_DB_SOCKET`. It only listens, on `listen =>` or `N6K_LISTEN`, and raises when neither is set.

Changes:

- New: `register(..., resolve=fn)` loads catalogs on first open. When a client opens a catalog that is not served, `fn(live, name)` (sync or async) runs while the client waits. If it served the catalog by the time it returns (e.g. `await live.add_catalog(PendingCatalog(name, attach))` or `add_upstream`), the open succeeds; otherwise it fails with "not found", or with the message `fn` raised. Concurrent opens of one name share one call; every open of a name not served calls `fn` again.
- New: `LazyCatalog(name)`, from `authenticate` or `live.add_catalog`, lists a catalog so clients can find it before anything serves it; opening it calls `resolve`. Needs `register(..., resolve=...)`. Once served it is listed once; while nothing serves it (a failed `resolve`, or its upstream gone) it stays listed and the next open calls `resolve` again. `LazyCatalog(name, rank=N)` holds the name against upstreams of higher rank (no rank: every upstream takes it); `remove_catalog` stops listing it.
- New: upstreams serve another n6k server's catalogs on a connection. `up = await live.connect_upstream(ws, token, on_text=...)` logs in over an open websocket to it (however long it stays pending) and lists its catalogs; `await live.add_upstream(up, rank=N)` serves the ones it wins and returns the rest, and `await live.set_upstream_rank(up, N)` re-ranks it. Who serves a name: a local catalog always wins; otherwise the lowest rank among lazy catalogs and upstreams (then the earliest added), re-decided on every change; a name that changes hands closes its sessions on the loser; `await live.remove_upstream(up)` stops. The upstream's catalog changes reach the client. When it goes away its sessions fail with `upstream for catalog "a" closed: <reason>`, and `await up.wait_closed()` returns the reason; nothing reconnects. Its text frames go to `on_text`, never to the client. A refused or failed login raises `UpstreamError`.
- New: `register`, `serve_ws` and `serve_and_close_connection` take `on_text(ws, text, send_text, **path_params)`, sync or async. Each text frame received once HELLO_ACK is sent runs it on its own task; earlier text is dropped. `await send_text(str)` replies and raises `RuntimeError` when the client's HELLO offered neither `jsonrpc` nor `mcp`, or once the connection ends. A handler's exception is logged and the connection keeps serving. Handlers are cancelled when the connection ends. With `max_text_inflight=N`, more than N at once closes the socket with 1008; by default there is no limit.
- Fixed: a text frame no longer ends the session.
- Fixed: closing a socket no longer deadlocks the server while a provider-backed query on its connection is still running (e.g. one started from `on_serve` with `asyncio.to_thread`). The connection now closes off the event loop, after that query finishes.
- New: `register(..., jsonrpc=JsonRpc())` serves `@api.method()` functions as JSON-RPC 2.0 over text frames. `register(..., mcp=MCPServer(...))` runs one MCP session per connection with the official SDK (extra `n6k-duckdb[mcp]`, mcp 2.x); tools are plain `@server.tool()` functions. `on_text` with either is a `ValueError`.
- New: `authenticate` may return `Authorized(catalogs, identity)`. `n6k_server.context.current()` inside a method, tool or `on_text` gives the connection's path params and identity.
- New: `await current().notify(method, params)` pushes a JSON-RPC notification to the client, JSON-RPC or MCP alike; keep the `Connection` to push later. A no-op (False) when the client's HELLO offered neither `jsonrpc` nor `mcp`, the route has no text handler, or the connection has ended; held for an MCP client mid-handshake.
- With `jsonrpc=` / `mcp=`, over `max_text_inflight` a JSON-RPC request is answered -32000 "server busy" instead of closing the socket. `on_text` still closes it (1008).
- A JSON-RPC result `json.dumps` cannot encode answers -32603 instead of leaving the call unanswered.
- The `mcp` extra requires `mcp>=2.3,<3` and Python 3.10+; on 3.9 it installs nothing.
- New: HELLO capabilities `jsonrpc` and `mcp`. The server grants each one the client offered and `jsonrpc=` / `mcp=` serves.
- With both `jsonrpc=` and `mcp=`, a request for a method registered on `JsonRpc` goes there; every other message goes to MCP, so MCP methods need no list (e.g. `subscriptions/listen`).
- New: `bridge(..., context={"user_id": "abc"})`. Policies reference values as `$user_id`; they are frozen once `bridge` returns.

# 0.11.2

- New: `async with handle.paused("lnk")`. Swap the catalog inside it: DETACH, re-bridge, `CALL n6k_serve_push_invalidate('lnk', 'main')`. Sessions stay open and requests wait.
- Fixed: the teardown hang. Remove `con.close()` from your `connect`; the pump closes `con`.
- Not adding: `replace_catalog`, generation numbers, `detach=`. DETACH yourself after `remove_catalog`.

# 0.11.1

- `register`, `serve_ws` and `serve_and_close_connection` take `on_serve(ws, handle, **path_params)`, sync or async. It runs once serving starts and is cancelled when the connection ends. An exception from it is logged and the connection keeps serving. `handle` is an `n6k_server.pump.LiveServe`:
  - `await handle.add_catalog(name)` serves an already attached catalog. Raises `duckdb.BinderException` if it is not attached, `duckdb.InvalidInputException` if it is already served or pending.
  - `await handle.add_catalog(PendingCatalog(...))` runs its `attach` first, then serves it. Until then clients opening it get "not served" (not a pending wait), and `message` is unused. Raises the attach's exception, `TimeoutError` past `timeout`, or the errors above (`BinderException` if `attach` returned without attaching it).
  - `await handle.remove_catalog(name)` stops serving a served or pending catalog. Clients' open sessions on it close and their running queries fail; clients waiting to open a pending one get an error, and its `attach` still runs but it is not served. Raises `duckdb.InvalidInputException` if it is neither.
  - After the connection ends, `handle.closed` is `True` and both calls raise `RuntimeError`.
- Extension functions `n6k_serve_add_catalog(fd, catalog)` and `n6k_serve_remove_catalog(fd, catalog)` do the same for the `n6k_serve_fd` serving `fd`, with the same errors. They also raise `duckdb.BinderException` for an invalid `fd`, `duckdb.PermissionException` when run through a served client connection, and wait up to 10s for that serve to start, then raise `duckdb.InvalidInputException`.

# 0.11.0

## Breaking

- Requires the n6k extension built from the same commit as this package.
- Protocol version 11 (see `docs/PROTOCOL_CHANGELOG.md`).
- `authenticate` returning the same catalog twice raises `ValueError`. Returning anything other than a name or `PendingCatalog` raises `TypeError`.
- `Provider.invalidate()` and `Provider.unregister()` raise `RuntimeError` once the connection the provider was registered on no longer exists.

## Added

- `authenticate` may return a `PendingCatalog(name, attach, message="", timeout=None)` (from `n6k_server.pump`) in place of a catalog name. The catalog is served immediately, and `attach(cursor)` runs while serving; it must `ATTACH` `name` on the cursor it is given. A client opening the catalog waits until `attach` returns. If `attach` raises, the client gets an error with the exception's text; if it exceeds `timeout`, an error saying the catalog did not become ready in time.
- `authenticate` may take as long as it needs. `register`, `serve_ws` and `serve_and_close_connection` take `hello_pending_message="authenticating"`, the status message the client sees while waiting.
- `n6k_server.provider.release_providers(con)` unregisters every provider on `con`. Call it before closing a connection you registered providers on; `serve_and_close_connection` does this for you.
- `test-server`: catalogs named `slowauth_<ms>` delay authentication, `slowopen_<ms>` delay the catalog becoming ready, and `slowopen_fail_<ms>` fail after the delay.

## Fixed

- Memory leak: a registered provider kept its connection, and every provider on it, in memory after the connection closed.
- A failed `unregister_provider` / `Provider.unregister()` no longer leaves the provider half-removed; it stays registered.
