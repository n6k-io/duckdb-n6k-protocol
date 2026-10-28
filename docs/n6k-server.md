# n6k Server Extension (`n6k_server`)

A DuckDB-loadable C++ extension that **serves** a DuckDB catalog over the n6k
protocol — the server-side counterpart to the client `n6k` extension, with no
Python. It reuses the same wire protocol and Arrow IPC framing the client
speaks; this doc covers how to start serving, the transport, and which protocol
ops it answers. For the **on-the-wire** frame format, see
[`n6k-network-protocol.md`](n6k-network-protocol.md). For the **client / SQL**
side — `ATTACH`, auth, and the function registry — see
[`n6k-network.md`](n6k-network.md).

The extension exposes three `CALL`-able entry points, all of which block the
calling thread and serve one or more catalogs.

- **`n6k_serve_socket`** listens on a Unix domain socket and serves every client
  that connects until interrupted.
- **`n6k_serve_fd(<fd>, <catalog>... [, named params])`** serves one connection
  on a socket the *caller* already owns and passes by file descriptor, then returns. This is
  what an embedding host uses: it accepts the WebSocket itself, makes a
  `socketpair()`, and hands over one end. It assumes the client's `FT_HELLO` was
  already read (by `n6k_handshake`), sends `HELLO_ACK`, then serves `OPEN`s.
  `hello_acked => true` means the host already sent `HELLO_ACK` itself
  (`n6k_hello_ack`), so `n6k_serve_fd` does not.
  `ping_interval_ms`: omitted or NULL uses `$N6K_PING_INTERVAL` (`ResolvePingIntervalMs`), 0 disables the
  keepalive, a negative value is a `BinderException`.
  `capabilities => [...]` passes on the list `n6k_handshake` returned, so the
  `HELLO_ACK` carries the negotiated intersection; omitted, it carries none.
  `app_capabilities => ['jsonrpc', 'mcp']` names the `APP_HELLO_CAPS` the host
  serves on the socket's text frames; the others are never granted. An unknown
  name is a `BinderException`.
  `pending => MAP {'name': 'status'}` also serves catalogs that are not attached
  yet: their `OPEN`s get `FT_OPEN_PENDING` until the host calls
  `n6k_serve_open_ready(<fd>, 'name')` (after ATTACHing it, from another cursor)
  or `n6k_serve_open_failed(<fd>, 'name', 'reason')`. The Python framework drives
  this from a `PendingCatalog` returned by `authenticate`.
  `resolve => true` parks an `OPEN` for a catalog that is not served, pending
  or failed: it gets `FT_OPEN_PENDING` (every `open_pending_interval_ms`) and the
  host gets one `STREAM_FRAME_HOST` `{"resolve": "name"}` per name, however many
  `OPEN`s wait on it. The host settles it with
  `n6k_serve_resolved(<fd>, 'name', NULL)` once it has done what it will (e.g.
  `n6k_serve_add_catalog`, which ACKs the parked `OPEN`s at once), or
  `n6k_serve_resolved(<fd>, 'name', 'reason')` on failure. Parked `OPEN`s then get
  `OPEN_ACK` if the catalog is served, else `OPEN_ERR` ("not found", or the
  reason). Nothing is remembered: the next `OPEN` of a failed name asks again.
  The Python framework drives this from `register(..., resolve=...)`.
  `lazy => MAP {'name': rank, ...}` (with `resolve => true`; a NULL rank loses to
  every upstream) and `n6k_serve_add_lazy_catalog(<fd>, 'name', rank => N)` list
  names before anything serves them, so clients can find them; opening one
  resolves it as above. Once
  served it is listed once; while nothing serves it (a failed resolve, or its
  upstream gone) it is listed as lazy again. `n6k_serve_remove_catalog` stops
  listing it.
  Upstreams forward another n6k server's catalogs. On a stream-framed socket to
  it, `n6k_upstream_hello(<up_fd>, 'token')` sends `HELLO` (waiting through
  `HELLO_PENDING`) and returns `(catalogs, capabilities)`: capabilities from
  `HELLO_ACK`, catalogs from `CATALOG_LIST`. The token may be NULL; a stalled
  reply times out.
  `n6k_serve_add_upstream(<fd>, <up_fd>, ['a', ...], rank => N)` (default 0) then
  serves the ones it wins and returns the rest; `n6k_serve_set_upstream_rank(<fd>,
  <up_fd>, N)` re-ranks it. A local catalog always wins a name; otherwise the
  lowest rank among lazy names and upstreams that report it wins (then the
  earliest added), re-decided on every change, including names an upstream
  reports later. A name that changes hands closes its sessions on the loser:
  later requests on them get "unknown session". Every frame of
  a session opened on one goes to the upstream unmodified, and every frame the
  upstream stamps with an `ns` comes back unmodified; its `CATALOGS_CHANGED`
  updates the served set. The upstream is this connection's alone, so `ns` and
  request ids need no rewriting. The reactor PINGs it and drops it after
  `OPEN_PENDING_STALL_MS` past the ping interval with nothing received. When it
  goes (EOF, ping timeout, or `n6k_serve_remove_upstream(<fd>, <up_fd>)`), its
  opening sessions get `OPEN_ERR` and in-flight requests `FT_ERR` with
  `upstream for catalog "a" closed: <reason>`, and
  `n6k_serve_upstream_status(<fd>, <up_fd>)` returns `(open, reason)`.
  `n6k_serve_add_catalog`, `n6k_serve_remove_catalog` and `n6k_serve_pause` refuse an upstream's names.
  While it serves, `n6k_serve_add_catalog(<fd>, 'name')` (already attached) and
  `n6k_serve_remove_catalog(<fd>, 'name')` change the served set. Removing closes
  that catalog's sessions: in-flight requests get `FT_ERR` and later ones on that
  `ns` get "unknown session". Any change of the listed set sends a connection-scoped
  `PUSH {op: CATALOGS_CHANGED, catalogs}` with the full list. The Python
  framework exposes this as the `LiveServe` handle passed to `on_serve`.
  To swap a served catalog without closing its sessions,
  `n6k_serve_pause(<fd>, 'name')` holds new requests for it (CREDIT, CANCEL and
  PING still flow), `n6k_serve_inflight(<fd>, 'name')` returns
  `(holding BOOLEAN, running BIGINT)` — whether the pause has applied, and how
  many requests still run against it; drained is `holding AND running = 0`. It
  raises on a catalog that is not paused. And
  `n6k_serve_cancel_inflight(<fd>, 'name')` cancels them. After the host
  re-attaches it, `n6k_serve_resume(<fd>, 'name')` runs the held requests in
  order. Held-request limits and refusals: `ServeReactor`. Python wraps this as
  `async with handle.paused('name')`.
- **`n6k_serve_http`** listens on a TCP host/port and serves many WebSocket
  clients concurrently until interrupted.

These helpers let a host authenticate before it builds anything, on the same fd:

- **`n6k_handshake(<fd>) -> (token, capabilities)`** reads the client's
  `FT_HELLO` and returns its token and offered capabilities. It sends nothing on
  success; a failure (`ReadHello`) gets `HELLO_ERR` and raises.
- **`n6k_hello_pending('status') -> frame BLOB`** returns one `FT_HELLO_PENDING`
  for the host to send between `n6k_handshake` and `HELLO_ACK` / `HELLO_ERR`. The
  Python framework repeats it while `authenticate` runs (`pump.py`).
- **`n6k_reject(<reason>) -> frame BLOB`** returns `HELLO_ERR {reason}` for the
  host to send before it closes the socket.
- **`n6k_hello_ack([capabilities => [...]] [, app_capabilities => [...]]) -> frame BLOB`**
  returns the `HELLO_ACK` `n6k_serve_fd` would send, for the host to send itself
  before `n6k_serve_fd(..., hello_acked => true)`. Sending it from the host orders
  it before any text frame the host sends.

These return the frame (no stream header) rather than writing the fd, so the host
puts every handshake reply on the socket itself.

One DuckDB may serve any number of sockets. The connections a reactor opens to
run client requests are marked served, and every serve and handshake function
refuses to bind on them ("not callable from a served connection"), so a client's
SQL cannot reach process file descriptors.

All three run the same `ServeReactor`; only the `ServeTransport` behind it differs
(`src/n6k_server/include/serve_transport.hpp`).

## `n6k_serve_socket`

```sql
CALL n6k_serve_socket('<catalog>' [, '<catalog>' ...]);
CALL n6k_serve_socket('*');   -- every attached non-system catalog
CALL n6k_serve_socket();      -- no catalogs
```

`n6k_serve_socket` serves exactly the named **already-attached** catalogs — none
when none are named. Getting them open is the caller's job — `n6k_serve_socket`
does not `ATTACH` anything. A lone `'*'` discovers every attached catalog except
the system and temp ones (`'*'` with names is a `BinderException`); the set is
frozen at bind time, so a later `ATTACH` is **not** picked up by a running serve.
The same rule applies to `n6k_serve_fd` and `n6k_serve_http`.

`'*'` also excludes the startup in-memory database (`AttachedUserCatalogs` in
`src/common/serve_bind_common.cpp`). `'*'` with nothing attached at all is a
`BinderException`. Naming catalogs explicitly overrides all of this.
```sh
N6K_LISTEN=/run/n6k.sock duckdb mydata.duckdb -c "CALL n6k_serve_socket('mydata')"
duckdb mydata.duckdb -c "CALL n6k_serve_socket('mydata', listen => '/run/n6k.sock')"
```

`listen => '<path>'` (or `N6K_LISTEN` when the parameter is absent) names the Unix
socket the call listens on. It serves every client that connects, concurrently, each
on its own reactor over the one `DatabaseInstance` — the `n6k_serve_http` model over
a Unix socket. A client that leaves, including one that connects and closes before
`FT_HELLO`, does not end the call; only an interrupt does, so, as with
`n6k_serve_http`, the status row is declared but not returned:

| Column | Type | Meaning |
|--------|------|---------|
| `connected` | BOOLEAN | always true |
| `requests_handled` | BIGINT | count of `FT_REQ` frames processed, all clients |
| `socket` | VARCHAR | the Unix socket path that was served |

A stale socket file is replaced, a path a live server accepts on is refused, the
file is mode 0600, and it is unlinked when the call ends.

Several catalogs share the one socket, each addressed by its own session:

```sql
ATTACH 'sales.duckdb' AS sales (READ_ONLY);
ATTACH 'ops.duckdb'   AS ops;
CALL n6k_serve_socket('sales', 'ops');
```

For a read-only surface, attach the catalog read-only yourself before serving.

Every catalog name must resolve to an attached catalog, no name may repeat, and
a listen path must be set (non-empty, and short enough for `sun_path`) — all validated
at bind time, raising a `BinderException` otherwise (`CALL n6k_serve_socket(NULL)`,
`CALL n6k_serve_socket('nope')`, and `CALL n6k_serve_socket('a', 'a')` all fail before
serving).

## `n6k_serve_http`

```sql
CALL n6k_serve_http('<host>', <port> [, '<catalog>' ...]);
CALL n6k_serve_http('0.0.0.0', 7823, '*');   -- every attached non-system catalog
```

Binds `host:port` and accepts WebSocket clients; each n6k frame is one binary
message, with **no** length prefix (WebSocket already delimits messages). Catalog
arguments are validated by the same helper as `n6k_serve_socket`
(`src/common/serve_bind_common.cpp`), so the NULL / not-attached / duplicate
errors are identical. `N6K_LISTEN` is not read. A port outside 1–65535 is a
bind error.

Every accepted connection gets its own `ServeReactor` on its own thread, so this
form serves many clients at once and survives any one of them disconnecting. It
blocks until interrupted (`Ctrl-C`), then stops the listener and drains every
reactor.

Its declared return schema is:

| Column | Type | Meaning |
|--------|------|---------|
| `connections` | BIGINT | WebSocket clients accepted over the run |
| `requests_handled` | BIGINT | count of `FT_REQ` frames processed, all clients |
| `url` | VARCHAR | the `ws://host:port` that was served |

**That row is not observable in practice.** The serve loop's only exit is the
interrupt, and DuckDB aborts an interrupted statement instead of materialising
its result — the same thing happens to any long query you `Ctrl-C`. The schema is
declared because a table function must declare one. `n6k_serve_socket` is the
same; only `n6k_serve_fd`, which ends when its one peer disconnects, returns its row.

```sh
duckdb mydata.duckdb -c "CALL n6k_serve_http('127.0.0.1', 7823, 'mydata')"
# from another duckdb:
#   ATTACH 'n6k://127.0.0.1:7823' AS x (TYPE n6k);
```

Two things follow from all clients sharing one `DatabaseInstance`:

- **One catalog set, shared.** `n6k_serve_socket` shares one instance the same way. Only a
  `n6k_serve_fd` host can give a client its own `DatabaseInstance`, by opening a fresh
  `duckdb.connect()` per WebSocket before handing over its fd. Here concurrent writers
  meet DuckDB's MVCC; transaction conflicts come back as `FT_ERR` with
  `"retriable":true`.
- **No authentication.** This form reads `FT_HELLO` itself, ignores its `token`,
  and acks. A Unix socket has file permissions as its
  trust boundary; a TCP port has no equivalent. Bind `127.0.0.1` unless something in
  front is doing authn. There is also no TLS — plain `ws://` only, terminate at a proxy.

## Sessions (`ns`)

The wire carries a session key, `ns`, on every session-scoped frame, so one
socket carries many catalog sessions (`n6k-network-protocol.md`). After the
socket's one `HELLO`/`HELLO_ACK`, a client opens a session with
`FT_OPEN {ns, catalog}`; every response for it is stamped with that `ns`, and
each session owns its own `req_id` space — two sessions may both have `req_id` 1
in flight. The same rules apply however many catalogs are served; there is no
default session.

| Client frame | Server response |
|---|---|
| `OPEN {ns, catalog}` naming a served catalog | `OPEN_ACK {ns, max_concurrent_reqs, default_batch_credits, capabilities}` — session open |
| `OPEN` naming an unserved catalog | `OPEN_ERR {ns}`, listing what *is* served |
| `OPEN` naming an unserved catalog, with `resolve => true` | `OPEN_PENDING {ns, message}` while the host resolves it, then `OPEN_ACK` or `OPEN_ERR` on `n6k_serve_resolved` |
| `OPEN` naming a pending catalog | `OPEN_PENDING {ns, message}` now and every `open_pending_interval_ms`, then `OPEN_ACK` on `n6k_serve_open_ready` or `OPEN_ERR` on `n6k_serve_open_failed` |
| `OPEN` naming a pending catalog that failed | `OPEN_ERR {ns}` with the host's reason |
| `OPEN` for an `ns` already open or pending | `OPEN_ERR {ns}` — duplicate |
| `OPEN` with `ns` 0 | `OPEN_ERR` |
| a second `HELLO` | `HELLO_ERR`, socket closes |
| `REQ` for an un-OPENed `ns` (or none) | `FT_ERR` — "unknown session ns=N; send FT_OPEN first" |
| `CANCEL {ns, id: 0}` | closes that session (what a client sends on `DETACH`) |
| `PING` | `PONG`, never `ns`-stamped — keepalive is connection-scoped |
| `REQ {op: CATALOG_LIST}` (no `ns`) | `RESP_CHUNK {catalogs}` (order: `ListedCatalogs`) + `RESP_END` |

## Transport

- **`N6K_LISTEN`** (env) or `listen =>` names a Unix domain socket (`AF_UNIX`,
  `SOCK_STREAM`), read once at bind time, that `n6k_serve_socket` binds and listens
  on itself; peers dial in, one connection per client.
- **`n6k_serve_http`** is the **server** at both layers: it binds and listens, and
  ixwebsocket performs the HTTP `Upgrade` handshake per client.
- **Framing.** Over the Unix socket every message uses n6k stream framing,
  `[4-byte big-endian length][1-byte type][payload]`, where the length counts the
  payload bytes. Type `0x02` (`STREAM_FRAME_N6K`) is an n6k frame (msgpack header + optional Arrow
  body), `0x01` (`STREAM_FRAME_JSONRPC`) a JSON-RPC message; any other type closes the connection. The
  values are the WebSocket binary/text opcodes. `0x03` (`STREAM_FRAME_HOST`) flows
  only from `n6k_serve_fd` to its host, never to a client: a JSON host event
  (`{"resolve": name}`). A WebSocket supplies message
  boundaries and types for free; a raw stream socket needs this header. This is the same
  "transport owns length-framing" convention used by the
  [native `wsFd` attach path](n6k-network.md#attaching-an-existing-socket-fd-native)
  (`src/n6k_client/ws_fd_hub.cpp`), so a pump can shuttle frames between this
  socket and a real WebSocket unchanged — which is what `n6k_server.pump` does for
  `n6k_serve_fd`. Over `n6k_serve_http` there is no prefix: one binary message is
  one frame.
- **Handshake.** The client's first frame is `HELLO {token, protocol_version,
  capabilities}`. `n6k_serve_socket` and `n6k_serve_http` read it themselves,
  ignore the token, and send `HELLO_ACK {protocol_version, capabilities}`. With
  `n6k_serve_fd` the host reads it first via `n6k_handshake` and either rejects
  (sends `n6k_reject`'s frame) or sends `n6k_hello_ack`'s frame and calls
  `n6k_serve_fd(..., hello_acked => true)`. Each
  `OPEN` then gets an `OPEN_ACK {ns, ..., capabilities}` — see
  [Sessions](#sessions-ns).

## Architecture

A naive read→handle loop would deadlock under backpressure (a streaming op
blocks waiting for an `FT_CREDIT` it can't read while busy). The reactor uses
three roles:

- a **reader** (the `CALL`'s thread) reads stream-framed frames and routes
  them by `ns` — `FT_OPEN` opens a session; `FT_REQ` spawns a worker;
  `FT_CREDIT`/`FT_CANCEL` signal the matching worker; `FT_PING`→`FT_PONG`. It
  polls with a short timeout so `Ctrl-C` (`ClientContext::IsInterrupted`) ends
  the serve loop even against an idle peer. The session table is **reader-owned**:
  nothing else reads or writes it, which is why it needs no lock;
- a **writer thread** serializes all outbound frames onto the socket so frames
  never interleave;
- one **worker thread per in-flight `(ns, req_id)`**, each with its own DuckDB
  `Connection` (connections aren't thread-safe), its own credit counter, and a
  cancel flag. A worker gets a *copy* of its session's `{ns, catalog}`, so it
  never reaches back into the reader's table and survives its session being
  closed mid-request. Shutdown drains workers (each closes its `Connection`)
  before the `CALL` returns.

## Op coverage

The extension answers the REQ ops dispatched in `HandleRequest`
(`src/n6k_server/request_handlers.cpp`):

| Op | Status |
|---|---|
| `SCHEMA_LIST` | ✅ schemas of the session's catalog (system schemas excluded) |
| `TABLES_LIST` | ✅ tables/views + verbs + primary keys, schema verbs (optional schema filter) |
| `TABLE_SCHEMA` | ✅ Arrow schema (`SELECT … LIMIT 0`) |
| `QUERY` | ✅ raw passthrough SQL, streamed |
| `READ_PLAN` | ✅ Substrait plan decoded over the served catalog (and, under `catalog_plans`, the socket's other open catalogs), streamed |
| `WRITE_PLAN` | ✅ crossing write |
| `TXN_BEGIN` / `TXN_COMMIT` / `TXN_ROLLBACK` | ✅ one pinned `Connection` per `(ns, txn)` |
| `EXEC` | ✅ → `{"rowcount":N}` |
| `INSERT` | ✅ Arrow IPC body → `INSERT`, → `{"rowcount":N}` |
| `CREATE_TABLE` / `ALTER_TABLE` | ✅ → `{"ok":true}` (ALTER: add/drop/rename column) |
| `CREATE_VIEW` / `DROP_VIEW` | ✅ → `{"ok":true}`; the body is a Substrait plan decoded over the served catalog |
| `RPC_SCALAR` / `RPC_TABLE` | ✅ calls a host registration, or a table function or macro resolved in the served DuckDB |

Streaming ops (`QUERY`, `READ_PLAN`, `RPC_*`) follow the protocol's credit and
cancel rules ([Lifecycle](n6k-network-protocol.md#lifecycle)). Errors are typed
`ERR` frames, with `retriable` set per `IsRetriableExceptionType` — the same [error reporting](n6k-network-protocol.md#error-reporting)
contract the client expects.

## Host-backed RPC

An RPC that resolves through the catalog can only be what SQL can express, and
no SQL construct produces a relation that never ends. A subscription — a
generator in the host process that yields a batch every so often and may run
forever — therefore has no catalog form at all.

So the host gives it one: a table function whose rows come from the generator.
DuckDB's Python API cannot register a table function (`create_function` is
scalar-only), so the host registers three scalar UDFs and asks the extension to
build the catalog entry over them:

```sql
SELECT vcatprovider_create_stream_function(catalog, schema, name, open_udf, next_udf, close_udf);
SELECT vcatprovider_drop_stream_function(catalog, schema, name);
```

`vcatprovider` is in the sibling `duckdb-virtual-catalog` repo. The server only
recognises such an entry — by finding it in `vcatprovider_stream_functions()` — to
forward the generator's Arrow bytes untouched.
`register_rpc_streams()` loads `vcatprovider` for you.

That entry is an ordinary table function, so it needs nothing passed to the
serving call and it is callable as plain SQL on the host's own connection:

```sql
SELECT * FROM db.main.random_walk(100);   -- no client, no reactor
```

Because a catalog entry lives in the database, every connection on that
`DatabaseInstance` sees it. The UDF *names* carry a unique suffix so two
connections sharing an instance do not collide when registering (`create_function`
raises on a duplicate name), but nothing looks that suffix up.

For a stream function the worker drives the UDFs instead of building SQL:

| UDF | Signature | Meaning |
|---|---|---|
| open | `(handle, function, args_json, input_ipc BLOB) -> BLOB` | starts the call; returns the Arrow IPC **schema message** sent as `RESP_SCHEMA` |
| next | `(handle) -> BLOB` | one Arrow IPC record batch, or `NULL` once the source is exhausted |
| close | `(handle) -> BLOB` | runs the source's teardown; returns the trailing end-of-stream bytes |

The UDFs speak Arrow IPC because the host must declare its result schema up
front anyway — the wire sends `RESP_SCHEMA` before the first batch — and once it
has, the bytes cross the boundary once instead of being decoded into DuckDB
vectors only to be re-encoded on the way out. `RPC_TABLE`'s pushed rows reach
`open` as the raw request body, so they are never staged through a temp view.

Each batch is pulled *before* its credit is acquired, so a source that paces
itself in wall-clock time keeps its cadence rather than starting its next
interval only once the client's credit arrives. `close` runs on every exit —
end of stream, `CANCEL`, or an error mid-stream — which is what lets a
subscription unsubscribe.

`packages/python/src/n6k_server/rpc_stream.py` is the Python half:
`register_rpc_streams(con, catalog=…, registry={name: (fn, schema)})`, where
`fn` is either a plain callable returning one `pa.Table` or a `StreamingRpc`
wrapping an async iterator. It creates the UDFs and one stream function per
name; see its docstring.

### Caveats

- **RPC** (`RPC_SCALAR`/`RPC_TABLE`) resolution: `ResolveRpcTarget`
  (`src/n6k_server/request_handlers.cpp`). What is callable is whatever the host
  put in the database.
- **Auth** is the host's job and only on the `n6k_serve_fd` path: the host reads
  the token with `n6k_handshake`, decides what that identity may see, and either
  sends `n6k_reject`'s frame or attaches those catalogs, sends `n6k_hello_ack`'s
  frame and calls `n6k_serve_fd`.
  `n6k_serve_socket` and `n6k_serve_http` are unauthenticated — the local socket
  is trusted.
- **`QUERY`** is raw passthrough SQL and is *not* catalog-qualified, so a query
  could reach another attached catalog. Every other
  op qualifies as `"catalog"."schema"."table"` and so stays inside its session,
  but `QUERY` does not — serving several catalogs makes that far more visible,
  since a `QUERY` on one session can read another's data. `EXEC` is the same. The
  served catalog list gates which sessions can open, not what that session's SQL
  can reach. Use a `READ_ONLY` / restricted connection
  if you need hard isolation.

## Testing

- **Integration:** `integration_tests/test_reactor_wire.py` (`n6k_serve_fd`),
  `integration_tests/test_n6k_serve_http.py`, `integration_tests/test_n6k_serve_listen.py`.
- **Binder contract:** `test/sql/n6k_server.test` covers bind-time errors only —
  the serving path can't go in sqllogictest because the scan blocks until
  interrupted. Note there is deliberately no `CALL n6k_serve_socket()` or
  `('*')` case there: both clear every bind check, so a developer shell that
  exports `N6K_LISTEN` would reach the scan and hang the suite.

```sh
./build/release/test/unittest test/sql/n6k_server.test
```

## Build

Built alongside `n6k_client` via the root `extension_config.cmake` (platform conditions there).
The extension links `duckdb_yyjson` and compiles the vendored nanoarrow sources
(for Arrow IPC encode/decode). Native builds additionally compile in the vendored
ixwebsocket objects for `n6k_serve_http`, which forces an OpenSSL link even though
the listener only speaks plain `ws://` — the vendored library is built with
`IXWEBSOCKET_USE_TLS`/`IXWEBSOCKET_USE_OPEN_SSL`. A wasm build defines neither
`N6K_SERVER_WITH_WS` nor `n6k_serve_http` and keeps only the Unix-socket form.
Source lives in `src/n6k_server/` (`README.md` there is the implementation
reference).

## See also

- [`n6k-network-protocol.md`](n6k-network-protocol.md) — the on-the-wire frame
  format, ops, and lifecycle this extension implements.
- [`n6k-conformance.md`](n6k-conformance.md) — the shared test harness.
- [`n6k-network.md`](n6k-network.md) — the client / SQL side: `ATTACH`, auth,
  `wsId`/`wsFd`, and the function registry.
</content>
