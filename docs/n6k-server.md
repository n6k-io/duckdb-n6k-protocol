# n6k Server Extension (`n6k_server`)

A DuckDB-loadable C++ extension that **serves** a DuckDB catalog over the n6k
protocol — the server-side counterpart to the client `n6k` extension, with no
Python. It reuses the same wire protocol and Arrow IPC framing the client
speaks; this doc covers how to start serving, the transport, and which protocol
ops it answers. For the **on-the-wire** frame format, see
[`n6k-network-protocol.md`](n6k-network-protocol.md). For the **client / SQL**
side — `ATTACH`, auth, and the function registry — see
[`n6k-network.md`](n6k-network.md).

The extension's serving `CALL`s each block the calling thread and serve one or more catalogs.

- **`n6k_serve_socket`** listens on a Unix domain socket and serves every client
  that connects until interrupted.
- **`n6k_serve_fd(<session>, <catalog>... [, named params])`** serves one connection
  on a socket the *caller* already owns, then returns. `<session>` is the UUID
  `n6k_serve_create_session(<fd>)` returned for that socket's file descriptor. This is
  what an embedding host that cannot hand over its WebSocket uses (the Python framework): it
  accepts the WebSocket itself, makes a `socketpair()`, hands over one end and pumps frames
  between the two. The extension reads the client's `FT_HELLO`, runs the host's callbacks (see
  [Host callbacks](#host-callbacks)), sends `HELLO_ACK`, then serves `OPEN`s.
  `request => '<json>'` is passed through to `authenticate` as its `request` argument.
  Pending catalogs (`"pending"` from `authenticate`) are served before they are attached:
  their `OPEN`s get `FT_OPEN_PENDING` until the host calls
  `n6k_serve_open_ready(<session>, 'name')` (after ATTACHing it, from another cursor)
  or `n6k_serve_open_failed(<session>, 'name', 'reason')`. The Python framework drives
  this from a `PendingCatalog` returned by `authenticate`.
  With a `resolve` callback, an `OPEN` for a catalog that is not served parks until
  `resolve` returns (see [Host callbacks](#host-callbacks)).
  The Python framework drives this from `register(..., resolve=...)`.
  Lazy catalogs (`"lazy": {"name": rank}` from `authenticate`, which needs a `resolve`
  callback; a NULL rank loses to every upstream) and
  `n6k_serve_add_lazy_catalog(<session>, 'name', rank => N)` list
  names before anything serves them, so clients can find them; opening one
  calls `resolve`. Once
  served it is listed once; while nothing serves it (a failed resolve, or its
  upstream gone) it is listed as lazy again. `n6k_serve_remove_catalog` stops
  listing it.
  Upstreams forward another n6k server's catalogs. On a stream-framed socket to
  it, `n6k_upstream_hello(<up_fd>, 'token')` sends `HELLO` (waiting through
  `HELLO_PENDING`) and returns `(catalogs, capabilities)`: capabilities from
  `HELLO_ACK`, catalogs from `CATALOG_LIST`. The token may be NULL; a stalled
  reply times out.
  `n6k_serve_add_upstream(<session>, <up_fd>, ['a', ...], rank => N)` (default 0) then
  serves the ones it wins and returns the rest; `n6k_serve_set_upstream_rank(<session>,
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
  goes (EOF, ping timeout, or `n6k_serve_remove_upstream(<session>, <up_fd>)`), its
  opening sessions get `OPEN_ERR` and in-flight requests `FT_ERR` with
  `upstream for catalog "a" closed: <reason>`, and
  `n6k_serve_upstream_status(<session>, <up_fd>)` returns `(open, reason)`.
  `n6k_serve_add_catalog`, `n6k_serve_remove_catalog` and `n6k_serve_pause` refuse an upstream's names.
  While it serves, `n6k_serve_add_catalog(<session>, 'name')` (already attached) and
  `n6k_serve_remove_catalog(<session>, 'name')` change the served set. Removing closes
  that catalog's sessions: in-flight requests get `FT_ERR` and later ones on that
  `ns` get "unknown session". Any change of the listed set sends a connection-scoped
  `PUSH {op: CATALOGS_CHANGED, catalogs}` with the full list. The Python
  framework exposes this as the `LiveServe` handle passed to `on_serve`.
  To swap a served catalog without closing its sessions,
  `n6k_serve_pause(<session>, 'name')` holds new requests for it (CREDIT, CANCEL and
  PING still flow), `n6k_serve_inflight(<session>, 'name')` returns
  `(holding BOOLEAN, running BIGINT)` — whether the pause has applied, and how
  many requests still run against it; drained is `holding AND running = 0`. It
  raises on a catalog that is not paused. And
  `n6k_serve_cancel_inflight(<session>, 'name')` cancels them. After the host
  re-attaches it, `n6k_serve_resume(<session>, 'name')` runs the held requests in
  order. Held-request limits and refusals: `ServeReactor`. Python wraps this as
  `async with handle.paused('name')`.
- **`n6k_serve_http`** listens on a TCP host/port and serves many WebSocket
  clients concurrently until interrupted.
- **`n6k_serve_ws(<session>, '<request>', <catalog>... [, named params])`** serves one
  WebSocket on a TCP socket the host accepted itself and whose HTTP upgrade request it
  already read, then returns. The extension completes the upgrade and owns the
  WebSocket, so no frame passes through the host. See [`n6k_serve_ws`](#n6k_serve_ws).

While a serve runs, these reach it by its session id: the UUID from
`n6k_serve_create_session`, or for `n6k_serve_http`, one the extension makes per accepted
socket and passes to `authenticate`. Unlike an fd number, a session id is never reused, so a
call for a session that ended raises instead of reaching a later one:

- **`n6k_serve_send_text(<session>, 'text')`** sends a JSON-RPC text frame to the client,
  after anything already queued. It raises unless the client offered `jsonrpc` or
  `mcp` and the host serves it (`app_capabilities` names it, or is empty, which accepts
  any offered one).
- **`n6k_serve_close(<session>, code, 'reason')`** closes the client's WebSocket with that
  code after what is already queued, and ends the session. A code outside 1000–4999 is a
  `BinderException`; a reason over 123 bytes is cut on a UTF-8 boundary (`CloseReason`).
- **`n6k_serve_session_stats(<session>)`** returns the session's traffic so far (columns as
  in [Session stats](#session-stats)). It raises at once if no serve has registered `session`.

A session registers only once `HELLO_ACK` is queued. Until then every session control
function except `n6k_serve_session_stats` (the `open_ready` / `open_failed`, catalog, pause,
inflight, upstream, `send_text` and `close` ones) waits up to `RESOLVE_REGISTER_WAIT_MS`
(`AwaitServe`) for it and then raises, so a callback must not call them for its own session
before it returns. A session id that was never created, or whose serve has ended, raises at once.

One DuckDB may serve any number of sockets. The connections a reactor opens to
run client requests are marked served, and every serve function
refuses to bind on them ("not callable from a served connection"), so a client's
SQL cannot reach process file descriptors.

Every serving `CALL` runs the same `ServeReactor`; only the `ServeTransport` behind it differs
(`src/n6k_server/include/serve_transport.hpp`).

## Host callbacks

`n6k_serve_fd`, `n6k_serve_http` and `n6k_serve_ws` take the same named parameters
(defaults in `ServeOptions`, accepted ranges and the `BinderException` outside them in
`BindServeOptions`). Besides the callbacks:

- `ping_interval_ms`: omitted or NULL resolves through `ResolvePingIntervalMs`; 0 disables
  the keepalive.
- `workers`: requests that run at once on the connection; the rest of the window waits in
  arrival order.
- `idle_stream_timeout_ms` (0 is off): a stream parked for credit longer than this is
  answered `FT_ERR` and ended.
- `idle_txn_timeout_ms` (0 is off): a pinned transaction no request has used for this long
  is rolled back, and a later request naming it gets `TransactionException`.

The callbacks are the
names of scalar UDFs the host registered on the database; the extension calls each on
a served connection of its own, from its own threads, with the session id first:

| Parameter | UDF signature | Without it |
|---|---|---|
| `authenticate` | `(session UUID, token VARCHAR, capabilities VARCHAR[], request VARCHAR) -> VARCHAR` | every token is acked; the catalogs are the serve's own |
| `on_text` | `(session UUID, text VARCHAR) -> VARCHAR` reply or NULL | a JSON-RPC request gets -32601 |
| `resolve` | `(session UUID, catalog VARCHAR) -> VARCHAR` error or NULL | an `OPEN` of an unknown catalog fails at once |

`authenticate` returns JSON, every key optional:

```json
{"reject": "reason", "code": 4403}
{"catalogs": ["a"], "pending": {"b": "warming"}, "lazy": {"c": null}, "app_capabilities": ["jsonrpc"]}
```

Any `reject` key rejects: it sends `HELLO_ERR {reason}` (an empty or non-string reason is
`"unauthorized"`) and closes the WebSocket with `code` (default: `AuthResult::code`,
`src/n6k_server/include/host_callbacks.hpp`; a non-integer `code` also gets it, and `code` is
not range-checked). An `authenticate` that raises or returns NULL, malformed JSON, a
non-object root, a `pending` or `lazy` that is neither an object nor null, a
`catalogs` or `app_capabilities` that is not an array of strings, an unknown app capability, a name given twice, an unattached catalog, or lazy catalogs
without `resolve` send `HELLO_ERR "internal error"` and close with 1011. `"catalogs"` replaces the serve's own list. `request` is
`{"uri", "headers"}` from the upgrade on `n6k_serve_http` / `n6k_serve_ws`, and the
host's `request =>` (or NULL) on `n6k_serve_fd`. While `authenticate` runs, the client
gets `FT_HELLO_PENDING {hello_pending_message}` after `hello_pending_first_ms`
(`ServeOptions::hello_pending_first_ms`) and then every `open_pending_interval_ms`.

`on_text` gets text frames only after `HELLO_ACK`, and only from a client that offered
`jsonrpc` or `mcp` where `app_capabilities` is empty or names it. One thread per session calls it, in arrival order, so it should return
quickly and send later replies with `n6k_serve_send_text`. At most `MAX_QUEUED_TEXT`
messages wait for it; past that a request is answered `-32000` (server busy) and a notification is
dropped.

With `resolve`, an `OPEN` for a catalog that is not served, pending or failed parks: it gets
`FT_OPEN_PENDING` every `open_pending_interval_ms`. `resolve` is called once per name, however
many `OPEN`s wait on it, on one thread per session, a name at a time in arrival order, and may
block until it has served the catalog. When it returns, parked `OPEN`s get `OPEN_ACK` if the
catalog is served by then (e.g. it ran `n6k_serve_add_catalog`), are forwarded if an upstream
serves it, else `OPEN_ERR` ("not found", or the returned text or raised message). Nothing is
remembered: the next `OPEN` of a failed name calls it again. At most `MAX_CONCURRENT_REQS`
names wait on `resolve` (queued or running); an `OPEN` for one more is answered `OPEN_ERR`.

Callbacks may run SQL on any other connection to the database, but not on
the one running the serving `CALL`, which is busy until the session ends.

## Session stats

`n6k_serve_ws` returns them in its result row when the session ends, and
`n6k_serve_session_stats(<session>)` returns them while it runs:

| Column | Type | Meaning |
|--------|------|---------|
| `frames_in` | BIGINT | frames read from the client after `HELLO` |
| `bytes_in` | BIGINT | their payload bytes |
| `frames_out` | BIGINT | frames written to the client |
| `bytes_out` | BIGINT | their payload bytes |
| `requests_handled` | BIGINT | `FT_REQ` frames admitted to the worker queue |
| `duration_ms` | BIGINT | since the session started reading `HELLO` |
| `authenticate_ms` | BIGINT | time in `authenticate` (0 without it) |
| `peak_workers` | BIGINT | worker threads started, at most `workers` (`n6k_serve_session_stats` only) |

Payload bytes exclude stream and WebSocket framing.

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
form serves many clients at once and survives any one of them disconnecting.
`max_connections` (default: ixwebsocket's `kDefaultMaxConnections`) caps how many are open at once; ixwebsocket closes
any connection past it as soon as it is accepted. It
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
same; only `n6k_serve_fd` and `n6k_serve_ws`, which end when their one peer disconnects,
return their row.

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
- **No authentication unless given an `authenticate` callback.** Without one this form
  acks every `token`. A Unix socket has file permissions as its
  trust boundary; a TCP port has no equivalent. Bind `127.0.0.1` unless something in
  front is doing authn. There is also no TLS — plain `ws://` only, terminate at a proxy.

## `n6k_serve_ws`

```sql
CALL n6k_serve_ws(<session>, '{"method":"GET","uri":"/","version":"HTTP/1.1","headers":{...}}', 'mydata',
                  authenticate => 'my_auth');
```

For a host that accepts TCP itself (to route by path, or share a port) but can hand over
the socket. The host reads the upgrade request up to and not past its blank line, gives
up the fd (the extension owns and closes it) to
`n6k_serve_create_session`, and passes the returned session and the parsed request. The extension sends the `101` and serves the session on the CALL's
thread. Plain `ws://` only: a TLS connection cannot be
handed over. Catalog arguments and named parameters are as for `n6k_serve_http`, without
`max_connections`.

It returns when the WebSocket closes:

| Column | Type | Meaning |
|--------|------|---------|
| `connected` | BOOLEAN | the upgrade succeeded |
| `requests_handled`, `frames_in`, ... | BIGINT | the session's [stats](#session-stats) |

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
| `OPEN` naming an unserved catalog, with a `resolve` callback | `OPEN_PENDING {ns, message}` while `resolve` runs, then `OPEN_ACK` or `OPEN_ERR` |
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
  values are the WebSocket binary/text opcodes. `0x04` (`STREAM_FRAME_CLOSE`) flows
  only from a Unix-socket serve (`n6k_serve_fd`, `n6k_serve_socket`) to its peer, never
  the other way: msgpack `{code, reason}`,
  the WebSocket close the host performs; it is the last frame. A `HELLO` the serve cannot
  read or accept gets `HELLO_ERR` and a write shutdown instead, with no `CLOSE`. A WebSocket supplies message
  boundaries and types for free; a raw stream socket needs this header. This is the same
  "transport owns length-framing" convention used by the
  [native `wsFd` attach path](n6k-network.md#attaching-an-existing-socket-fd-native)
  (`src/n6k_client/ws_fd_hub.cpp`), so a pump can shuttle frames between this
  socket and a real WebSocket unchanged — which is what `n6k_server.pump` does for
  `n6k_serve_fd`. Over `n6k_serve_http` there is no prefix: one binary message is
  one frame.
- **Handshake.** See [Host callbacks](#host-callbacks). Each
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
  never interleave, and sends the keepalive `FT_PING` `ping_interval_ms` after the previous
  one, or once the queue drains if it is not empty then; outbound traffic does not reset the
  interval. Past `MAX_QUEUED_OUT_BYTES` queued for a client that is not reading, it drops
  the queue and closes the connection with 1008;
- up to **`workers` worker threads**, started only as the queue needs them, running admitted
  `(ns, req_id)`s in arrival order. Each request gets its own DuckDB `Connection`
  (connections aren't thread-safe), its own credit counter, and a cancel flag. A request
  carries a *copy* of its session's `{ns, catalog}`, so it never reaches back into the
  reader's table and survives its session being closed mid-request. A stream out of
  credit **parks**: it leaves its worker and holds no thread until `FT_CREDIT` (or a
  cancel) queues it again, and one that could keep sending yields between frames while
  another request waits. A read in a pinned transaction is materialized, so it releases
  the transaction before it streams. Shutdown drains requests (each closes its
  `Connection`) and joins the workers before the `CALL` returns.

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
- **Auth** is the host's job, through the `authenticate` callback on `n6k_serve_fd`,
  `n6k_serve_http` and `n6k_serve_ws`: it gets the token, decides what that identity
  may see, attaches it, and returns the catalogs or a reject.
  `n6k_serve_socket` is unauthenticated — the local socket is trusted.
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
  `integration_tests/test_n6k_serve_http.py`, `integration_tests/test_n6k_serve_ws.py`,
  `integration_tests/test_n6k_serve_listen.py`.
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
ixwebsocket objects for `n6k_serve_http` and `n6k_serve_ws`, which forces an OpenSSL link even though
the listener only speaks plain `ws://` — the vendored library is built with
`IXWEBSOCKET_USE_TLS`/`IXWEBSOCKET_USE_OPEN_SSL`. A wasm build defines neither
`N6K_SERVER_WITH_WS` nor `n6k_serve_http` / `n6k_serve_ws` and keeps only the Unix-socket forms.
Source lives in `src/n6k_server/` (`README.md` there is the implementation
reference).

## See also

- [`n6k-network-protocol.md`](n6k-network-protocol.md) — the on-the-wire frame
  format, ops, and lifecycle this extension implements.
- [`n6k-conformance.md`](n6k-conformance.md) — the shared test harness.
- [`n6k-network.md`](n6k-network.md) — the client / SQL side: `ATTACH`, auth,
  `wsId`/`wsFd`, and the function registry.
</content>
