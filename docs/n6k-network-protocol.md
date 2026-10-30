# n6k Network Protocol

The **on-the-wire** protocol for `TYPE n6k`: the frame format, ops, and lifecycle
a client and server exchange over the `/ws` WebSocket session. This is the spec
for building a compatible server or an alternate client. For the **SQL / host-integration** side — how to `ATTACH`,
authenticate, ride an existing socket, and which functions exist — see
[`n6k-network.md`](n6k-network.md).

Attached catalogs run over a single WebSocket per ATTACH. Every operation —
schema discovery, scans, inserts, DDL, exec, query, and RPC — is one frame
multiplexed on that socket. There is no HTTP data path.

## Wire format

- Tabular responses: Arrow IPC (schema + record-batch messages)
- Frame headers: one self-delimiting **msgpack map** per frame (see below)
- A frame's optional body is **raw Arrow IPC bytes**, appended after the header

## Constants

| Constant | Notes |
|----------|-------|
| `protocol_version` | `N6K_PROTOCOL_VERSION`, the major version: bumped only on incompatible changes. Sent on `HELLO` and `HELLO_ACK`; the server rejects a mismatch (or a missing one) with `HELLO_ERR`, and a client closes on a mismatched `HELLO_ACK`. Additive changes are negotiated as capabilities instead. |
| `default_batch_credits` | Starting stream budget, advertised per session (on `OPEN_ACK`). |
| `open_pending_interval_ms` | Default cadence (overridable per `n6k_serve_fd`) at which the server repeats `OPEN_PENDING` for each parked `ns`, and `HELLO_PENDING` while it authenticates. |
| `open_pending_stall_ms` | A client fails a pending HELLO or OPEN after this long with no pending frame, ACK or ERR for it. There is no overall deadline. |
| `max_concurrent_reqs` | Enforced; past it the server answers `ERR`. |

These, the frame-type numbers, and the op numbers are all generated from the SSOT
(`packages/python/src/n6k_protocol/protocol.py`) into the TypeScript, C++ and
JSON-Schema mirrors. `make protocol-check` fails if a mirror drifts, so treat the
SSOT — not this document — as authoritative if they ever disagree.

## WebSocket /ws — frame format

Each WebSocket **binary** message is exactly one frame: a **msgpack header map**
optionally followed by a **raw body**. There is no length prefix — a single
msgpack map is self-delimiting, so the decoder reads the header map and
everything after it is the body (zero-copy: the body is sliced from the same
buffer, never re-wrapped through msgpack `bin`).

```
 +---------------------------+--------------------------+
 | msgpack header map        | raw body (optional)      |
 | {t, id, op?, ...meta}     | Arrow IPC bytes or empty |
 +---------------------------+--------------------------+
```

| Key | Type | Notes |
|-----|------|-------|
| `t` | int | frame type (table below); present on every frame |
| `id` | int | the `req_id`: client-assigned, monotonic per session (`ns`). Present on request/response frames; absent on connection-scoped HELLO*/PING/PONG, session-lifecycle OPEN*, and unsolicited PUSH |
| `op` | int | REQ/PUSH discriminator (op tables below) |
| …meta | — | per-frame fields (see tables) |

`end_of_stream` is implied by the
`RESP_END`/`ERR` frame types, and an Arrow body is marked by `arrow: true` on
`RESP_CHUNK` (`RESP_SCHEMA` always carries one). Header keys are strictly
JSON-typed (no msgpack `bin`/`ext`) so each decoded header validates directly.

The exact header shape of every frame is generated from the protocol SSOT
(`packages/python/src/n6k_protocol/protocol.py`) into
`packages/python/src/n6k_protocol/schema.json` — a JSON-Schema `oneOf`
discriminated on `t`/`op`. The Python codec (`n6k_protocol.engine.validate_header`)
validates headers against it.

## Frame types

`id` is carried in the header where noted in the §WebSocket frame format table;
the columns below list only the **other** header keys plus the body.

`ns` (integer) is the **session routing key**. It is required (non-zero) on
`OPEN`/`OPEN_ACK`/`OPEN_ERR`, rides every session-scoped frame, and is absent on
the connection-scoped frames (`_NS_EXEMPT` in `protocol.py`). The
client picks a distinct `ns` per catalog session on its `OPEN`, and the server
stamps every session-scoped response with the same `ns` so the client can
demultiplex. There is no default session: `ns` 0 (or absent) names no session,
so a `REQ` without an OPENed `ns` gets `ERR` "unknown session ns=N; send FT_OPEN
first". See [Sessions](#sessions).

| Value | Name | Direction | Header keys + body |
|-------|------|-----------|--------------------|
| 1 | REQ | C→S | `{op, …op-args}` (see REQ ops); INSERT/WRITE_PLAN/RPC_TABLE append a raw Arrow body |
| 2 | RESP_SCHEMA | S→C | `{}`; body = Arrow-IPC schema message |
| 3 | RESP_CHUNK | S→C | Arrow chunk `{arrow:true}` + Arrow body; **or** a list chunk (no body; variants in `schema.json`) |
| 4 | RESP_END | S→C | fields in `schema.json`; terminal frame (implies end-of-stream) |
| 5 | ERR | S→C | `{exception_type, exception_message, retriable?}`; terminal frame |
| 6 | CANCEL | C→S | `{}` (just `id`); early termination of `id` |
| 7 | CREDIT | C→S | `{n}` — refill count for `id` |
| 8 | PUSH | S→C | `{op, …}`; server-initiated, no `id` (see PUSH ops) |
| 9 | PING | either | `{id?}` |
| 10 | PONG | either | `{id?}` |
| 11 | HELLO | C→S | `{token, protocol_version, capabilities?: [..]}` — the socket's handshake. Sent **once per socket**, as the first frame; no `ns`, no `catalog`. `token` is the socket's only credential: a required string, empty when anonymous. |
| 12 | HELLO_ACK | S→C | `{protocol_version, capabilities: [..]}`; connection-scoped (see Capabilities). The socket is authenticated; the client may now send `OPEN`. |
| 13 | HELLO_ERR | S→C | `{exception_type, exception_message, retriable?}`; connection-scoped (no `id`, no `ns`). Sent **in lieu of** HELLO_ACK when the handshake is rejected (bad token, a non-HELLO first frame — "first frame must be FT_HELLO", no HELLO within `HANDSHAKE_TIMEOUT_MS` (`src/n6k_server/include/serve_handshake.hpp`) — "no FT_HELLO within <HANDSHAKE_TIMEOUT_MS>ms", a missing or mismatched `protocol_version`) and in reply to a second HELLO on the same socket. After HELLO_ERR the extension closes the connection; for a HELLO it cannot read or accept, a Unix-socket serve only shuts down its write side, with no close frame. For an `authenticate` reject, see [Host callbacks](n6k-server.md#host-callbacks). |
| 14 | — | — | Unused. |
| 15 | OPEN | C→S | `{ns, catalog, capabilities?: [..]}` — opens a catalog session; sent once per ATTACH. `ns` is required and non-zero. |
| 16 | OPEN_ACK | S→C | `{ns, max_concurrent_reqs, default_batch_credits, capabilities: [..]}` (see Capabilities). The session is usable. |
| 17 | OPEN_ERR | S→C | `{ns, exception_type, exception_message, retriable?}` — the OPEN failed (unserved catalog, duplicate `ns`, `ns` 0, a pending catalog the host reported failed, or a catalog the host could not resolve). Only that session fails; the socket and its other sessions are unaffected. |
| 18 | OPEN_PENDING | S→C | `{ns, message?}` — the catalog is accepted but not ready, or the host is resolving it. Sent instead of `OPEN_ACK` and repeated every `open_pending_interval_ms` until exactly one `OPEN_ACK` or `OPEN_ERR` ends the open. `message` is a human-readable status. |
| 19 | HELLO_PENDING | S→C | `{message?}`; connection-scoped. The server is still authenticating. Sent instead of HELLO_ACK, repeated every `open_pending_interval_ms`, until exactly one HELLO_ACK or HELLO_ERR. |

## REQ ops

A REQ header is `{t:1, id, op, …op-args}`. The op numbers are `OP_*` in
`packages/python/src/n6k_protocol/protocol.py`; each op's header fields, and
whether it carries a raw Arrow-IPC body after the header (`body`), are in its
`_FRAME_HEADERS`, from which `schema.json` is generated.

Ops that stream Arrow send a `RESP_SCHEMA` (no header keys beyond `id`/`ns`,
Arrow body) followed by `RESP_CHUNK{arrow:true}` frames (Arrow body). The list
ops instead send a single `RESP_CHUNK` whose header carries the result array
and no body (variants in `schema.json`).

### Arrow message framing

The bodies of `RESP_SCHEMA` and the `RESP_CHUNK{arrow:true}` frames that follow
it form **one continuous Arrow IPC stream**, split across frames. Concatenating
them in order yields a valid IPC stream: schema message, then one record-batch
message per chunk, then the end-of-stream marker.

Where the boundaries fall is **not** fixed, and a client must not assume it:

- A server that flushes eagerly puts the schema message in `RESP_SCHEMA` and one
  record batch in each `RESP_CHUNK`.
- A server whose Arrow writer flushes lazily (e.g. pyarrow) sends an **empty** `RESP_SCHEMA`
  body and prepends the schema message to the first `RESP_CHUNK`, so that one
  frame carries two complete IPC messages.

Both are conformant. A client must therefore treat each frame payload as a
*stream of IPC messages*: loop decode-header → decode-message, advancing by
`header_size + body_size`, until the payload is consumed, and tolerate an empty
payload as a no-op. `N6kArrowFrameDecoder`
(`src/common/include/n6k_arrow_frame_decoder.hpp`) is the reference for this.

The trailing end-of-stream marker is sent as a final `RESP_CHUNK{arrow:true}`
when the writer emits one. It does **not** consume a credit, and it is not sent
when the stream ends via `CANCEL`.

Streams the server *reads* may contain dictionary-encoded fields and dictionary
batches (replacement and delta), e.g. a pandas Categorical column from a
provider; they decode to their value type (a `dictionary<values=string>` column
becomes `VARCHAR`). Streams the server *writes* never contain dictionary
encoding: `ENUM` columns travel as `VARCHAR`, because nanoarrow's IPC writer
cannot emit dictionary batches. Clients cannot reconstruct the enum-ness.

`_batch_rows` asks the server for a specific number of rows per emitted
`RecordBatch`. It is a **testing affordance**, not a tuning knob: the underscore
marks it as such, and it exists so a suite can force a small scan to arrive as
several chunks and exercise the credit path. Absent or 0, the reference server
leaves batching to DuckDB. Production clients omit it and let the server pick a
batch size per [Chunk sizing](#chunk-sizing); a server is free to treat it as
advisory, but honouring it is what makes the flow-control tests meaningful.

`ALTER_TABLE` `kind` is one of `"add_column"` (details: `{name, type}`),
`"drop_column"` (details: `{name}`), `"rename_column"` (details:
`{old_name, new_name}`). Unsupported alter kinds (column type change,
table rename, defaults, constraints) are rejected client-side with
`BinderException` before the RPC is sent.

## Capabilities

Additive protocol changes do not bump `protocol_version`; they are negotiated
as capabilities, at two levels with the same rule:

- **connect**: `HELLO.capabilities` → `HELLO_ACK.capabilities`, once per socket.
- **open**: `OPEN.capabilities` → `OPEN_ACK.capabilities`, per catalog session.

The sender lists what it can use; the ACK carries the intersection with what
the server supports. An unknown name is ignored, never an error — a newer client
against an older server just gets a shorter list, and decides itself whether a
missing capability is fatal. Each name is defined at exactly one level.

| Name | Level | Meaning |
|------|-------|---------|
| `crossing` | connect | `READ_PLAN` / `WRITE_PLAN` crossing ops are implemented. |
| `jsonrpc` | connect | Text frames carry JSON-RPC 2.0 requests the socket owner answers ([Text frames](#text-frames)). |
| `mcp` | connect | Text frames carry an MCP session (implies `jsonrpc`). |
| `catalog_plans` | open | A `READ_PLAN`, `WRITE_PLAN` or `CREATE_VIEW` plan may name a table `[catalog, schema, table]`, for any catalog a session on this socket has open. |

`catalog_plans` is granted only for a catalog the server serves itself, never one it forwards upstream. A
plan naming a catalog no session on the socket has open fails with `CatalogException`, and one naming a
paused catalog fails with a retriable `TransactionException`. The plan runs in its own session's
transaction (`txn`), so it sees only the other catalogs' committed state.

`jsonrpc` and `mcp` (`APP_HELLO_CAPS`) are granted only when the app serving the
socket provides them, not by the n6k server itself. The SSOT is `HELLO_CAPS` / `APP_HELLO_CAPS` / `OPEN_CAPS` in
`protocol.py`.

This exists because a run-time `NotImplementedException` is too late for some
clients. The DuckDB driver's crossing optimizer rewrites the query
plan *before* any request is sent and cannot fall back once it has, so it must
know the answer at planning time. Capability is read per attached catalog, since
one client can hold several n6k connections of different vintages. On a
host-owned socket (`wsFd` / `wsId`) the socket owner sent HELLO, so it hands the
`HELLO_ACK` capabilities to each catalog that rides the socket.

## PUSH ops

PUSH frames are server-initiated (no `id`). The header is `{t:8, op, …}` —
op + op-specific keys, mirroring the REQ convention.

| Op | Name | Header keys | Semantics |
|----|------|-------------|-----------|
| 12 | CATALOG_INVALIDATED | `{schemas: ["a", "b"]}` | Client should drop its cached table list for each named schema in this WebSocket's catalog and refetch on next access. Unknown schema names are ignored. |
| 20 | CATALOGS_CHANGED | `{catalogs: [..]}` | Connection-scoped. The socket's listed catalog set changed; `catalogs` is the full set, as `CATALOG_LIST` returns. |

Clients that don't recognise a PUSH op MUST ignore the frame (no error).
This keeps the op space additive: a new op can ship server-side without
forcing a protocol-version bump.

## Error reporting

Any op can be terminated by a single `ERR` frame (frame type 5) carrying
`{exception_type, exception_message}` in the header — the same shape DuckDB
uses internally for `ErrorData` serialization. An optional `retriable: true`
flags transient faults the client may safely retry (e.g. a
`TransactionException` write-write conflict).

`exception_type` is the name of the DuckDB `Exception` subclass the
client should throw; the allowed values are `EXCEPTION_TYPES` in
`packages/python/src/n6k_protocol/protocol.py`:

`BinderException`, `ConstraintException`, `ConversionException`,
`DependencyException`, `InvalidInputException`, `InvalidTypeException`,
`OutOfRangeException`, `ParserException`, `SyntaxException`,
`TypeMismatchException`, `CatalogException`, `NotImplementedException`,
`ConnectionException`, `FatalException`, `HTTPException`, `InternalException`,
`InterruptException`, `IOException`, `OutOfMemoryException`,
`PermissionException`, `SequenceException`, `SerializationException`,
`TransactionException`.

Unknown values MUST be treated
as `IOException` by the client.

The client prefixes the reconstructed message with the catalog and op
(`ThrowN6kError`).

## TABLES_LIST row shape

Each entry in the `tables` array:

| Column | Type | Required | Default |
|--------|------|----------|---------|
| `schema` | string | no | `"main"` |
| `name` | string | yes | — |
| `verbs` | string[] | no | `[]` |
| `primary_keys` | string[] | no | `[]` |

`verbs` lists what may be done to the table, in crossing's spelling: any of
`select`, `insert`, `update`, `delete`, `alter`, `drop`. `primary_keys` lists
the primary-key column names in key order. Both are surfaced client-side by
`n6k_table_permissions` on the attached `TYPE n6k` catalog (as `verbs`,
`primary_key`).

The chunk also carries a `schemas` array, one entry per schema:

| Column | Type | Required | Default |
|--------|------|----------|---------|
| `name` | string | yes | — |
| `verbs` | string[] | no | `[]` |

A schema's only verb is `create` (CREATE TABLE / CREATE VIEW in it). A plain
catalog grants it unless attached READ_ONLY; bridge, provider and chained n6k
catalogs grant none.

The client uses `{schema, name}` as the table identity on subsequent
`TABLE_SCHEMA` / `INSERT` frames.

## Lifecycle

1. Client opens `wss://host/ws` and sends `HELLO {token, protocol_version, capabilities}` as its first frame —
   exactly once per socket. The token rides the frame, not the WebSocket
   upgrade. A non-HELLO first frame, no HELLO within `HANDSHAKE_TIMEOUT_MS`, or a second HELLO gets
   `HELLO_ERR`, as does a missing or mismatched `protocol_version`.
2. Server authenticates the token and sends `HELLO_ACK {protocol_version, capabilities}` (`N6K_PROTOCOL_VERSION`), or
   `HELLO_ERR` (the extension's own half-closes; see HELLO_ERR above). Authentication may take as long as it needs: the
   server sends `HELLO_PENDING {message?}` within the client's connect timeout (`WsClientOptions::connect_timeout`, `src/n6k_client/include/ws_client.hpp`)
   and every `open_pending_interval_ms` after, under the same stall rule as
   `OPEN_PENDING` below. The client reports that message rather than a
   bare "socket closed".
3. Per `ATTACH`, the client sends `OPEN {ns, catalog, capabilities}`; the server replies
   `OPEN_ACK {ns, max_concurrent_reqs, default_batch_credits, capabilities}`,
   `OPEN_ERR {ns, ...}`, or `OPEN_PENDING {ns, message?}`. The first reply must
   arrive within the client's connect timeout. After `OPEN_PENDING` the
   server repeats it every `open_pending_interval_ms` and ends the open with
   exactly one `OPEN_ACK` or `OPEN_ERR`; the client waits with no overall
   deadline and fails only after `open_pending_stall_ms` without a frame for that
   `ns`, or on a drop. A pending catalog is listed by `CATALOG_LIST`, and
   `CANCEL {ns, id: 0}` abandons a pending open.
4. Client issues `REQ` frames on an OPENed `ns`; server replies with matching
   `req_id` and `ns`.
5. **Flow control.** Streaming responses start with
   `default_batch_credits` batches of budget. Server decrements on each
   `RESP_CHUNK`; pauses at 0. Client sends `CREDIT{id, n}` (the refill count
   `n` is a header field) to refill.
6. **Cancellation.** Client sends `CANCEL{id}`; server stops producing
   and emits a terminal `RESP_END` with `{"cancelled": true}`, unless the
   request has already ended.
7. **One terminal frame.** Each request gets at most one `RESP_END` or
   `ERR`, and nothing for its `req_id` follows it. It gets none if the
   connection closes first. A `REQ` that reuses a `req_id` still in flight
   is refused with an `ERR` on that `req_id`; the original keeps running and
   still sends its own frames and terminal frame, so the client sees that
   `req_id` end twice.
8. **Drop.** Either side closing fails all in-flight `req_id`s with a
   transport error. The client must re-ATTACH — session resume is not
   supported.

### Sessions

Every catalog session is scoped by `ns` (see the field note above the
frame-type table). One `HELLO` authenticates the socket; each `OPEN {ns,
catalog}` then opens one session, its `OPEN_ACK` is stamped with that `ns`, and
every subsequent `REQ`/`CREDIT`/`CANCEL`/response for it carries the same `ns`.
Sessions are independent — separate `req_id` spaces, and an `OPEN_ERR {ns}`
fails only its own session. `PING`/`PONG` stay connection-scoped and are never
`ns`-stamped. `CANCEL {ns, id: 0}` (req id 0 is reserved) closes a single
session without touching the socket.

The native `n6k_server` extension serves this shape
([`n6k-server.md`](n6k-server.md#sessions-ns)). On the client side, a native
`wsFd` may be shared across several `ATTACH`es; a hub owns the fd and
demultiplexes inbound frames by `ns` ([`n6k-network.md`](n6k-network.md)).

## Text frames

Binary WebSocket frames are n6k. Text frames are not: they belong to whoever owns the socket on
the server side (conventionally JSON-RPC, e.g. MCP tool calls) and n6k never parses them.

- A client may send text only after `HELLO_ACK`. A server may drop earlier text or fail the handshake.
- The server sends text only to a client whose `HELLO` offered `jsonrpc` or `mcp`; a client that
  cannot read text frames offers neither. To such a client the server may send JSON-RPC
  notifications at any time, before the client has sent text; to an MCP client, not between
  `initialize` and `notifications/initialized`.
- A text frame never ends the session (except past the in-flight limit below), and text handling
  never blocks binary traffic.
- Past the reference host's optional text in-flight limit (`max_text_inflight`, off by default), its `jsonrpc`
  handler answers a request with error -32000 ("server busy") and MCP-routed messages still reach
  the MCP session; with plain `on_text` it closes the socket (1008).
- Each text frame is one JSON-RPC 2.0 message; batches are not supported. A request for one of
  the server's plain `jsonrpc` methods is answered by it; with `mcp` granted, every other message,
  responses included, belongs to the MCP session. A client checks `HELLO_ACK` for `jsonrpc` /
  `mcp` before sending; native clients get the list from `n6k_connect`'s `capabilities` column.

## Chunk sizing

One Arrow `RecordBatch` per `RESP_CHUNK`. The reference server sends one DuckDB
`DataChunk` per batch, split further when `_batch_rows` is set.

## HTTP endpoints

None. Every operation — including DDL, via `OP_CREATE_TABLE` and
`OP_ALTER_TABLE` — flows over the `/ws` WebSocket session. A conforming server
needs to serve exactly one endpoint.

The only HTTP a client speaks is OAuth: discovery, token exchange and the
device-code flow, against the authorization server rather than against an n6k
server. See [`n6k-network.md`](n6k-network.md).

## See also

- [`n6k-network.md`](n6k-network.md) — the SQL / host-integration side: `ATTACH`,
  authentication, `wsId`/`wsFd`, and the function registry.
- [`n6k-conformance.md`](n6k-conformance.md) — what a server must provide *beyond*
  this protocol to be driven by the test suites, and how to register it as a target.
