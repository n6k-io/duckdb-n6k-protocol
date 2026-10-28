# Architecture

## Extension layout

The repo builds three DuckDB extensions:

- **n6k_client** — network extension. Queries remote servers using Arrow IPC over a WebSocket session. Requires a server implementing the [n6k network protocol](docs/n6k-network-protocol.md).
- **n6k_server** — serves attached catalogs over the n6k protocol.
- **n6k_testing** — test-only functions; never published.

The in-process bridge and provider catalogs (`vcatbridge`,
`vcatprovider`) come from the sibling `duckdb-virtual-catalog` repo.
The only contract between the two repos is SQL: `n6k_table_permissions` /
`n6k_table_describe` read `vcatbridge_table_permissions` / `vcatprovider_table_permissions`
for those catalog types, `n6k_server` finds host stream functions through
`vcatprovider_stream_functions()`, and providers declare primary keys in Arrow schema
metadata under `vcat.primary_keys`. The Python package (`bridge.py`, `provider.py`,
`rpc_stream.py`) drives their SQL API; local builds resolve through
`VIRTUAL_CATALOG_EXT_DIR`.

## n6k_client (Network Extension)

### Attach Flow

```
ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k, token '...')
```

The storage extension parses the URL and dials `n6k://` as `ws://` and `n6ks://` as `wss://`.

### Schema and Table Discovery

Attached catalogs route everything through the `/ws` WebSocket session — `REQ SCHEMA_LIST` for schemas and `REQ TABLES_LIST` for tables. `TABLES_LIST` rows carry `{schema, name, verbs, primary_keys}`, plus each schema's verbs; the extension uses those fields directly to address later ops.

The WebSocket session is the only data path. The native extension makes HTTP
calls only for OAuth; the browser build makes none.

### Reads and writes

Served tables are entries of a `CrossingAttach` (`third_party/crossing`, the
`duckdb-crossing` submodule). Crossing's optimizer pass folds every crossable
operator — filters, projections, limits, sorts, aggregates, joins between
served tables — onto the source and hands `N6kSession` (`src/n6k_client/n6k_source.cpp`)
one `LogicalOperator` fragment per scan and per write. The client renders that
fragment to Substrait JSON (`third_party/crossing/src/render/substrait/substrait_render.cpp`, yyjson, no
protobuf) and ships it:

- **reads**: `REQ READ_PLAN {plan}` → Arrow stream; the reader parks the worker
  between frames through crossing's waker.
- **writes**: `REQ WRITE_PLAN {verb, schema, table, key_columns, set_columns, plan}`
  + Arrow seam rows when the rows were gathered locally, or the shipped plan when
  the rows come from served tables. Keyed writes address rows by the table's
  primary key.
- **transactions**: a non-autocommit DuckDB transaction opens `REQ TXN_BEGIN` on
  first use and ends it with `TXN_COMMIT` / `TXN_ROLLBACK`.
- **CREATE TABLE AS**: JSON schema sent as `OP_CREATE_TABLE`, then rows inserted
  with `REQ INSERT`.

The server (`src/n6k_server/request_handlers.cpp`) decodes the Substrait JSON
into DuckDB's `Relation` API (`third_party/crossing/src/render/substrait/substrait_decode.cpp`)
over the served catalog; a transaction pins one `Connection` per `(ns, txn)`.
A write hands the seam (the Arrow rows, or the decoded plan) to crossing's
`ExecuteWrite`, which builds the INSERT, UPDATE or DELETE as parser nodes and
binds it on that connection. No SQL text is assembled for writes.

### RPC

`n6k_catalog_rpc` (scalar args as JSON) and `n6k_catalog_rpc_table` (Arrow IPC
input + optional JSON args) are global table functions registered at load. An
attached schema also creates a per-name table function lazily on lookup
(`N6kSchemaEntry::GetOrCreateTableFunctionEntry`). Both travel over the WebSocket
session. The server resolves the name to a DuckDB function registered in the
served catalog and calls it.

### WASM

WebSocket sessions (`/ws`) are managed by the JavaScript layer, which respawns
the pump on reconnect. The WASM build has no HTTP
path: every operation travels over that session.

## Build System

The extensions are registered with DuckDB's build system via a shared extension config. Each extension has its own CMake configuration; `src/common/n6k_common.cmake` declares the sources they share.

- n6k links against DuckDB's internal yyjson library and embeds nanoarrow for Arrow IPC handling

CI architectures: `.github/workflows/MainDistributionPipeline.yml` (`exclude_archs`, and the deploy matrix).

## Dependencies

| Dependency | Used by | Purpose |
|------------|---------|---------|
| DuckDB | all | Core database engine and extension APIs |
| yyjson | n6k | JSON serialization for RPC arguments |
| nanoarrow | n6k | Arrow IPC serialization/deserialization |

## Python server framework

The Python side ships as one distribution (`n6k-duckdb`) containing two
importable packages, split by dependency weight:

- **`packages/python/src/n6k_protocol/`** — the wire format and its codec, with
  no FastAPI/Starlette/duckdb dependency: `protocol.py` (the constants, and the
  single source of truth the TypeScript and C++ mirrors are generated from),
  `engine.py` (`pack_frame` / `unpack_frame` / header validation), and
  `filters.py`.
- **`packages/python/src/n6k_server/`** — adapters and consumers; the FastAPI
  adapter is `server_fastapi/register.py` (see `packages/python/docs/api.md`).

### Shape

Python does not implement the protocol. The server is the `n6k_server` DuckDB
extension (C++); Python accepts the WebSocket, reads the client's `FT_HELLO`
token via `n6k_handshake(<fd>)`, decides which catalogs that identity may see,
sends the handshake reply the extension builds (`n6k_hello_pending`, `n6k_reject`,
`n6k_hello_ack`), hands the socket to `CALL n6k_serve_fd(..., hello_acked => true)`,
and shuttles binary frames; text frames go to the app (see `server_fastapi/register.py`).

- **Protocol layer** (`n6k_protocol/`): the frame codec and the constants. No
  FastAPI `app`, no auth, no duckdb imports. A Python client or a wire test uses
  it to speak the protocol directly; the server does not.
- **Pump** (`n6k_server/pump.py`): `socketpair()`, one end's integer fd passed to
  `n6k_serve_fd` in SQL, the other pumped against the WebSocket. In-process
  rather than a spawned `duckdb`, because the reactor's workers must open
  connections on *this* `DatabaseInstance` — that is what lets a table backed by
  Python callbacks (`provider.py`) be served at all.
- **Test server** (`n6k_server/test_server/`): owns a `FastAPI` app,
  Bearer-token auth, request counters, `/debug/*` endpoints, and seeded DuckDB
  fixtures. Calls `register()` to mount the WS route.

### Key design decisions

- **Consumer owns the app.** `register()` takes a FastAPI app and mounts the
  route via APIRouter. No module-global `FastAPI()` in the framework.
- **Native FastAPI routing.** Uses `APIRouter(prefix=path)` directly; no
  custom path matcher or registry dict.
- **The factory returns a database, not a handler.** `connect=` hands back an
  *empty* DuckDB connection with the extensions loaded (sync, async, or an async
  generator for teardown), because the C++ reactor serves *catalogs*, not Python
  objects implementing ops.
- **Identity before session.** `n6k_handshake` reads the `FT_HELLO` token and
  `authenticate` decides what to serve (contract: the `register.py` docstring).
  The credential always rides the frame — never a WS upgrade header or query
  param — and the extension, not Python, parses it.
- **Rejection.** `authenticate` raising sends `n6k_reject`'s FT_HELLO_ERR and
  serves nothing; close codes: `register.py` and `pump.py`.
- **No re-entry.** n6k functions refuse to bind on a reactor's own connections (`n6k::ThrowIfServed`).

### Catalog handling

On WS connect, the C++ driver names its server catalog (the ATTACH `catalog`
option, else the alias) two ways: `?catalog=<name>` on the dial URL for a normal
attach, and the same name in the `FT_OPEN` frame it sends after the socket's
`FT_HELLO`. A host may read `?catalog=` from the URL (the test server's root
mount does) to decide what to ATTACH, and the reactor reads the OPEN's `catalog`
to bind the session's namespace.
