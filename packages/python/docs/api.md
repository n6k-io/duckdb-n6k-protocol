
# n6k-duckdb

The distribution is `n6k-duckdb`; it installs two importable packages —
`n6k_protocol` (wire format, frame codec, filter helpers) and
`n6k_server` (bridge, provider, byte pump, FastAPI adapter).

Two ways to plug Python into DuckDB:

- **Bridge** — mirror tables from one DuckDB connection to another with
  per-table policies. No network, no server.
- **Provider** — back a DuckDB schema with a Python class. DuckDB issues
  `SELECT` / `INSERT` / `UPDATE` / `DELETE`; your code answers.

An optional extra mounts a FastAPI server that speaks the n6k network protocol.

## Install

```bash
pip install "n6k-duckdb[duckdb]"        # bridge + provider
pip install "n6k-duckdb[test-server]"   # + FastAPI server framework
```

---

## Bridge

```python
bridge(
    source: duckdb.DuckDBPyConnection,
    target: duckdb.DuckDBPyConnection,
    name: str,
    *,
    source_catalog: str,
    policies: Sequence[n6k_server.bridge.Policy],
    context: dict[str, object] | None = None,
    lock: threading.Lock | None = None,
) -> str
```

Mirror tables from `source` into a new catalog `name` on `target`.

One bridge is one attached catalog: `name` must not exist on `target` yet, and
`DETACH name` (see `unbridge`) tears it down. Each granted source schema lands
in the target schema of the same name.

## Args

- `source`, `target` — DuckDB connections.
- `name` — catalog created on the target.
- `source_catalog` — where the source tables live.
- `policies` — `Policy` settings, one per `(table, setting)`. `setting` is a verb
  (`select`, `insert`, `update`, `delete`, `alter`, `drop`) or `primary_key`.
  A verb's value is `True` or a predicate; `update` also takes
  `{"using": ..., "check": ...}`. `primary_key` takes a column list and
  overrides PK auto-discovery; without a key, `UPDATE` and `DELETE` are rejected.
- `context` — `{name: value}` that policies reference as `$name`. Values are
  frozen once `bridge` returns.
- `lock` — held while `bridge` registers on `source`.

Returns the bridge id. Raises `duckdb.Error` for an empty or invalid policy.

## Example

```python
import duckdb
from n6k_server.bridge import Policy, bridge

cfg = {"allow_unsigned_extensions": "true"}
source = duckdb.connect(config=cfg)
source.sql("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR, tenant_id INTEGER)")

target = duckdb.connect(config=cfg)
bridge(source, target, "app", source_catalog="memory", policies=[
    Policy("main.users", "select", "true"),
    Policy("main.users", "insert", "true"),
    Policy("main.users", "update", "true"),
    Policy("main.users", "delete", "true"),
])

target.sql("SELECT * FROM app.main.users")
```

Restricted to one tenant:

```python
bridge(source, target, "app", source_catalog="memory", policies=[
    Policy("main.users", "select", "tenant_id = 42"),
    Policy("main.users", "insert", "tenant_id = 42"),
    Policy("main.users", "update", "tenant_id = 42"),
    Policy("main.users", "delete", "tenant_id = 42"),
])
```

Per-user rows via a context value:

```python
bridge(source, target, "app", source_catalog="memory", context={"user_id": "abc"}, policies=[
    Policy("main.docs", "select", "owner_id = $user_id"),
    Policy("main.docs", "insert", "owner_id = $user_id"),
])
```


---

## Provider

Provider-backed dynamic virtual tables.

A `Provider` describes a namespace of virtual tables. Bind it 1:1 to
`(connection, catalog, schema_name)` via `register_provider()`. DuckDB
caches what the provider returns; `invalidate_provider_tables()` bumps a
version counter so the next lookup refills.

The extension sees one provider per catalog whose table names are `schema.table`;
`register_provider` multiplexes every schema's `Provider` behind one UDF set per
catalog and splits the qualified name on the way in.

Primary keys (required for UPDATE/DELETE) live in schema-level metadata
keyed by `PK_METADATA_KEY`, comma-separated.

## Example

```python
import asyncio, duckdb, pyarrow as pa
from n6k_server.provider import Provider, PK_METADATA_KEY, TableNotFound, register_provider

class Users(Provider):
    async def list_tables(self):
        return ["users"]
    async def schema(self, name):
        if name != "users":
            raise TableNotFound(name)
        return pa.schema(
            [("id", pa.int64()), ("name", pa.utf8())],
            metadata={PK_METADATA_KEY: b"id"},
        )
    async def scan(self, name, columns, filters):
        return pa.table({"id": [1, 2], "name": ["a", "b"]})
    async def insert(self, name, rows): return rows.num_rows
    async def update(self, name, rows, keys): return rows.num_rows
    async def delete(self, name, keys): return keys.num_rows

async def main():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute("ATTACH ':memory:' AS app (TYPE virtual_catalog_provider)")
    register_provider(con, catalog="app", schema_name="public", provider=Users())

asyncio.run(main())
```

## Optional write/edit operations

`list_tables`, `schema`, and `scan` are required. `insert`, `update`, `delete`,
and `alter` are **optional** — a provider supports an operation only by
implementing the method. An unimplemented op is rejected at plan time and
left out of the table's `verbs` reported by the `n6k_table_permissions` SQL
function.

## Limitations

- Provider methods must not call `conn.sql()` on the same connection
  (deadlock). Use `conn.cursor()`.
- `RETURNING` on INSERT/UPDATE/DELETE is not supported.
- Only `add_column`, `drop_column`, `rename_column` reach `alter()`; other
  ALTER kinds raise `BinderException` client-side.
- Primary-key columns must be a fixed-width signed integer, `pa.utf8()`, or
  `pa.large_string()`. UPDATE/DELETE read the key values straight out of the
  Arrow buffers `scan` returned, so the layout has to be one the key reader
  knows; any other is refused with an error naming it, never misread. Non-key
  columns are unrestricted.

## How it crosses into C++

Every UDF below speaks Arrow, in both directions. `schema` answers with an Arrow
IPC *schema message* (`pa.Schema.serialize()`), carrying column names, types and
the `PK_METADATA_KEY` metadata in one payload; `scan` answers with a whole IPC
*stream*; and the write ops receive one. Nothing is rendered into SQL text and
nothing lands in a temp table on the way past, so the rows the provider returns
are decoded exactly once, by the extension that asked for them.


### `Provider`

```python
class Provider()
```

Abstract base for a Python-backed virtual-table namespace. Subclass and implement.


#### `Provider.list_tables`

```python
list_tables(self) -> list[str]
```


#### `Provider.schema`

```python
schema(self, name: str) -> pyarrow.Schema
```

Return the Arrow schema for `name`. Raise `TableNotFound` if unknown.


#### `Provider.scan`

```python
scan(
    self,
    name: str,
    columns: list[str] | None,
    filters: Filters,
) -> pyarrow.Table
```

Read rows from `name`.

- `columns` — projection DuckDB wants, or `None` for all. Push down
  if you can; a superset is also fine (the library projects down).
- `filters` — list of `(col, op, value)` tuples or `None`. The
  provider **must** apply them; DuckDB does not re-filter. Operators
  are in `n6k_protocol.filters.FilterOp`.

Can't push down? Use `n6k_protocol.filters.filter_and_project()` on the
full table. Parquet-backed? `split_pyarrow_filters()` partitions
the clauses pyarrow understands from the `is_null` / `is_not_null`
leftover.

LIMIT is not pushed. Return a full Arrow table; no streaming in v1.


#### `Provider.insert`

```python
insert(self, name: str, rows: pyarrow.Table) -> int
```

Append `rows`. Return the row count written.

Optional; override to support INSERT.


#### `Provider.update`

```python
update(self, name: str, rows: pyarrow.Table, keys: list[str]) -> int
```

Update by primary key. `rows` has PK + changed cols; `keys` names the changed cols.

Optional; override to support UPDATE (also requires a primary key).


#### `Provider.delete`

```python
delete(self, name: str, keys: pyarrow.Table) -> int
```

Delete by primary key. `keys` is an Arrow table of PK column values.

Optional; override to support DELETE (also requires a primary key).


#### `Provider.alter`

```python
alter(self, name: str, change: AlterChange) -> None
```

Apply a schema change. Optional; override to support ALTER TABLE.


#### `Provider.invalidate`

```python
invalidate(self) -> None
```

Bump the version counter so the next catalog lookup refills from this provider.


### `register_provider`

```python
register_provider(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
    schema_name: str,
    provider: Provider,
    main_loop: asyncio.AbstractEventLoop | None = None,
) -> None
```

Bind `provider` to `(con, catalog, schema_name)`.

The target catalog must be `TYPE virtual_catalog_provider`; the schema is
materialised by the extension when the provider lists tables in it.

Provider coroutines run on `main_loop` (default: the loop running at
call time). That loop must own any loop-bound resources the provider
uses (e.g. an asyncpg pool). After registration, the loop thread must
not make synchronous DuckDB calls against `con` — use `asyncio.to_thread`.

Call `release_providers(con)` before closing `con`; `serve_and_close_connection` does.


### `invalidate_provider_tables`

```python
invalidate_provider_tables(
    con: duckdb.DuckDBPyConnection,
    *,
    catalog: str,
) -> None
```

Bump the version counter on `catalog`; next lookup refills.

In-flight queries keep their already-bound plans (DuckDB's standard
catalog-versioning behavior). Only plans compiled after this call see
the refreshed state.


### `TableNotFound`

```python
class TableNotFound
```

Raised by `Provider` methods when the requested table name is unknown.


### `AlterChange`

```python
class AlterChange(kind: str, details: dict[str, Any]) -> None
```

`ALTER TABLE` payload passed to `Provider.alter`.

- `kind` — one of `"add_column"`, `"drop_column"`, `"rename_column"`.
- `details` — kind-specific fields (e.g. `name` / `type` for add,
  `old_name` / `new_name` for rename).


### `PK_METADATA_KEY`

```python
PK_METADATA_KEY = b'vcat.primary_keys'
```


---

## Filter helpers

Filter-clause types and Arrow helpers for the `Provider` API.

Shape-compatible with pyarrow's tuple-form filter argument — a list of
`(col, op, value)` tuples, structurally identical to what
`pyarrow.parquet.read_table(filters=...)` and `pyarrow.dataset` accept.
Providers that read parquet can pass an n6k `filters` value straight to
pyarrow for the operators pyarrow understands; `is_null` and `is_not_null`
are n6k extras with no pyarrow tuple-form equivalent — use
`split_pyarrow_filters()` to partition them out, then apply the leftover
via `filter_and_project()`.


### `filter_and_project`

```python
filter_and_project(
    table: pyarrow.Table,
    columns: list[str] | None,
    filters: Filters,
) -> pyarrow.Table
```

Apply filters, then project, on an in-memory Arrow table.

Filters run first so a clause can reference a column that's absent from
`columns` (projected away on output). Every n6k operator is handled,
including `is_null` and `is_not_null`. Projection honours `columns`
(`None` keeps all columns).

Use this when your provider can't push projection or filters down at
all — load the full table and return `filter_and_project(tbl, columns, filters)`.


### `split_pyarrow_filters`

```python
split_pyarrow_filters(
    filters: Filters,
) -> tuple[list[FilterClause], list[FilterClause]]
```

The first list is directly passable to `pq.read_table(filters=...)` or
`pyarrow.dataset` — the operators `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`
all map to pyarrow's tuple form unchanged.

The second list contains `is_null` / `is_not_null` clauses that
pyarrow's tuple form cannot express. Apply them after the parquet read
via `filter_and_project()` or `pyarrow.compute` directly.


---

## SQL functions (extension)

Table functions registered by the `n6k` DuckDB extension. Load it first
(`LOAD n6k_client;`), then call them like any table function.

### `n6k_table_permissions`

Enumerate tables in a catalog along with their write/edit capability.

```
n6k_table_permissions(catalog VARCHAR, schema := VARCHAR, table := VARCHAR)
```

- `catalog` *(positional, required)* — catalog/database name to inspect.
- `schema` *(named, optional)* — restrict to one schema.
- `table` *(named, optional)* — restrict to one table. `table` is a reserved
  word, so quote it: `"table" := 'foo'`.

Returns one row per table:

| column        | type           | description                                                            |
| ------------- | -------------- | ---------------------------------------------------------------------- |
| `schema`      | VARCHAR        | schema the table lives in                                              |
| `name`        | VARCHAR        | table name                                                             |
| `kind`        | VARCHAR        | `n6k_remote` for network tables; `native_table` / `native_view` for plain/bridge catalogs |
| `verbs`       | VARCHAR[]      | allowed: `select`, `insert`, `update`, `delete`, `alter`, `drop`     |
| `primary_key` | VARCHAR[]      | primary-key column names in key order; empty list if none             |

```sql
-- every table in a catalog
SELECT * FROM n6k_table_permissions('app');

-- one schema
SELECT name, kind, verbs, primary_key
FROM n6k_table_permissions('app', schema := 'main') ORDER BY name;

-- one table (note the quoted reserved word)
SELECT name, kind FROM n6k_table_permissions('app', schema := 'main', "table" := 'nt');
```

### `n6k_table_describe`

A single-row, fully-typed descriptor for one table — columns, primary key,
write/edit capability, and enum domains in one call (so a client doesn't have to
stitch `DESCRIBE` + `duckdb_constraints()` + per-column `enum_range()` together,
none of which see remote/bridge/provider tables correctly).

```
n6k_table_describe(catalog VARCHAR, schema := VARCHAR, table := VARCHAR)
```

- `catalog` *(positional, required)* — catalog/database name.
- `schema` *(named, optional)* — defaults to `main`.
- `table` *(named, required)* — `table` is a reserved word, so quote it:
  `"table" := 'foo'`.

Returns exactly one row:

| column        | type                       | description                                            |
| ------------- | -------------------------- | ------------------------------------------------------ |
| `columns`     | STRUCT(name, type, nullable, "default")[] | ordered column metadata                 |
| `primary_key` | VARCHAR[]                  | primary-key column names in key order; empty if none   |
| `verbs`       | VARCHAR[]                  | allowed: `select`, `insert`, `update`, `delete`, `alter`, `drop` |
| `enums`       | MAP(VARCHAR, VARCHAR[])    | enum-typed column name → its labels                    |

```sql
SELECT columns, primary_key, verbs, enums
FROM n6k_table_describe('app', schema := 'main', "table" := 'users');
```

Note: for remote (`n6k_remote`) tables, `nullable` and `default` are not
transmitted over the wire, so columns report `nullable = true` / `default = NULL`;
columns, types, primary key, and the capability bits are populated for all kinds.

### `n6k_split_statements`

Split a multi-statement SQL string into its individual statements using
DuckDB's parser, so semicolons inside string literals, comments, or quoted
identifiers don't cause a wrong split (unlike a naive `split(';')`).

```
n6k_split_statements(sql VARCHAR)
```

Returns one row per statement, in order:

| column           | type    | description                                            |
| ---------------- | ------- | ------------------------------------------------------ |
| `ordinality`     | BIGINT  | 1-based position of the statement                      |
| `statement`      | VARCHAR | verbatim statement text, trimmed, trailing `;` removed |
| `statement_type` | VARCHAR | DuckDB statement type, e.g. `SELECT`, `INSERT`, `DELETE` |

```sql
SELECT * FROM n6k_split_statements('SELECT 1; INSERT INTO t VALUES (1);');
-- ordinality | statement                | statement_type
--     1      | SELECT 1                 | SELECT
--     2      | INSERT INTO t VALUES (1) | INSERT

-- a semicolon inside a literal stays in one statement
SELECT statement FROM n6k_split_statements('SELECT ''a;b;c''');
-- SELECT 'a;b;c'
```

---

## Server framework (optional)

Mount the n6k WebSocket endpoint on a FastAPI app.

    register(app, "/db/{tenant}", connect=open_db, authenticate=authorize)

Mounts `<prefix>/ws`. Per WebSocket:

1. `connect(ws, **path_params)` returns an empty DuckDB connection with the n6k
   extensions loaded. It may be sync, async, or an async generator (yield the
   connection; code after the `yield` runs once the socket drops).
2. The extension reads the client's FT_HELLO and hands its token to
   `authenticate(ws, con, token, **path_params)`, which verifies it, attaches onto
   `con` what that identity may see, and returns the catalogs to serve. It may be
   sync or async, and may take as long as it needs: the client gets FT_HELLO_PENDING
   (carrying `hello_pending_message`) until it returns. An entry may be a
   `PendingCatalog` instead of a name: its `attach` runs while serving, and clients
   opening it wait (FT_OPEN_PENDING) until it returns.
3. `n6k_serve_fd` serves exactly those catalogs. If given, `on_serve(ws, handle, **path_params)`
   then gets a `LiveServe` that adds and removes catalogs while the socket is served.
4. If given, `resolve(handle, name)` runs when a client opens a catalog that is not served, while that
   client waits (FT_OPEN_PENDING); `handle` is the connection's `LiveServe`. The open gets OPEN_ACK if
   `resolve` served the catalog by the time it returns (e.g. ATTACH plus `handle.add_catalog`), or is
   forwarded to the upstream serving it, else OPEN_ERR. Concurrent opens of one name share one call;
   every OPEN of a name not served calls `resolve` again.
   `authenticate` (or `handle.add_catalog`) may list a `LazyCatalog(name)`: listed to clients at
   once, served only once `resolve` serves it.
5. Binary frames are n6k. Text frames, once HELLO_ACK is sent, go to
   `on_text(ws, text, send_text, **path_params)` if given, each on its own task; `send_text`
   replies, and raises if the client's FT_HELLO offered neither `jsonrpc` nor `mcp`, or once the
   connection has ended.
6. Instead of `on_text`, `jsonrpc=` (an `n6k_server.jsonrpc.JsonRpc`) and/or `mcp=` (an MCP SDK `MCPServer`)
   serve JSON-RPC methods and MCP tools on the text frames. HELLO_ACK grants the client each of
   `jsonrpc` / `mcp` that it offered and the app serves; `mcp=` serves both. `n6k_server.context.current()` inside a method or
   tool is the connection's path params and the identity `authenticate` returned in `Authorized`.

`authenticate` refuses by raising `WsReject(code, reason)`: the reason reaches the
client as FT_HELLO_ERR and nothing is served. Any other exception refuses the same way, with
reason "internal error" and close code 1011. An empty list logs the client in with no catalogs
(text only, or catalogs added later through `on_serve` or `resolve`). `connect` may also raise
`WsReject`, before any frame is read.


### `register`

```python
register(
    app: fastapi.FastAPI,
    prefix: str,
    *,
    connect: Callable[..., duckdb.DuckDBPyConnection | Awaitable[duckdb.DuckDBPyConnection] | AsyncIterator[duckdb.DuckDBPyConnection]],
    authenticate: Callable[..., Sequence[str | n6k_server.pump.PendingCatalog | n6k_server.pump.LazyCatalog] | n6k_server.pump.Authorized | Awaitable[Sequence[str | n6k_server.pump.PendingCatalog | n6k_server.pump.LazyCatalog] | n6k_server.pump.Authorized]],
    ping_interval_ms: int | None = None,
    hello_pending_message: str = 'authenticating',
    on_serve: Callable[..., Awaitable[NoneType] | None] | None = None,
    resolve: Callable[[n6k_server.pump.LiveServe, str], Awaitable[NoneType] | None] | None = None,
    on_text: Callable[..., Awaitable[NoneType] | None] | None = None,
    jsonrpc: n6k_server.jsonrpc.JsonRpc | None = None,
    mcp: ForwardRef('MCPServer') | None = None,
    max_text_inflight: int | None = None,
    max_mcp_inflight: int | None = 8,
) -> fastapi.APIRouter
```

Mount `<prefix>/ws` on `app` and return the router.

`prefix` follows FastAPI's convention: empty for root, else starts with `/` and
does not end with `/`. Path params it captures are passed to `connect` and
`authenticate` as keyword arguments. `ping_interval_ms`: None leaves the keepalive to
`$N6K_PING_INTERVAL` (seconds), 0 disables it. `max_text_inflight`: None sets no limit on text
handlers running at once. `max_mcp_inflight`: MCP requests running at once per connection; past
it a request is answered "server busy". None sets no limit.


### `serve_ws`

```python
serve_ws(
    ws: starlette.websockets.WebSocket,
    connect: Callable[..., duckdb.DuckDBPyConnection | Awaitable[duckdb.DuckDBPyConnection] | AsyncIterator[duckdb.DuckDBPyConnection]],
    authenticate: Callable[..., Sequence[str | n6k_server.pump.PendingCatalog | n6k_server.pump.LazyCatalog] | n6k_server.pump.Authorized | Awaitable[Sequence[str | n6k_server.pump.PendingCatalog | n6k_server.pump.LazyCatalog] | n6k_server.pump.Authorized]],
    params: dict[str, str],
    *,
    ping_interval_ms: int | None = None,
    hello_pending_message: str = 'authenticating',
    on_serve: Callable[..., Awaitable[NoneType] | None] | None = None,
    resolve: Callable[[n6k_server.pump.LiveServe, str], Awaitable[NoneType] | None] | None = None,
    on_text: Callable[..., Awaitable[NoneType] | None] | None = None,
    jsonrpc: n6k_server.jsonrpc.JsonRpc | None = None,
    mcp: ForwardRef('MCPServer') | None = None,
    max_text_inflight: int | None = None,
    max_mcp_inflight: int | None = 8,
) -> None
```

Open the connection `connect` describes, authenticate and serve it, and let it clean up.


### `serve_and_close_connection`

```python
serve_and_close_connection(
    ws: 'WebSocketLike',
    con: 'duckdb.DuckDBPyConnection',
    authenticate: 'Authenticate',
    params: 'dict[str, str]',
    *,
    ping_interval_ms: 'Optional[int]' = None,
    hello_pending_message: 'str' = 'authenticating',
    on_serve: 'Optional[OnServe]' = None,
    on_text: 'Optional[OnText]' = None,
    text_app: 'Optional[TextApp]' = None,
    resolve: 'Optional[Resolve]' = None,
    max_text_inflight: 'Optional[int]' = None,
) -> 'None'
```

Handshake, authenticate, then serve `ws` from `con` until either side drops; then close `con`.

`con` arrives empty. `n6k_handshake` reads the client's FT_HELLO token on it, and only then
does `authenticate(ws, con, token, **params)` attach what that identity may see and return the
catalogs to serve: names already attached, `PendingCatalog`s attached while serving, and, with
`resolve`, `LazyCatalog`s. While it runs past `HELLO_PENDING_FIRST_S`, the client gets
FT_HELLO_PENDING carrying `hello_pending_message`. A `WsReject` or any other exception sends
FT_HELLO_ERR (built by `n6k_reject`) and nothing is served; an empty list logs the client in with
no catalogs. Returning `Authorized(catalogs, identity)` hands `identity` to text handlers.

Ownership passes here: the serving CALL holds `con` for the connection's life. A caller that
needs to reach the data meanwhile must take a `con.cursor()` inside `authenticate`.

Once serving starts, `on_serve(ws, handle, **params)` (sync or async) gets a `LiveServe` for
adding and removing catalogs; it is cancelled when the connection ends.

With `resolve`, a client's OPEN for a catalog that is not served waits (FT_OPEN_PENDING) while
`resolve(handle, name)` (sync or async) runs on that same `LiveServe`, once per name however many
OPENs wait on it. When it returns, the OPENs get OPEN_ACK if it served the catalog (e.g. ATTACH plus
`handle.add_catalog`), are forwarded if an upstream then serves it, and get OPEN_ERR "not found"
otherwise; if it raises, OPEN_ERR with its message.
Nothing is remembered, so a later OPEN of the name calls it again. It is cancelled when the
connection ends.

Binary frames are n6k; text frames go to `on_text(ws, text, send_text, **params)` (sync or
async), or to a session `text_app` opens, each on its own task, once HELLO_ACK is sent. Earlier
text is dropped; with no handler, a JSON-RPC request gets -32601 and other text is dropped.
`send_text` raises when the client's FT_HELLO offered neither jsonrpc nor mcp (or, for a
`text_app`, HELLO_ACK granted neither), or once the connection has ended. With `max_text_inflight`,
more handlers than that running at once closes the socket (1008) unless the session's `handle_busy`
answers it; None (the default) sets no limit. Handlers are cancelled when the
connection ends. `n6k_server.context.current()` inside a handler is the connection's params and
identity.

`con` is closed off the event loop and only after every query on it returns; this returns without
waiting for them, so a query that never returns leaves `con` open.


### `Authorized`

```python
class Authorized(
    catalogs: 'Sequence[ServedCatalog]',
    identity: 'Any' = None,
) -> None
```

What `authenticate` may return instead of a bare catalog list, to hand the connection's
`identity` to its text handlers (`n6k_server.context.current().identity`).


### `LazyCatalog`

```python
class LazyCatalog(name: 'str', rank: 'Optional[int]' = None) -> None
```

A catalog listed before anything serves it, so clients can find it. Opening it calls `resolve`
like any unknown name (so `register` needs `resolve=`); once something serves it, it is listed
once. While nothing does (a failed `resolve`, or the upstream serving it gone) it stays listed and
the next open calls `resolve` again. `remove_catalog` stops listing it.

Against upstreams that report the same name, it is a claim of `rank`: the lowest rank wins (then
the earliest added), so an upstream of lower rank serves it and one of higher rank never does.
`rank=None` loses to every upstream. A local catalog (`add_catalog`) always wins.


### Upstreams

```python
class Upstream(
    ws: 'UpstreamSocket',
    live: "'LiveServe'",
    on_text: 'Optional[OnUpstreamText]',
) -> 'None'
```

One connection to another n6k server, from `LiveServe.connect_upstream`. `catalogs` is what it
served when the HELLO finished; `add_upstream` serves them on this connection.

Its binary frames are n6k and pass between the extension and the websocket unmodified. Its text
frames go to `on_text`, never to the client. `wait_closed()` returns why it ended: the extension's
reason when the extension ended it (ping timeout, `remove_upstream`, ...) while it was added and
this `LiveServe` was open, else the websocket's or a generic one.


#### `LiveServe.connect_upstream`

```python
connect_upstream(
    self,
    ws: 'UpstreamSocket',
    token: 'str' = '',
    on_text: 'Optional[OnUpstreamText]' = None,
) -> 'Upstream'
```

HELLO another n6k server over `ws`, an open websocket to it, with `token`, and read its
catalogs. Waits through its HELLO_PENDING with no overall deadline, but the first reply and each
HELLO_PENDING must arrive in time (see `HelloOnFd`). Raises `UpstreamError` if
`n6k_upstream_hello` fails, including on those timeouts; cancelling closes `ws`. `on_text(text)` (sync or async) gets the
upstream's text frames for as long as it is open.


#### `LiveServe.add_upstream`

```python
add_upstream(self, upstream: 'Upstream', rank: 'int' = 0) -> 'list[str]'
```

Forward to `upstream` each of its catalogs it wins, now and as it reports more. A local
catalog always wins a name; against `LazyCatalog`s and other upstreams the lowest rank wins
(then the earliest added), re-decided on every change. A name that changes hands fails its
sessions on the loser. Returns the names it reports but does not win.


#### `LiveServe.set_upstream_rank`

```python
set_upstream_rank(self, upstream: 'Upstream', rank: 'int') -> 'list[str]'
```

Change an added upstream's rank and re-decide every name it reports. Returns the names it
reports but does not win.


#### `LiveServe.remove_upstream`

```python
remove_upstream(self, upstream: 'Upstream') -> 'None'
```

Stop forwarding to `upstream` and close it; its sessions fail with "removed by host".


### `n6k_server.context`

The connection a handler is serving: an RPC method, an MCP tool, `on_text`, or `resolve`.


#### `current`

```python
current() -> 'Connection'
```

Raises `RuntimeError` outside a connection's handler.


#### `Connection`

```python
class Connection(
    params: 'Mapping[str, str]' = <factory>,
    identity: 'Any' = None,
    _notify: 'Optional[Callable[[str, Any], Awaitable[bool]]]' = None,
) -> None
```

`params` are the route's path params; `identity` is what `authenticate` returned in
`Authorized(..., identity=...)` (None for a plain catalog list).


#### `Connection.notify`

```python
notify(self, method: 'str', params: 'Any' = None) -> 'bool'
```

Send the client a JSON-RPC notification. Keep the `Connection` to push after the handler
returns. False, with nothing sent, when the client's FT_HELLO offered neither jsonrpc nor mcp,
the route has no text handler, or the connection has ended. To an MCP client mid-handshake it is
held and sent once the handshake ends, and dropped if the connection ends first; True either way,
unless more than `mcp._MAX_HELD` are held: then the socket closes (1008) and it returns False.
A failed WebSocket send raises.


### `n6k_server.jsonrpc`

JSON-RPC 2.0 and MCP over the n6k socket's text frames.

    api = JsonRpc()

    @api.method()
    def add_block(after: str, kind: str) -> str: ...

    register(app, "/db/{page}", connect=..., authenticate=..., jsonrpc=api, mcp=mcp_server)

A text frame is one JSON-RPC message. A message whose method is registered on `jsonrpc` goes there;
everything else, responses included, goes to `mcp` when given. Batches are not supported.


#### `JsonRpc`

```python
class JsonRpc() -> 'None'
```

Methods served by `register(..., jsonrpc=)`, added with `@api.method()` or `@api.method("name")`.

Methods may be sync or async and take params by name or position. A `JsonRpcError` answers with
that error; any other exception, or a result `json.dumps` cannot encode, answers INTERNAL_ERROR
and is logged. Unknown methods answer METHOD_NOT_FOUND, params that do not bind INVALID_PARAMS,
invalid JSON PARSE_ERROR, a malformed request or a batch INVALID_REQUEST. Notifications get no
reply.


#### `JsonRpcError`

```python
class JsonRpcError(code: 'int', message: 'str', data: 'Any' = None) -> 'None'
```

Raise from a method to answer with this JSON-RPC error instead of INTERNAL_ERROR.


### MCP

One MCP session per connection, run by the official SDK (`pip install n6k-duckdb[mcp]`).

```python
from mcp.server.mcpserver import MCPServer

server = MCPServer("pages")

@server.tool()
def add_block(after: str, kind: str, content: str) -> str:
    page = n6k_server.context.current().params["page"]
    ...

register(app, "/page/{page}", connect=..., authenticate=..., mcp=server)
```


### `WsReject`

```python
class WsReject(code: 'int' = 4400, reason: 'str' = '') -> 'None'
```

Raise from `connect` or `authenticate` to refuse a connection.

From `authenticate` the reason reaches the client as FT_HELLO_ERR and the code
closes the WebSocket. From `connect` (before any frame) the code and reason close it.

