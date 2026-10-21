---
modules:
  _duckdb.DuckDBPyConnection: duckdb.DuckDBPyConnection
  _thread.lock: threading.Lock
  asyncio.events.AbstractEventLoop: asyncio.AbstractEventLoop
  fastapi.applications.FastAPI: fastapi.FastAPI
  fastapi.routing.APIRouter: fastapi.APIRouter
  pyarrow.lib.Table: pyarrow.Table
  pyarrow.lib.Schema: pyarrow.Schema
  pyarrow.lib.Field: pyarrow.Field
  collections.abc.Callable: Callable
  collections.abc.Awaitable: Awaitable
  collections.abc.AsyncIterator: AsyncIterator
  typing.Literal: Literal
  typing.Any: Any
  n6k_server.provider.Provider: Provider
  n6k_server.provider.AlterChange: AlterChange
  list[tuple[str, Literal['=', '!=', '<', '<=', '>', '>=', 'in', 'is_null', 'is_not_null'], Any]] | None: Filters
  list[tuple[str, Literal['=', '!=', '<', '<=', '>', '>=', 'in', 'is_null', 'is_not_null'], Any]]: list[FilterClause]
---

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

::: n6k_server.bridge.bridge

---

## Provider

::: n6k_server.provider

### `Provider`

::: n6k_server.provider.Provider

#### `Provider.list_tables`

::: n6k_server.provider.Provider.list_tables

#### `Provider.schema`

::: n6k_server.provider.Provider.schema

#### `Provider.scan`

::: n6k_server.provider.Provider.scan

#### `Provider.insert`

::: n6k_server.provider.Provider.insert

#### `Provider.update`

::: n6k_server.provider.Provider.update

#### `Provider.delete`

::: n6k_server.provider.Provider.delete

#### `Provider.alter`

::: n6k_server.provider.Provider.alter

#### `Provider.invalidate`

::: n6k_server.provider.Provider.invalidate

### `register_provider`

::: n6k_server.provider.register_provider

### `invalidate_provider_tables`

::: n6k_server.provider.invalidate_provider_tables

### `TableNotFound`

::: n6k_server.provider.TableNotFound

### `AlterChange`

::: n6k_server.provider.AlterChange

### `PK_METADATA_KEY`

::: n6k_server.provider.PK_METADATA_KEY

---

## Filter helpers

::: n6k_protocol.filters

### `filter_and_project`

::: n6k_protocol.filters.filter_and_project

### `split_pyarrow_filters`

::: n6k_protocol.filters.split_pyarrow_filters

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

::: n6k_server.server_fastapi.register

### `register`

::: n6k_server.server_fastapi.register.register

### `serve_ws`

::: n6k_server.server_fastapi.register.serve_ws

### `serve_and_close_connection`

::: n6k_server.pump.serve_and_close_connection

### `Authorized`

::: n6k_server.pump.Authorized

### `LazyCatalog`

::: n6k_server.pump.LazyCatalog

### Upstreams

::: n6k_server.pump.Upstream

#### `LiveServe.connect_upstream`

::: n6k_server.pump.LiveServe.connect_upstream

#### `LiveServe.add_upstream`

::: n6k_server.pump.LiveServe.add_upstream

#### `LiveServe.set_upstream_rank`

::: n6k_server.pump.LiveServe.set_upstream_rank

#### `LiveServe.remove_upstream`

::: n6k_server.pump.LiveServe.remove_upstream

### `n6k_server.context`

::: n6k_server.context

#### `current`

::: n6k_server.context.current

#### `Connection`

::: n6k_server.context.Connection

#### `Connection.notify`

::: n6k_server.context.Connection.notify

### `n6k_server.jsonrpc`

::: n6k_server.jsonrpc

#### `JsonRpc`

::: n6k_server.jsonrpc.JsonRpc

#### `JsonRpcError`

::: n6k_server.jsonrpc.JsonRpcError

### MCP

::: n6k_server.mcp

### `WsReject`

::: n6k_server.pump.WsReject
