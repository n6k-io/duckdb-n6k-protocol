# n6k-duckdb

In-process bridge between DuckDB connections with policy-based access control.

## Install

```bash
pip install "n6k-duckdb[duckdb]"
```

## Usage

```python
import duckdb
from virtual_catalog.bridge import Policy, bridge

cfg = {"allow_unsigned_extensions": "true"}
source = duckdb.connect(config=cfg)
source.sql("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR)")
source.sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")

target = duckdb.connect(config=cfg)
bridge(source, target, "app", source_catalog="memory", policies=[
    Policy("main.users", "select", "true"),
    Policy("main.users", "insert", "true"),
    Policy("main.users", "update", "true"),
    Policy("main.users", "delete", "true"),
])

target.sql("SELECT * FROM app.main.users").show()
target.sql("INSERT INTO app.main.users VALUES (3, 'charlie')")
target.sql("UPDATE app.main.users SET name = 'Alice' WHERE id = 1")
target.sql("DELETE FROM app.main.users WHERE id = 2")
```

## Policies

One `Policy(table, setting, value)` per setting, passed to `vcatbridge_set_policy`.
A verb (`select`, `insert`, `update`, `delete`) takes `True` or a predicate;
`update` also takes `{"using": ..., "check": ...}`. `primary_key` takes a column
list. Tables and verbs without a policy are inaccessible.

## How it works

`bridge()` loads the `vcatbridge` DuckDB extension — from `$VIRTUAL_CATALOG_EXT_DIR` when it holds a build, otherwise installed from the n6k extension repository. The extension creates an in-process bridge between two DuckDB database instances using a token-based handshake; one `bridge()` call is one attached catalog, and `unbridge()` detaches it.

No network server is required — data flows directly between connections in the same process.

## Server framework (optional extra)

This package also ships a reusable FastAPI framework for serving the n6k
protocol — install with `pip install "n6k-duckdb[test-server]"`.

The protocol itself is **not implemented in Python**. It is served by the
`n6k_server` DuckDB extension (C++); this package accepts the WebSocket, reads
the client's FT_HELLO token (via the extension), asks you which catalogs that
identity may see, hands the socket to the extension, and shuttles bytes.
It never decodes an n6k frame.

Minimal server with a DuckDB backend:

```python
import duckdb
from fastapi import FastAPI, WebSocket
from n6k_server.extension import load_n6k_server
from n6k_server.pump import WsReject
from n6k_server.server_fastapi.register import register

app = FastAPI()


def open_db(ws: WebSocket, **_):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_n6k_server(con)
    return con                    # empty: nothing attached yet


def authenticate(ws: WebSocket, con, token: str, **_) -> list[str]:
    if token != expected:
        raise WsReject(4401, "unauthorized")
    con.execute("ATTACH ':memory:' AS db")
    # ... populate tables in `con` ...
    return ["db"]                 # served exactly


register(app, "", connect=open_db, authenticate=authenticate)

# Run: `uvicorn your_module:app --port 8099`
```

Per WebSocket, `connect(ws, **path_params)` returns an **empty** connection with
the extensions loaded. The extension then reads the client's FT_HELLO, and
`authenticate(ws, con, token, **path_params)` verifies the token, attaches onto
`con` what that identity may see, and returns the catalogs to serve. Only after
that is anything served — a rejected client builds nothing.

`con` is surrendered to the serving call for the connection's lifetime and closed
on teardown, so nothing else may touch it. `connect` may be sync, async, or an
async generator — yield when you need a hook on both ends:

```python
async def open_db(ws):
    con = duckdb.connect()
    ...
    live[id(ws)] = con.cursor()   # a way into the data while it is being served
    try:
        yield con                 # served here; resumes when the socket drops
    finally:
        del live[id(ws)]
```

A worked example — auth, per-WS observability, `/debug/*` hooks — lives in
`src/n6k_server/test_server/app.py`. Run it with
`python -m n6k_server.test_server --port 8099`.

### Auth

The only credential is the token in the client's FT_HELLO frame, sent once per
socket; there is no `Authorization` header or `?token=` on the upgrade. It
reaches you as `authenticate`'s `token` argument.

To refuse, raise `WsReject(code, reason)`: the reason reaches the client as
FT_HELLO_ERR, nothing is served, and the WebSocket closes with `code`. Raising
any other exception (sent as "internal error", close 1011) refuses the same way.
`connect` may also raise `WsReject`, before any frame is read.

### Notifying clients of stale catalog entries

When the server's catalog state changes outside of a client's own DDL (e.g.
another writer dropped a view), peers' cached table listings go stale. Call

```sql
CALL n6k_serve_push_invalidate('<catalog>', 'main')
```

on any connection to the same database (a `cursor()` of the served connection
works) to send one `FT_PUSH(OP_CATALOG_INVALIDATED)` to every session serving
that catalog; each client drops its cached entries for the named schemas and
refetches on next access. It returns how many sessions it reached.

Nothing fires this automatically — the server decides when a catalog changed.
