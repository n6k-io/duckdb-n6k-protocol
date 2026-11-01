# n6k Network Extension (`TYPE n6k`)

Attach a remote catalog served over WebSocket. This documents the **SQL /
host-integration** surface — how to attach, authenticate, ride a socket your app
already opened, and which functions route to the server. For the **on-the-wire**
frame format the client and server speak, see
[`n6k-network-protocol.md`](n6k-network-protocol.md). The in-process bridge and
provider catalogs (no network, no server) live in the sibling
`duckdb-virtual-catalog` repo.

Attached catalogs run over a single WebSocket per ATTACH; every operation —
schema discovery, scans, inserts, DDL, exec, query, and RPC — is multiplexed on
that socket, `CREATE TABLE` included (`OP_CREATE_TABLE`). Catalog-scoped table
functions use the `n6k_catalog_*` prefix (e.g. `n6k_catalog_exec`,
`n6k_catalog_rpc`). The only HTTP a client speaks is OAuth discovery / token
exchange, and that is against the authorization server, not an n6k server.

## Base URL

```sql
ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k);
```

`TYPE n6k` is required — DuckDB does not auto-detect the storage type from
the `n6k://` URL scheme.

The client dials `n6k://` as `ws://` and `n6ks://` as `wss://`, path
`/ws?catalog=<name>`, where `<name>` is the `catalog` ATTACH option, else the
`ATTACH ... AS <name>` alias. Trailing slashes on the base URL are stripped.

## Authentication

### OAuth (standards-based, native)

The native extension can obtain and auto-renew a short-lived data-service JWT
from a durable credential, with no n6k-specific endpoints — the auth server need
only implement these standards:

- RFC 8414 — discovery (`/.well-known/oauth-authorization-server`)
- RFC 8628 — device login (`n6k_login`)
- RFC 6749 — token endpoint (underlies the device grant + the exchange)
- RFC 8693 — token exchange (durable subject token → `aud=data-service` JWT)
- RFC 7519 + 7517 + 7518/8037 — JWT, published key set (JWKS), EdDSA

A **`n6k_refresh` secret** stores the durable subject token plus the `issuer`
(auth base URL), `client_id`, and `resource` (the data-service audience). When an
attach/request needs a token and none is cached (or the cached JWT is within
`kJwtExpiryMarginSeconds` of `exp`), the extension performs RFC 8414 discovery against `issuer`, then an
RFC 8693 token exchange at the discovered token endpoint, and caches the minted
JWT as a **temporary `n6k` access secret**. The exchange is single-flight
process-wide. This whole path is **native-only** — in the browser the host app owns the
OAuth lifecycle and supplies the `n6k` access secret directly.

```sql
CREATE PERSISTENT SECRET n6k_auth (
  TYPE n6k_refresh, SUBJECT_TOKEN '…', ISSUER 'https://auth.example',
  CLIENT_ID 'n6k-duckdb', RESOURCE 'data-service', SCOPE 'data.example:443'
);
ATTACH 'n6k://data.example:443' AS db (TYPE n6k);  -- mints + renews automatically
```

### bearer token

The bearer token sent as the `token` of the socket's `FT_HELLO` frame (never as
a WebSocket upgrade header) is resolved with this precedence:

1. **Inline `token` option** on the ATTACH (plaintext in SQL):

   ```sql
   ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k, token 'your_token');
   ```

2. **`TYPE n6k` secret** — the token lives in DuckDB's Secret Manager
   (redacted in `duckdb_secrets()`, never in the ATTACH text):

   ```sql
   CREATE SECRET my_n6k (TYPE n6k, TOKEN 'your_token', SCOPE 'host:port');
   ATTACH 'n6k://host:port/prefix' AS db (TYPE n6k);   -- no token in SQL
   ```

   `SCOPE` is matched as a literal **prefix** of the scheme-stripped base URL
   (`host[:port][/prefix]`), so `SCOPE 'host'` matches any port/path on that
   host and an omitted `SCOPE` is a catch-all. The scheme is stripped on both
   sides, so the bare hostname works regardless of how ATTACH was spelled
   (`n6k://…` or `(host '…', port '…')`). Use `CREATE OR REPLACE SECRET` to
   rotate; the next ATTACH picks it up.

3. **`TYPE n6k_refresh` secret (native)** — if no valid `n6k` access token is
   present, an on-demand OAuth mint (see *OAuth* above) produces and caches one.

4. Otherwise the HELLO carries an **empty token** (anonymous); the server rejects
   with `HELLO_ERR` if it requires auth.

Passing **`anonymous true`** on the ATTACH forces case 4 directly: it skips the
`TYPE n6k` secret lookup (2) and any `n6k_refresh` mint (3), so the connection stays anonymous regardless of
what secrets exist. It is mutually exclusive with the inline `token` option (1).

```sql
ATTACH 'n6k://host:port' AS db (TYPE n6k, anonymous true);
```

This is useful for unauthenticated local/test servers, and it keeps a stale
`n6k_refresh` secret from breaking an otherwise-anonymous attach (a dead issuer
would otherwise fail the connect during OAuth discovery).

Note: the token is checked once, on the socket's single `FT_HELLO`, and never
re-validated on a live connection.

## Attaching an existing WebSocket (WASM/browser)

By default the driver opens and owns one WebSocket per ATTACH. In the browser
build you can instead hand it a socket your app already opened — so the app
keeps control of the socket's auth and reconnection, and can send its own
JSON-RPC traffic on the same connection through the registration's `rpc`:

```js
const { conn, registerWebsocket, replaceWebsocket } = await createDuckDB();
// ws: a WebSocket you opened. Sends the socket's one FT_HELLO {token}; resolves
// on HELLO_ACK with { wsId, capabilities, rpc, close }, rejects on HELLO_ERR or close.
const { wsId } = await registerWebsocket(ws, token);
await conn.query(`ATTACH '' AS foo (TYPE n6k, wsId '${wsId}')`);
```

The `wsId` ATTACH option is **WASM/browser only** — native throws. Because the
socket is already open, several things the driver normally owns become the app's
responsibility:

- **Catalog binding** — each `ATTACH` sends an `FT_OPEN {ns, catalog}` naming
  the server catalog (the `catalog` ATTACH option, else the alias); the server answers
  `OPEN_ACK` or `OPEN_ERR`. See the
  [handshake lifecycle](n6k-network-protocol.md#lifecycle).
- **Auth** — the token goes in the socket's single `FT_HELLO`, sent by
  `registerWebsocket`. An `ATTACH` with `wsId` sends `OPEN` only; an inline
  `token` ATTACH option alongside `wsId` fails the ATTACH with an `IOException`.

**Binary and text.** Every n6k frame is a binary WebSocket message; text frames
are JSON-RPC 2.0, one message per frame. The driver attaches its listener with
`addEventListener` (never overwriting `onmessage`), routes binary frames to the
catalogs and text frames to the registration's `rpc` client. `binaryType` is
forced to `"arraybuffer"`; don't put your own binary traffic on a shared socket.
`rpc` throws unless the server granted `jsonrpc` in HELLO_ACK (a Python server
does when its app serves JSON-RPC; see `register()`). The driver's HELLO offers
`jsonrpc` and `mcp`, so the server may send notifications before the first call. The registration outlives the last
`DETACH`. `close()` ends it; with catalogs still attached, the `wsId` stays bound until the last DETACH.

**Reconnection.** The driver cannot reopen a socket it does not own. On a drop it
reports `disconnected` status; the app opens a fresh socket and hands it over,
keeping the catalog attached — no DETACH/ATTACH, and in-flight requests fail so
the app re-issues them:

```js
await replaceWebsocket(wsId, newWs, token); // re-HELLOs, then re-OPENs every attached catalog
```

One registered socket can back several catalogs: each `ATTACH` opens its own
`ns`-scoped session on it.

`N6kDuckDB`'s `setSockets`/`setDatabases` do the register → ATTACH →
`replaceWebsocket` steps for you, and its catalogs are `pending` until they
attach. See the [npm README](../packages/npm/README.md#setdatabasesdatabases--setsocketssockets).

## Attaching an existing socket fd (native)

The native build has the fd-level analogue of `wsId`: hand the extension one end
of a connected socket — typically `socket.socketpair()` — as a raw file
descriptor, and it speaks n6k frames over that fd instead of dialing the server
itself. The host process opens the real WebSocket and performs the HTTP upgrade,
then pumps frames between the WebSocket and its end of the pair. The socket's
one `FT_HELLO` is sent by `n6k_connect`; each `ATTACH` then sends `FT_OPEN`:

```python
import socket, struct, threading
import duckdb
from n6k_protocol.protocol import STREAM_FRAME_JSONRPC, STREAM_FRAME_N6K
from websockets.sync.client import connect as ws_connect

py_sock, ext_sock = socket.socketpair()
ws = ws_connect("ws://localhost:8099/ws")

# Stream framing: see below.
def ws_to_sock():
    for msg in ws:
        if isinstance(msg, bytes):
            py_sock.sendall(struct.pack(">IB", len(msg), STREAM_FRAME_N6K) + msg)

def sock_to_ws():
    while True:
        hdr = py_sock.recv(5, socket.MSG_WAITALL)
        if len(hdr) < 5:
            break
        n, frame_type = struct.unpack(">IB", hdr)
        payload = py_sock.recv(n, socket.MSG_WAITALL)
        ws.send(payload.decode() if frame_type == STREAM_FRAME_JSONRPC else payload)

threading.Thread(target=ws_to_sock, daemon=True).start()
threading.Thread(target=sock_to_ws, daemon=True).start()

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.load_extension("/path/to/n6k_client.duckdb_extension")
fd = ext_sock.fileno()
con.execute(f"SELECT * FROM n6k_connect(wsFd => {fd}, token => '...')")  # FT_HELLO
con.execute(f"ATTACH '' AS db (TYPE n6k, wsFd {fd})")  # FT_OPEN
ext_sock.close()  # the extension dup()'d the fd; drop our copy

con.execute("SELECT * FROM db.main.users")
```

The `wsFd` ATTACH option is **native only** — the WASM build throws (use `wsId`
there instead). Because the socket is already open, the same responsibilities
that `wsId` shifts to the app apply here:

- **Catalog binding** — each `ATTACH` sends an `FT_OPEN {ns, catalog}` naming
  the server catalog (the `catalog` ATTACH option, else the alias) and waits for
  `OPEN_ACK` (credit tunables + capabilities) or `OPEN_ERR`.
- **Auth / handshake** — `n6k_connect(wsFd => <fd>, ...)` sends the socket's
  one `FT_HELLO` and waits for `HELLO_ACK`, which arrives over the fd via the
  pump; its `capabilities` column is the `HELLO_ACK` list. Parameters and errors:
  `Bind` in `src/n6k_client/n6k_connect_function.cpp`. An `ATTACH` with `wsFd`
  sends `OPEN` only; an inline `token` ATTACH option alongside `wsFd` fails the
  ATTACH with an `IOException`.

**Stream framing (required).** Over a WebSocket each binary message is exactly
one n6k frame and each text message one JSON-RPC message, so boundaries and types
come for free. A raw byte stream has neither — so the fd path **must** use n6k
stream framing (the "Stream framing" constants in
`packages/python/src/n6k_protocol/protocol.py`); `packages/python/src/n6k_server/pump.py` is a reference pump.
The extension framing is implemented in `src/n6k_client/ws_fd_hub.cpp`.

**fd ownership.** The extension `dup()`s the fd at `ATTACH` time and owns the
copy, so the host closes its own `ext_sock` right after attaching — no
double-close race. On `DETACH` (or connection teardown) the extension closes its
dup, which the host observes as EOF on `py_sock`, letting its pump threads exit.

**Sharing one fd across several `ATTACH`es.** The same `wsFd` may back more than
one catalog. A process-global hub (`src/n6k_client/ws_fd_hub.cpp`) owns the fd: exactly
one `dup()`, one reader thread, one serialized writer, no matter how many
sessions ride it. Inbound frames are demultiplexed by the protocol's `ns` key —
each `ATTACH` stamps a distinct `ns` on its `FT_OPEN`, the server stamps every
session-scoped reply with the same `ns`, and the hub routes on it. Routing of
frames without an `ns`: `FdHub::DispatchInbound`.

The fd is closed only when the **last** session on it detaches, and teardown
never calls `shutdown()` — that would tear the socket down for every sibling.
Because the hub registry is keyed by fd *number*, a host must not close and
recycle an fd number while an `ATTACH` still rides it.

**Conflicts.** `wsFd` is mutually exclusive with a URL / `host` / `port` / `secure`
/ `prefix`: the socket is already connected, so any dial target is rejected as a
hard `IOException` rather than silently ignored. Token resolution is skipped too —
the token was sent by `n6k_connect`.

A full worked example (with the bidirectional pump and a `DETACH`-closes-the-fd
assertion) is in `integration_tests/test_wsfd_attach.py`.

## Function registry

| Function | Transport | Scope |
|----------|-----------|-------|
| `n6k_catalog_exec('db', sql)` | WS EXEC | attached catalog |
| `n6k_catalog_query('db', sql)` | WS QUERY | attached catalog |
| `n6k_catalog_rpc('db', 'func', args)` | WS RPC_SCALAR | attached catalog |
| `n6k_catalog_rpc_table('db', 'func', sub, args)` | WS RPC_TABLE | attached catalog |
| `db.exec(sql)` | WS EXEC | attached catalog |
| `db.query(sql)` | WS QUERY | attached catalog |
| `db.func(args)` | WS RPC | attached catalog |
| `n6k_version()` | none | utility |
| `n6k_parse_sql_get_tables(sql)` | none | utility — returns `(catalog, schema, table_name, ref_type, privilege_type)`; `catalog`/`schema` are NULL when the user did not write them, `ref_type` is `'base_table'` or `'table_function'`, `privilege_type` is the access the statement needs for that table: `'select'`/`'insert'`/`'update'`/`'delete'` for DML and `'create'`/`'drop'`/`'alter'` for DDL targets (CREATE/DROP/ALTER TABLE and VIEW). DDL views are reported with `ref_type='base_table'`. `DESCRIBE`/`SUMMARIZE` of a table (or sub-select) report their underlying table(s) as `'select'`, so cached `DESCRIBE` queries invalidate on writes; bare catalog listings (`SHOW TABLES`/`SHOW DATABASES`) reference no specific table and return nothing. TRUNCATE is not parseable by DuckDB and is unsupported. |

Each op above maps to one WS REQ op; the request args and streamed Arrow
responses are specified in
[`n6k-network-protocol.md` → REQ ops](n6k-network-protocol.md#req-ops).

## See also

- [`n6k-network-protocol.md`](n6k-network-protocol.md) — the on-the-wire frame
  format, ops, and lifecycle (for server / alternate-client implementers).
