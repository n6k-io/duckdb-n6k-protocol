# n6k Network Extension (`TYPE n6k`)

Attach a remote catalog served over WebSocket. This documents the **SQL /
host-integration** surface — how to attach, authenticate, ride a socket your app
already opened, and which functions route to the server. For the **on-the-wire**
frame format the client and server speak, see
[`n6k-network-protocol.md`](n6k-network-protocol.md). The in-process bridge and
provider catalogs (no network, no server) live in the sibling
`duckdb-virtual-catalog` repo.

Attached catalogs with the same URL and token share one WebSocket; every operation —
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

The client dials `n6k://` as `ws://` and `n6ks://` as `wss://`, path `/ws`,
followed by any `?query` the URL carries. The catalog it opens travels in
`FT_OPEN`: the `catalog` ATTACH option, else the `ATTACH ... AS <name>` alias.
A trailing slash on the base URL is stripped.

Every `ATTACH` of one URL (and token) shares one connection, opened by the first
and closed when the last is detached (see Shared connections).

## Authentication

### OAuth (standards-based, native)

The native extension can obtain and auto-renew a short-lived data-service JWT
from a durable credential, with no n6k-specific endpoints — the auth server need
only implement these standards:

- RFC 8414 — discovery (`/.well-known/oauth-authorization-server`)
- RFC 8628 — device login (`n6k_login`)
- RFC 6749 — token endpoint (underlies the device grant + the exchange); §6 refresh-token grant
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

If the secret has a `refresh_token`, the extension instead POSTs the RFC 6749 §6
refresh-token grant to `token_endpoint` with `client_id` (and `resource`, when
set). `token_endpoint` and `client_id` are required; `subject_token` and `issuer`
are not used, and no discovery runs. `refresh_token` is redacted.

```sql
CREATE PERSISTENT SECRET n6k_auth (
  TYPE n6k_refresh, REFRESH_TOKEN '…', TOKEN_ENDPOINT 'https://auth.example/oauth/token',
  CLIENT_ID 'n6k-duckdb', RESOURCE 'data-service', SCOPE 'data.example:443'
);
```

A new refresh token in the response replaces the stored one in the same secret. A
temporary secret stays temporary, so its rotated token is lost on exit. An
`invalid_grant` response fails with a "log in again" error.

Refreshes are serialized within a process but not across processes. With a provider
that rotates refresh tokens, a second process still holding the old token gets the
"log in again" error even though the login is valid; restarting it picks up the new
token. A provider with reuse detection may instead revoke the login for both.

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

   `SCOPE` names a server, and both it and the URL are parsed before matching:

   | `SCOPE` | Matches |
   | --- | --- |
   | `host` | exactly that host, any port and path, `n6k://` or `n6ks://` |
   | `host:443` | that port, any path |
   | `host:443/prefix` | that path and below |
   | `n6ks://host:443` | that port, any path, TLS only (`n6k://host:443` is plaintext only) |
   | `/prefix` | a browser-relative URL at that path and below |
   | omitted | every server |

   A URL with no port is looked up at its scheme's default, so `n6k://host` matches
   `host:80` and not `host:8099`. The most specific match wins, and hostnames match
   case-insensitively. `host/prefix` is
   an error, since a path needs a port. A URL with `user@` in it is refused.
   `duckdb_secrets()` shows the stored form, such as `n6k://host:443/`. Use
   `CREATE OR REPLACE SECRET` to rotate; the next ATTACH picks it up (for redials, see
   the note below).

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
re-validated on a live connection. On a native redial, a token that came from
secrets (2, 3) is resolved again, so an expired JWT is re-minted first. An inline
token (1) is resent as is, and a connection that opened with no token stays
anonymous.

## Attaching an existing WebSocket (WASM/browser)

By default the driver opens and owns one WebSocket per URL and token (see
[Shared connections](#shared-connections)). In the browser build you can instead
hand it a socket your app already opened — so the app keeps control of the
socket's auth and reconnection, and can send its own JSON-RPC traffic on the same
connection through the registration's `rpc`:

```js
const { conn, registerWebsocket, replaceWebsocket } = await createDuckDB();
// ws: a WebSocket you opened. Sends the socket's one FT_HELLO {token}; resolves
// on HELLO_ACK with { wsId, capabilities, rpc, close }, rejects on HELLO_ERR or close.
const { wsId } = await registerWebsocket(ws, token);
await conn.query(`ATTACH '' AS foo (TYPE n6k, CONNECTION '${wsId}')`);
```

`registerWebsocket` is the browser's `n6k_connect`, and `wsId` is the connection's
id. A plain `ATTACH 'n6k://…'` with the same URL and token rides the registered
socket too, for as long as that socket is open. Because the socket is already
open, several things the driver normally owns become the app's responsibility:

- **Catalog binding** — each `ATTACH` sends an `FT_OPEN {ns, catalog}` naming
  the server catalog (the `catalog` ATTACH option, else the alias); the server answers
  `OPEN_ACK` or `OPEN_ERR`. See the
  [handshake lifecycle](n6k-network-protocol.md#lifecycle).
- **Auth** — the token goes in the socket's single `FT_HELLO`, sent by
  `registerWebsocket`. An `ATTACH` with `CONNECTION` sends `OPEN` only; an inline
  `token` ATTACH option alongside `CONNECTION` fails the ATTACH with an `IOException`.

**Binary and text.** Every n6k frame is a binary WebSocket message; text frames
are JSON-RPC 2.0, one message per frame. The driver attaches its listener with
`addEventListener` (never overwriting `onmessage`), routes binary frames to the
catalogs and text frames to the registration's `rpc` client. `binaryType` is
forced to `"arraybuffer"`; don't put your own binary traffic on a shared socket.
`rpc` throws unless the server granted `jsonrpc` in HELLO_ACK (a Python server
does when its app serves JSON-RPC; see `register()`). The driver's HELLO offers
`jsonrpc` and `mcp`, so the server may send notifications before the first call. The registration outlives the last
`DETACH`. `close()` ends it: with catalogs still attached, the socket stays bridged until the last DETACH.

**Reconnection.** The driver cannot reopen a socket it does not own. On a drop it
reports `disconnected` status; the app opens a fresh socket and hands it over,
keeping the catalog attached — no DETACH/ATTACH, and in-flight requests fail so
the app re-issues them. Status returns to `connected` at once, and each catalog
sends a new `OPEN` on its next request:

```js
await replaceWebsocket(wsId, newWs, token); // re-HELLOs; attached catalogs reopen on their next request
```

One registered socket can back several catalogs: each `ATTACH` opens its own
`ns`-scoped session on it.

`N6kDuckDB`'s `sockets`/`databases` options do the register → ATTACH →
`replaceWebsocket` steps for you, and its `connect()` resolves once they
attach or fail. See the [npm README](../packages/npm/README.md#databases--sockets).

## Shared connections

A connection is keyed by the URL it dials (query included) and the token its
`FT_HELLO` carries, for `wsFd` by the socket, or for a registered WebSocket by its
`wsId`. Every `n6k_connect` and every
`ATTACH 'n6k://…'` holds a lease on its connection, opening it if none is open
for that key. Every `ATTACH ... (CONNECTION '<id>')` holds a lease on the
connection that id names, and fails if there is none or it is dead. `n6k_disconnect(<id>)`
drops an `n6k_connect` lease and fails when none is left; `DETACH` drops its `ATTACH`'s
lease. The last one dropped closes the connection. The
connection's one `FT_HELLO` is sent when it opens, and each `ATTACH` sends only
`FT_OPEN`. `DETACH` closes that catalog's session on the server. So two
`ATTACH 'n6k://host'` with one token ride one socket, on native and in the browser.
`n6k_connect` and the functions below are native only; the browser's explicit
connection is `registerWebsocket`.

```sql
SELECT connection_id FROM n6k_connect(url => 'n6k://localhost:8099/tuned?catalogs=a,b');
ATTACH '' AS a (TYPE n6k, CONNECTION '<id>');
ATTACH '' AS b (TYPE n6k, CONNECTION '<id>');
SELECT * FROM n6k_connections();   -- connection_id, alive, capabilities, catalogs, leases
SELECT * FROM n6k_disconnect('<id>');   -- a and b keep it open until they are detached
```

`n6k_connect` takes exactly one of:

- `url => 'n6k(s)://host[:port][/prefix][?query]'`: the extension dials
  `ws(s)://host[:port][/prefix]/ws[?query]` itself. After a drop, the next request
  on any of its catalogs dials again, and `n6k_reconnect(<id>)` dials again at once
  (dropping the current socket if it is still up); its catalogs reopen on their
  next request.
- `wsFd => <fd>`: one end of a socket the host already connected (below). A dropped
  socket cannot be dialed again, and `n6k_reconnect` fails.

With `enable_external_access = false`, the extension opens no new connection and sends no
OAuth request. `ATTACH` and `n6k_connect` (`url` and `wsFd`) that would open a connection,
`n6k_reconnect`, redials after a drop and OAuth requests fail with a permission error.
Connections already open keep working and can still be shared by `ATTACH` and `n6k_connect`,
so open them first.

`n6k_connect` also takes `token` or `scope`, and two host callbacks. A `token`, even an empty
one, is sent as given with no secret lookup. Without one, secrets are resolved as
`ATTACH` resolves them: for the URL, or for `scope` with `wsFd`, which has no URL and
requires `token` or `scope`. `scope` with `url` is an error. `scope` is looked up as a URL:
no scheme reads as `n6k://` and no port as the scheme's default, so `scope => 'host'` matches
neither a `host:8099` secret nor a TLS-only one. Use `n6ks://host:port` for a TLS-only secret.
Each callback is the name of a scalar function taking
`(connection_id UUID, text VARCHAR)`:

- `on_text`: called with each JSON-RPC text message the server sends; a non-NULL,
  non-empty result is sent back as text. `HELLO` offers `jsonrpc` / `mcp` with it, with
  `on_close`, or on any `wsFd` connection.
  Without it, a JSON-RPC request is answered "Method not found".
  `n6k_send_text(<id>, text)` sends text the other way.
- `on_close`: called with the reason when the server or the socket drops the
  connection.

Only the `n6k_connect` that opens a connection may give `on_text` / `on_close`: one
given while a connection for that key is already open fails, since that
connection's `HELLO` is already sent.

The callbacks run on a thread of the connection's own, on a connection marked as
served, so they cannot call `n6k_connect` and the other connection functions.

Connection ids are per database.

Parameters and errors: `src/n6k_client/n6k_connect_function.cpp` and
`n6k_connection_functions.cpp`. The connection itself, its handshake and its
routing: `src/n6k_client/connection_hub.cpp`.

## Attaching an existing socket fd (native)

The native build has the fd-level analogue of `registerWebsocket`: hand the extension one end
of a connected socket — typically `socket.socketpair()` — as a raw file
descriptor, and it speaks n6k frames over that fd instead of dialing the server
itself. The host process opens the real WebSocket and performs the HTTP upgrade,
then pumps frames between the WebSocket and its end of the pair:

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
conn_id = con.execute(
    f"SELECT connection_id FROM n6k_connect(wsFd => {ext_sock.fileno()}, token => '...')"
).fetchone()[0]  # FT_HELLO
ext_sock.close()  # the extension dup()'d the fd; drop our copy
con.execute(f"ATTACH '' AS db (TYPE n6k, CONNECTION '{conn_id}')")  # FT_OPEN

con.execute("SELECT * FROM db.main.users")
```

`wsFd` is **native only** — the WASM build throws (use `registerWebsocket` there instead).
Its `HELLO` offers `jsonrpc` / `mcp` whether or not `on_text` is set: the host
relaying the socket may answer JSON-RPC itself. An inline `token` ATTACH option
alongside `CONNECTION` fails the ATTACH with an `IOException`.

**Stream framing (required).** Over a WebSocket each binary message is exactly
one n6k frame and each text message one JSON-RPC message, so boundaries and types
come for free. A raw byte stream has neither — so the fd path **must** use n6k
stream framing (the "Stream framing" constants in
`packages/python/src/n6k_protocol/protocol.py`); `packages/python/src/n6k_server/pump.py` is a reference pump.
The extension framing is implemented in `src/n6k_client/ws_transport_raw.cpp`.

**fd ownership.** `n6k_connect` `dup()`s the fd and the connection owns the
copy, so the host closes its own `ext_sock` right after connecting. When the
last lease is dropped (or the database closes) the extension closes its dup,
which the host observes as EOF on `py_sock`, letting its pump threads exit. A
second `n6k_connect` on a socket that already carries a live connection shares it
and takes another lease; with `on_text` / `on_close` it fails.

**Sharing.** One connection backs any number of catalogs: see
[Shared connections](#shared-connections) and
[Sessions](n6k-network-protocol.md#sessions). Routing of frames without an `ns`:
`ConnectionHub::OnMessage`.

A full worked example (with the bidirectional pump and a
last-release-closes-the-fd assertion) is in `integration_tests/test_wsfd_attach.py`.

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
