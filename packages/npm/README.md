# @n6k.io/db

DuckDB-wasm driver for the n6k extension.

Wraps `@duckdb/duckdb-wasm` so the n6k extension can talk to an n6k server over a WebSocket from inside WASM: a ws-worker owns the socket and moves frames to and from the extension through shared-memory rings.

## Install

```bash
npm install @n6k.io/db @duckdb/duckdb-wasm
```

## Requirements

- `SharedArrayBuffer` support — your server **must** set these headers:
  ```
  Cross-Origin-Opener-Policy: same-origin
  Cross-Origin-Embedder-Policy: require-corp
  ```
- Node >= 18 (for the `@n6k.io/db/node` export)

## Usage

### Browser

```js
import { createDuckDB } from '@n6k.io/db'

// Selects the threaded (coi) bundle, wires the n6k workers, opens the database and
// sets custom_extension_repository (defaults to the page origin).
const { conn } = await createDuckDB()

await conn.query('LOAD n6k_client;')
await conn.query("ATTACH 'n6k://example.com' AS remote (TYPE n6k);")
const result = await conn.query('SELECT * FROM remote.main.demo;')
const rows = result.toArray().map(r => r.toJSON())
```

`createDuckDB()` spawns the duckdb worker plus one byte-pump ws-worker per attached catalog; the extension's C++ WebSocket client drives the protocol over shared-memory rings.

### Serving WASM Extensions

The package ships pre-built WASM extensions under `wasm/`. Your dev server needs to serve these files so DuckDB can load them at runtime.

Use the `@n6k.io/db/node` export to get the path to the WASM directory:

```js
import { wasmDir } from '@n6k.io/db/node'
// => absolute path to the `wasm/` directory inside the package
```

The directory structure is:

```
wasm/
  v1.5.6/
    wasm_threads/
      n6k_client.duckdb_extension.wasm
      ...
```

Only the `wasm_threads` (cross-origin-isolated / shared-memory) variant is shipped:
n6k requires cross-origin isolation, and the extension is built solely for shared
memory, so the coi bundle is always the browser runtime. (eh/mvp can't load a
shared-memory extension.)

Example middleware (framework-agnostic, works with Express/Connect-style servers):

```js
import path from 'path'
import fs from 'fs'
import { wasmDir } from '@n6k.io/db/node'

function serveExtensions(req, res, next) {
  const filePath = path.join(wasmDir, req.url)

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const mimeTypes = {
      '.wasm': 'application/wasm',
      '.json': 'application/json',
      '.js': 'text/javascript',
    }
    const ext = path.extname(filePath)
    res.setHeader('Content-Type', mimeTypes[ext] || 'application/octet-stream')
    res.setHeader('Access-Control-Allow-Origin', '*')
    fs.createReadStream(filePath).pipe(res)
    return
  }
  next()
}
```

## API

### `createDuckDB(opts?: CreateDuckDBOptions)`

Returns a `DuckDBInstance`. Requires a cross-origin-isolated page; throws if the browser can only run a non-threaded bundle.

- **`bundle`** — a duckdb-wasm bundle to use instead of the jsDelivr coi bundle.
- **`maximumThreads`** — see `CreateDuckDBOptions.maximumThreads`.
- **`duckdbWorkerUrl` / `wsWorkerUrl`** — override the worker script URLs.
- **`extensionRepository`** — `custom_extension_repository` (default: the page origin).
- **`onStatus(catalog, status)`** — per-catalog socket status (`WsStatus`).

### `wasmDir: string` (from `@n6k.io/db/node`)

Absolute path to the `wasm/` directory shipped with the package. Use this to configure your dev server to serve extension files.

## `N6kDuckDB`

`new N6kDuckDB(opts?)` boots DuckDB-WASM and loads the `n6k` extension. It takes `CreateDuckDBOptions` without the callbacks. Create it once, at module scope (boot is deferred to the first `subscribe`/`ready`/`connect`, so this is SSR-safe); it lives until `dispose()`.

It owns the page's sockets and attached catalogs, and exposes them in the shape `@n6k.io/ui`'s `DuckDBProvider` takes:

```tsx
import { N6kDuckDB } from "@n6k.io/db";
import { DuckDBProvider, QueryProvider } from "@n6k.io/ui/duckdb";

const duckdb = new N6kDuckDB();
duckdb.setDatabases({ page_db: { path: "n6k://api.example.com/p/123", options: { token: "…" } } });

export function Root({ children }) {
  return (
    <QueryProvider>
      <DuckDBProvider connect={duckdb.connect} metadataFns={duckdb.metadataFns}>
        {children}
      </DuckDBProvider>
    </QueryProvider>
  );
}
```

- `connect(): Promise<ConnectionLike>` — waits for boot, then leases a fresh connection. Close it when done.
- `metadataFns` — `tablesOf` (`n6k_parse_sql_get_tables`), `tableMeta` (`n6k_table_describe`) and `catalogs`, a subscribable per-catalog `{ status, version }` store. `status` and `version` are defined by `CatalogState` / `catalogStatesOf`.
- `subscribe`/`getSnapshot` — `N6kDuckDBSnapshot` as an external store.
- `onInvalidated(fn)` / `onCatalogsChanged(fn)` — server pushes.

### `setDatabases(databases)` / `setSockets(sockets)`

`setDatabases` replaces the catalogs set by its previous call (catalogs set through `setDesired` are untouched); the reconciler DETACHes, ATTACHes or re-ATTACHes to match. `setSockets` replaces the socket set; see `SocketSet.apply` for which sockets keep their registration.

- `databases: Record<string, DatabaseSpec>` — catalog name → a bare path string (uses `TYPE n6k` by default), `{ path, options? }`, or `{ socket, catalog?, options? }`. Each becomes `ATTACH '<path>' AS <name> (<options>);`; `options` shallow-merges over `{ TYPE: "n6k" }`. `TYPE` is emitted unquoted; other values are single-quoted with `'` escaped to `''`.
- `sockets: Record<string, SocketSpec>` (browser only). A `{ socket: "<name>", catalog? }` database rides that socket: `ATTACH '' AS <alias> (TYPE n6k, wsId '<id>'[, catalog '<catalog>'])` once it registers. The catalog is `pending` until then. A spec is one of:
  - `{ connect: () => WebSocket, token }` — `N6kDuckDB` opens the socket, and on a drop reopens it with exponential backoff (`MIN_BACKOFF_MS`/`MAX_BACKOFF_MS` in `src/sockets/sockets.ts`) and calls `replaceWebsocket`. It closes the socket when the spec is removed.
  - `{ socket: WebSocket | null, token }` — the app opens the socket. While `null` before its first registration, its catalogs wait. A new socket after registration calls `replaceWebsocket`. It is never closed by `N6kDuckDB`.

  `token` is a string or a `() => string | Promise<string>`, called on every (re)register. A socket that fails to register, or whose reconnect the server refuses (HELLO_ERR), errors its catalogs (`Attach <alias> failed: …`). A `token` or `wsId` option on a socket-backed entry throws.

```ts
duckdb.setSockets({ main: { connect: () => new WebSocket("wss://api.example.com/ws"), token } });
duckdb.setDatabases({
  db: { socket: "main" },
  sales: { socket: "main", catalog: "warehouse" },
  pg: { path: "host=… dbname=…", options: { TYPE: "postgres" } },
});
```

`setDesired(catalog, config, setup?)` / `removeDesired(catalog)` add or drop a single catalog; `setup` statements run before its ATTACH and are kept out of the snapshot. `reconnect(catalog)` reopens a dropped driver-owned socket.

### React helpers (`@n6k.io/db/react`)

- `useAttach(duckdb, catalog, path, options?)` — `setDesired` while mounted, `removeDesired` on unmount; returns `{ status, reconnect }`.
- `useSocket(duckdb, name)` — the `RegisteredWebsocket` for a socket once registered, else `null`. It stays the same object across reconnects; `rpc` rebinds to the new socket.
- `useN6kSnapshot(duckdb)` — the snapshot, re-rendering on change.
- `useDuckDBIframeHost(iframeRef, duckdb, { allowedOrigins })` — see the iframe section.

### Attaching an existing WebSocket

`registerWebsocket(socket, token): Promise<RegisteredWebsocket>` and `replaceWebsocket(wsId, socket, token): Promise<void>` let you attach n6k onto a WebSocket your app already opened, instead of having the driver open its own. They are returned from `createDuckDB()` and are also methods on `N6kDuckDB`. Browser-only.

```ts
const { conn, registerWebsocket, replaceWebsocket } = await createDuckDB();

const ws = new WebSocket("wss://api.example.com/ws");
const { wsId } = await registerWebsocket(ws, token);
await conn.query(`ATTACH '' AS page_db (TYPE n6k, wsId '${wsId}')`); // sends FT_OPEN

// later, after your app reconnects its own socket:
await replaceWebsocket(wsId, newWs, token); // re-HELLOs, then re-OPENs page_db without a DETACH/ATTACH
```

The driver rides the socket: **binary** frames are n6k, **text** frames are JSON-RPC (see below). It listens with `addEventListener`, so your app can still observe the socket; it never closes a socket it doesn't own. The token rides the HELLO frame, not the socket's upgrade; an inline `token` ATTACH option alongside `wsId` is a bind error. Each ATTACH sends an `FT_OPEN` naming its catalog (the ATTACH alias, or the `catalog` option); an unserved catalog fails that ATTACH with the server's `OPEN_ERR`. On a drop, the catalog's `connStatus` becomes `"disconnected"`; open a fresh socket and call `replaceWebsocket`. With `N6kDuckDB`, declare the socket through `setSockets` (above) so its catalogs stay `pending` until it registers. See [n6k-network.md](../../docs/n6k-network.md#attaching-an-existing-websocket-wasmbrowser) for the full contract.

#### JSON-RPC

`registerWebsocket` resolves to `{ wsId, capabilities, rpc, close }`. `capabilities` is what the server granted in HELLO_ACK; `rpc` throws unless it includes `"jsonrpc"`.

```ts
const s = await registerWebsocket(ws, token);
await s.rpc.call("add_block", { after: "b1", kind: "text" }, { timeoutMs: 5000 });
s.rpc.notify("typing", { on: true });
const off = s.rpc.on("block_added", (params) => { /* ... */ });
s.close();
```

- `call(method, params?, { signal?, timeoutMs? })` resolves with the result or rejects with `JsonRpcError { code, message, data }`. The Python `n6k_server` answers `code: -32000` ("server busy") when over its in-flight limit. `timeoutMs` defaults to 60000; a call also fails on abort, socket close or `replaceWebsocket`.
- `on(method, handler)` receives server notifications; it returns `off`. A server request (with `id`) is answered -32601 and never reaches `on`. The server may notify before the first call; add handlers straight after `await registerWebsocket(...)`, before any other `await`, to receive it.
- After `replaceWebsocket`, the client sends on the new socket.
- The registration (and `rpc`) outlives the last `DETACH` on its `wsId`. `close()` fails pending calls and stops handling text; the `wsId` is released once no catalog is attached on it.

#### The `catalog` option

`catalog '<name>'` names the **server-side** catalog a session opens, decoupled from the local ATTACH alias. Without it the session opens a catalog named after the alias; with it, alias and server catalog are independent — so you can attach the same server catalog under different aliases, or (below) several server catalogs over one socket.

```ts
ATTACH '' AS foo (TYPE n6k, wsId '${wsId}', catalog 'analytics_prod');
// local catalog `foo` ⇒ server catalog `analytics_prod`
```

#### Several catalogs over one socket

One registered socket can back **several** catalog sessions at once — give each ATTACH its own `catalog`:

```ts
const ws = new WebSocket("wss://api.example.com/ws");
const { wsId } = await registerWebsocket(ws, token);
await conn.query(`ATTACH '' AS foo (TYPE n6k, wsId '${wsId}', catalog 'catA')`);
await conn.query(`ATTACH '' AS bar (TYPE n6k, wsId '${wsId}', catalog 'catB')`);
// foo and bar now run concurrently over the ONE socket.
await conn.query(`DETACH foo`); // bar keeps working; only foo's session closes
```

Each attach is tagged with a session id (`ns`) that the driver stamps on its frames and the server echoes back, so responses are demuxed to the right catalog; req-id spaces are per-session, so they never collide. An `OPEN_ERR` (e.g. a catalog the server does not serve for this token) fails only that ATTACH — siblings and the socket are unaffected. `DETACH` of one catalog closes just its server session (the socket stays open for the others); `replaceWebsocket` re-HELLOs and re-OPENs **every** session riding the socket on the new one. The server serves whichever catalogs it decided to serve for the socket's token; each ATTACH's `FT_OPEN` picks one of them.

## Server-side rendering (Node, RSC, edge)

These pieces ship for running DuckDB on the server in addition to (or instead of) the browser WASM path:

- `@n6k.io/db/native/*` — thin wrapper around `@duckdb/node-api`. Opens DuckDB in-process, loads the n6k extension, runs ATTACH, returns the raw `DuckDBConnection`. Useful on its own for Node scripts and request handlers; does not depend on React.
- `serverN6k(conn)` (from `@n6k.io/db`) — see [below](#servern6kconn).
- `@n6k.io/db/native-wasm-adapter/*` — `toWasmShape(conn)` translates a native `DuckDBConnection` into the `ConnectionLike` result shape (`{ schema, toArray() }`).

`@duckdb/node-api` is an **optional** peer dependency; install it only if you import the `./native` subpath.

### `createNativeDuckDB(opts)` (from `@n6k.io/db/native/create-native-duckdb`)

```ts
import { createNativeDuckDB } from "@n6k.io/db/native/create-native-duckdb";

const { conn, dispose } = await createNativeDuckDB({
  path: "/path/to/file.duckdb",              // optional; in-memory if omitted
  n6kExtensionPath: "/abs/path/n6k_client.duckdb_extension", // optional; LOAD if set
  databases: { db: "n6k://api.example.com/p/123" },   // ATTACH each entry
});
try {
  // `conn` is the raw @duckdb/node-api DuckDBConnection.
  const reader = await conn.runAndReadAll("SELECT 1 AS x");
} finally {
  dispose();
}
```

`dispose()` is idempotent. The factory registers each undisposed conn in a `FinalizationRegistry` and warns in dev when one is GC'd without being disposed — wire the call into your request lifecycle (`try/finally`, framework `onResponse`, etc.).

### `serverN6k(conn)`

`{ connect, metadataFns }` over one connection you own, for `@n6k.io/ui`'s `DuckDBProvider` during SSR. `connect` hands out `conn` itself (its `close` is a no-op), and `metadataFns` has no catalog store (see `serverN6k`). `createNativeDuckDB`'s `databases` option does the ATTACHes.

```tsx
import { renderToReadableStream } from "react-dom/server";
import { Suspense } from "react";
import { serverN6k } from "@n6k.io/db";
import { createNativeDuckDB } from "@n6k.io/db/native/create-native-duckdb";
import { toWasmShape } from "@n6k.io/db/native-wasm-adapter/adapter";
import { DuckDBProvider, QueryProvider, useSuspenseQuery } from "@n6k.io/ui/duckdb";

function UsersList() {
  const { rows } = useSuspenseQuery("SELECT id, name FROM db.main.users");
  return <ul>{rows.map((r) => <li key={String(r.id)}>{String(r.name)}</li>)}</ul>;
}

export async function handle(_req: Request) {
  const native = await createNativeDuckDB({
    n6kExtensionPath: process.env.N6K_EXTENSION_PATH,
    databases: { db: "n6k://api.example.com/p/123" },
  });
  const n6k = serverN6k(toWasmShape(native.conn));
  try {
    const stream = await renderToReadableStream(
      <QueryProvider>
        <DuckDBProvider connect={n6k.connect} metadataFns={n6k.metadataFns}>
          <Suspense fallback={<p>loading…</p>}>
            <UsersList />
          </Suspense>
        </DuckDBProvider>
      </QueryProvider>,
    );
    await stream.allReady;
    return new Response(stream, { headers: { "content-type": "text/html" } });
  } finally {
    native.dispose();
  }
}
```

## DuckDB in the parent, queries in an iframe (`@n6k.io/db/iframe`)

Share one parent-window DuckDB-wasm instance with a child **iframe**, so `@n6k.io/ui`'s hooks work in the iframe **without it loading DuckDB at all**. The parent owns the single real engine (WASM, workers, the n6k extension, all sockets); the iframe is a thin reactive *view* — every query is a `postMessage` RPC to the parent that returns Arrow IPC bytes, rebuilt with `apache-arrow`'s `tableFromIPC`.

The `@n6k.io/db/iframe` entry pulls in only `apache-arrow` — no `.wasm`, no worker, no n6k extension, no `SharedArrayBuffer`/COOP-COEP headers, no WebSocket.

```tsx
// PARENT — point the host at your iframe.
import { useRef } from "react";
import { N6kDuckDB } from "@n6k.io/db";
import { useDuckDBIframeHost } from "@n6k.io/db/react";

const duckdb = new N6kDuckDB();
duckdb.setDatabases({ db: "n6k://api.example.com/p/123" });

export function Shell() {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  useDuckDBIframeHost(iframeRef, duckdb, { allowedOrigins: [window.location.origin] });
  return <iframe ref={iframeRef} src="/widget" title="widget" />;
}
```

Without React, `bindIframeHost(() => iframe, duckdb, { allowedOrigins })` from `@n6k.io/db/iframe` does the same and returns a dispose function.

```tsx
// IFRAME app — no DuckDB engine here.
import { createIframeN6k } from "@n6k.io/db/iframe";
import { DuckDBProvider, QueryProvider, useQuery } from "@n6k.io/ui/duckdb";

const n6k = createIframeN6k({ parentOrigin: window.location.origin });

function Widget() {
  const { rows } = useQuery("SELECT id, name FROM db.main.users");
  return <ul>{rows.map((r) => <li key={String(r.id)}>{String(r.name)}</li>)}</ul>;
}

export function Embedded() {
  return (
    <QueryProvider>
      <DuckDBProvider connect={n6k.connect} metadataFns={n6k.metadataFns}>
        <Widget />
      </DuckDBProvider>
    </QueryProvider>
  );
}
```

Handshake: the iframe posts `hello` (with a fresh per-mount `clientId`) to `parentOrigin`; the host validates the origin against `allowedOrigins` **and** that the message came from its own iframe element, then transfers a `MessageChannel` port. A new `clientId` (e.g. after an iframe reload) triggers a fresh port — `WindowProxy` identity is stable across reloads, so `clientId` is what distinguishes a reloaded frame from a duplicate hello. The host pushes the parent's attach state (`status`, `desired`, `attached`, …), which backs the iframe's `metadataFns.catalogs` store, so the iframe's reads wait for and refetch with the parent's catalogs.

**Security — the server is the control level, not this bridge.** The iframe holds no token and no socket; it can only send SQL. It cannot exfiltrate a credential (n6k secrets are redacted in `duckdb_secrets()` and live in the parent's worker, unreachable from SQL) and cannot attach a private catalog (no credential to present). So there is no SQL-gating layer here. The only rule is a deployment one:

> The iframe can do exactly what the parent's already-attached catalogs allow — **including writes** (`INSERT`/`UPDATE`/`DELETE`) where the catalog's token grants them — and nothing more. Do not attach into an engine you share with an untrusted iframe anything you would not hand that iframe directly. (DuckDB `ATTACH` is global to the instance, so a shared engine cannot isolate the iframe from any attached catalog.)

Notes:

- Sockets live with the parent's worker and can't cross frames; the iframe has no socket API.
- Each `metadataFns.tablesOf` call is one extra round-trip to the parent.
- Same-origin parent/iframe is the supported configuration. `parentOrigin` and `allowedOrigins` take exact origins; see `IframeN6kOptions` / `IframeHostOptions` for what `"*"` does.

## Debug logging

The driver is quiet by default. Info/debug logs are gated behind a runtime
flag; warnings and errors always print. All output is prefixed `[n6k]` on the
main thread and `[n6k:<name>-worker]` from workers (`duckdb-worker`,
`ws-worker`).

Enable:

```js
localStorage.setItem("n6k:debug", "1"); // browser
globalThis.N6K_DEBUG = true; // any scope (incl. workers, SSR)
```

Disable: `localStorage.removeItem("n6k:debug")` and clear the global. Reload
to pick up the change in already-spawned workers.

## License

MIT
