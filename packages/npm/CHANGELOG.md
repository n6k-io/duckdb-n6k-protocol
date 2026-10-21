# Unreleased

## Breaking

- Protocol version 12 (see `docs/PROTOCOL_CHANGELOG.md`).
- `n6k_table_permissions` and `n6k_table_describe` return `verbs VARCHAR[]` in place of `writeable` / `editable`.
- `registerWebsocket` returns an object instead of the wsId string. Use its `wsId` field wherever you used the string.
- `registerWebsocket`'s HELLO offers the `jsonrpc` and `mcp` capabilities (`APP_HELLO_CAPS`); driver-owned sockets do not. A server grants them only when its app serves JSON-RPC / MCP on text frames.
- A registered socket now stays registered after its last DETACH, until you call `close()`.
- The driver now handles the socket's text frames as JSON-RPC. If your app sends its own text on a socket it shares with n6k, move that traffic to JSON-RPC methods.

## Added

- `capabilities`: what the server granted during the handshake.
- `rpc`: JSON-RPC 2.0 over the same logged-in socket. It can call methods, send notifications and listen for server notifications. It throws if the server didn't grant `jsonrpc`.
- Server errors reject with `JsonRpcError`, which carries the code, message and data.
- Nothing is sent before login finishes.
- `HelloRefusedError`: `registerWebsocket` / `replaceWebsocket` reject with it when the server answers HELLO_ERR. A provider socket whose reconnect is refused this way reports the error on its catalogs.
- `DuckDBProvider` `sockets` prop: `databases` entries `{ socket, catalog? }` attach over a provider-registered socket. `useSocket(name)` returns its registration.

## Fixed

- `useStreamSQLQuery` waits for its catalogs to attach and reports their attach errors, as `useQuery` does.
- `catalogOf` returns the catalog for every ref type, not only base tables.
- `withLease` closes its connection exactly once; a second close could kill another lease's reused connection.

## Notes

- Pending calls fail when the socket closes or is replaced. After a replace, the client uses the new socket.
- The Python `n6k_server` answers "server busy" (code -32000) when over its in-flight limit.
- Calls time out after 60s unless `timeoutMs` is given.
- `rpc` isn't available inside iframes, in the server-side provider, or in the native backend.

# 0.11.2

- When the server invalidates a catalog (e.g. after swapping it), mounted `useQuery` / `useTableMeta` / `useView` on that catalog now refetch automatically. Expect more queries if the server invalidates often.
- New optional `createDuckDB({ onInvalidated(catalog, schemas) })` for code outside the React provider.
- `DuckDBContextValue` gained a required `invalidations` field. Update any hand-built context or mocks.

# 0.11.1

- `listCatalogs(wsId)` (on `createDuckDB`, `createN6kWorker`, `useDuckDB()`) returns the catalogs the server serves on a registered socket, including ones not ready yet. No ATTACH needed. Rejects if the `wsId` is unknown (including after the last catalog on it is DETACHed), the socket is not open, closes or is replaced before the reply, or no reply arrives within 10s.
- `onCatalogsChanged(wsId, catalogs)` option on `createDuckDB` / `createN6kWorker` (in React, pass it in the provider's `duckdbOptions`): called with the full list whenever the server adds or removes catalogs on a registered socket. A removed catalog stays attached and its queries fail; DETACH it.

# 0.11.0

## Breaking

- Protocol version 11 (see `docs/PROTOCOL_CHANGELOG.md`).
- `n6k_capabilities()` returns a second column, `level` (`connect` or `open`).

## Added

- `ATTACH` waits while the server is still authenticating and while the catalog is not ready yet. It can be interrupted. Its error includes the server's last status message.
- `registerWebsocket` and `replaceWebsocket` wait while the server is still authenticating, with no time limit.

## Fixed

- A query could hang if the server reported a table change while that query was looking up a table.
