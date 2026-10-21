# ProtoVersion 12

## Other

- `TABLES_LIST` (breaking): each table row carries `verbs` (crossing's spelling: `select`, `insert`, `update`, `delete`, `alter`, `drop`) in place of `writable` / `editable`, and the chunk carries `schemas: [{name, verbs}]`, whose only verb is `create`. SQL (breaking): `n6k_table_permissions` and `n6k_table_describe` return `verbs VARCHAR[]` in place of `writeable` / `editable`.
- New REQ ops `CREATE_VIEW` (21) `{schema, name, on_conflict: error|ignore|replace, names, plan, txn?}` and `DROP_VIEW` (22) `{schema, name, if_exists, txn?}`. `plan` is the view's body as Substrait JSON over the session's catalog, with its columns in `names`; the server stores the view under its own catalog name, so the client's ATTACH alias never reaches it. Both answer `RESP_END {ok}`.
- New open capability `catalog_plans`: a `READ_PLAN`, `WRITE_PLAN` or `CREATE_VIEW` plan may name a table `[catalog, schema, table]`, for any catalog a session on the same socket has open. A server never grants it for a catalog it forwards upstream.
- `TABLES_LIST` and `TABLE_SCHEMA` take an optional `txn`: list and describe inside that open transaction, so it sees its own uncommitted DDL.
- No wire changes in this entry. SQL (breaking): `n6k_serve_socket` no longer dials `N6K_DB_SOCKET`; it only listens, on `listen =>` or `N6K_LISTEN`, and bind fails when neither is set.

# ProtoVersion 11

## Other

- Text frames: binary frames are n6k, text frames belong to the socket owner (conventionally JSON-RPC). Text is accepted only after `HELLO_ACK`, and the server sends text only to a client whose `HELLO` offered `jsonrpc` or `mcp`. A text frame no longer ends the session. No n6k frame changes; no version bump.
- New connect capabilities `jsonrpc` and `mcp`, offered by every client that reads text frames (not the native dial client) and granted only when the server's app serves them. `n6k_serve_fd` takes `app_capabilities => [...]` to grant them.
- No wire changes in this entry. SQL (breaking): `n6k_reject(reason)` and `n6k_hello_pending(message)` take no fd and return the frame (`frame BLOB`) for the host to send instead of writing it; `n6k_reject` no longer half-closes. New `n6k_hello_ack(capabilities => [...], app_capabilities => [...])` returns the HELLO_ACK frame, and `n6k_serve_fd(hello_acked => true)` then sends none, so a host that sends it orders HELLO_ACK before its own text.
- No wire changes in this entry. SQL (breaking): `n6k_serve_socket` / `n6k_serve_fd` / `n6k_serve_http` with no catalog names serve none; `'*'` alone serves every attached catalog. A socket may be served with zero catalogs. `n6k_connect` returns `(connected, capabilities)`. `n6k_serve_inflight` returns `(holding, running)` and raises on a catalog that is not paused. `n6k_serve_fd(ping_interval_ms => N)` rejects a negative `N`.

- Capabilities are negotiated per socket (`HELLO` → `HELLO_ACK`) and per catalog (`OPEN` → `OPEN_ACK`). The client lists the names it supports, and the ACK returns the ones the server also supports. Unknown names are ignored.
- `HELLO_ACK` and `OPEN_ACK` always include `capabilities`, even when the list is empty.
- `crossing` is now a per-socket capability (`HELLO_ACK`). It is no longer sent on `OPEN_ACK`.
- `HELLO` must include `protocol_version`. If it is missing or does not match, the server replies `HELLO_ERR`. Additive changes no longer bump the version.
- New frame `HELLO_PENDING` (19) `{message?}`: the server is still authenticating.
- New frame `OPEN_PENDING` (18) `{ns, message?}`: the catalog is not ready yet.
- The server repeats each pending frame every 2s until the ACK or ERR arrives. The client waits 5s for the first reply to `HELLO` / `OPEN`. Once a pending frame arrives there is no overall deadline: the client gives up only if 6s pass with no new frame.
- `CATALOG_LIST` includes pending catalogs.
- `CANCEL {ns, id: 0}` abandons a pending open.
- New PUSH op `CATALOGS_CHANGED` (20) `{catalogs}`, no `ns`: the socket's full catalog list (served, then pending), sent whenever it changes.
- The server can add or remove catalogs on a live socket. When it removes one, in-flight requests on its sessions get `ERR`, later requests on those `ns` get "unknown session", and parked `OPEN`s get `OPEN_ERR`.
- No wire changes in this entry.
- New SQL: `n6k_serve_pause`, `n6k_serve_resume`, `n6k_serve_inflight`, `n6k_serve_cancel_inflight`, each taking `(fd, catalog)`.
- No wire changes in this entry. New SQL, for `n6k_serve_fd` hosts:
  - `n6k_serve_fd(..., resolve => true)`: an `OPEN` for a catalog that is not served waits (`OPEN_PENDING`) and the host is asked to resolve it, once per name, with a JSON `{"resolve": name}` on stream frame type `0x03`, which only ever goes from the extension to its host. `n6k_serve_resolved(fd, name, error)` settles it: the waiting `OPEN`s get `OPEN_ACK` if the catalog is served by then, else `OPEN_ERR` ("not found", or `error`).
  - Upstreams: `n6k_upstream_hello(up_fd, token)` returns `(catalogs, capabilities)` from another n6k server on a stream-framed socket; `n6k_serve_add_upstream(fd, up_fd, catalogs)` serves them by forwarding frames unmodified and returns the names skipped as already served; `n6k_serve_remove_upstream(fd, up_fd)`; `n6k_serve_upstream_status(fd, up_fd)` returns `(open, reason)`. When an upstream goes away its opening sessions get `OPEN_ERR` and in-flight requests `ERR`. `n6k_serve_remove_catalog` and `n6k_serve_pause` refuse an upstream's catalogs.
  - Ranks: `n6k_serve_add_upstream(..., rank => N)` (default 0) and `n6k_serve_set_upstream_rank(fd, up_fd, N)`, both returning the reported names the upstream does not win. A local catalog always wins a name; otherwise the lowest rank among lazy names and upstreams wins (then the earliest added), re-decided on every change; a name that changes hands closes its sessions on the loser.
  - Lazy catalogs: `n6k_serve_fd(..., lazy => MAP {name: rank})` (needs `resolve => true`; a NULL rank loses to every upstream) and `n6k_serve_add_lazy_catalog(fd, name, rank => N)` list names in `CATALOG_LIST` / `CATALOGS_CHANGED` before anything serves them; opening one resolves it. an upstream of better rank takes a lazy name over; one nothing serves is listed as lazy again; `n6k_serve_remove_catalog` stops listing it.
- Fixed: a remove during a transaction stream could hang the connection.
