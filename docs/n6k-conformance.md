# n6k Conformance Harness

What a server implementation must provide **beyond the wire protocol** to be driven
by this repo's test suites, and how to add one as a target.

The protocol itself is specified in
[`n6k-network-protocol.md`](n6k-network-protocol.md). Everything here is harness
convention — seeded fixture data, a debug control plane, and auth mounts. None of it
is on the wire, and a production server needs none of it. But the suites do, so an
implementation that skips it can only be tested by writing a second suite, which is
exactly the drift this document exists to prevent.

## Targets

`integration_tests/_targets.py` is the registry. Each `Target` names an
implementation and the path it is mounted at on the one test server:

| Target | Mount | Implementation |
|--------|-------|----------------|
| `register` | `` (root) | `src/n6k_server` — the C++ reactor, behind the shipped `server_fastapi.register()` adapter |

`TUNED` (`/tuned`) sits in the same module but is deliberately **not** in `TARGETS`:
it is that same reactor behind that same pump, mounted separately only so the suite
can set per-connection knobs the shipped adapter does not expose (`?catalogs=`,
`?ping_interval_ms=`, `?require_token=`). Nothing about the wire differs, so
parametrizing over it would prove nothing.

Tests parameterized over `TARGETS` run against every entry, so **adding an
implementation is one `Target`**. `WireClient` and `connect()` in the same module
are the shared raw-protocol client: they speak `pack_frame`/`unpack_frame` over a
plain WebSocket with no DuckDB extension and no client-side code involved, which is
what makes those tests implementation-neutral.

`Target.supports` records where an implementation is deliberately and permanently
narrower than the protocol — not where it is merely unfinished. A test skips on a
missing capability instead of being deleted, so the gap stays visible in the report.
The target declares `auth` (the host's `authenticate` callback checks the
`FT_HELLO` token) and `rpc`. Its `rpc` means something different: a name resolves to a table function or macro in the served DuckDB rather than to
an entry in a host-supplied registry. The frames and their answers are identical, so a
wire test cannot tell the two apart — only the set of callable names differs.

The port is `8099`, overridable via `N6K_TEST_PORT` / `N6K_TEST_HOST`. The
`server` fixture in `integration_tests/conftest.py` probes `/debug/counts` and skips
the module if nothing answers — the suite never starts a server itself.

## Fixture data

`packages/python/src/n6k_server/test_server/fixtures.sql` is the single source of
truth, consumed by `seeding.seed_tables` and by the `/tuned` mount's serve prelude.
Assertions across the Python, TypeScript and any future suite hard-code these rows,
so **a change to that file is a change to the conformance contract**:

```
db.main.users            (id INTEGER, name VARCHAR, age INTEGER)
                         (1,'Alice',30), (2,'Bob',25), (3,'Charlie',35)
db.test_schema.products  (id INTEGER, name VARCHAR, price DOUBLE)
                         (1,'Widget',9.99), (2,'Gadget',19.99)
```

The `/tuned` mount adds a `main.marker` table naming its own catalog, so a mux
session reading the wrong catalog is visible in the data rather than as a missing
table. Only that mount seeds it, and its tests assert it.

## Debug control plane

Seven endpoints, all unauthenticated and all outside the protocol. `GET /debug/counts`
doubles as the liveness probe every test module gates on.

| Method | Path | Query | Returns |
|--------|------|-------|---------|
| GET | `/debug/counts` | — | `{"http": {"<METHOD> <path>": n}, "ws": {"credit_pauses": n, "cancels": n}}` |
| POST | `/debug/counts/reset` | — | `{"ok": true}`; zeroes both maps |
| POST | `/debug/server_exec` | `catalog`, `sql` | `{"handlers": n}` |
| GET | `/debug/server_query` | `catalog`, `sql` | `{"handlers": n, "rows": [[...]]}` — rows from the first handler only |
| POST | `/debug/push_invalidate` | `catalog`, `schemas` (comma-separated) | `{"handlers": n, "sent": n}` |
| POST | `/debug/ws/close_all` | — | `{"closed": n, "total": m}` |
| GET | `/debug/bridge_builds` | — | `{"builds": n}` — how many sockets passed `authenticate` (a rejected socket builds nothing) |

Three of them carry semantics a reimplementation has to preserve:

- **`server_exec` must bypass the client.** It runs SQL on the live handler's own
  connection, so the client-side schema cache is left stale. Tests then assert that
  a subsequent client query still sees the old shape until something invalidates it.
  An implementation that routes this through the normal request path silently breaks
  every cache-invalidation test.
- **`push_invalidate` returns a count that is asserted, not logged.** Tests read it
  as "how many live handlers are attached to this catalog" — 1 while attached, 0
  after DETACH. A handler whose socket is mid-close must be skipped without being
  counted and without raising, or a test polling for the count to reach 0 gets a 500
  instead.
- **`ws/close_all` closes with code 1011**, which is what the disconnect/reconnect
  tests match on.

The HTTP counters exist so tests can assert an operation rode the WebSocket rather
than HTTP — a large number of assertions are of the form "this endpoint's count is
0". Count every request by `"<METHOD> <path>"`, including ones that 404.

## Auth mounts

One server, auth mode selected by URL prefix:

| Prefix | Mode |
|--------|------|
| `` (root) | open |
| `/auth` | static bearer, token `n6k-test-token` |
| `/oauth` | EdDSA JWT, issued by the mock authorization server mounted in-process |
| `/tuned` | anonymous, unless `?require_token=` names a credential to demand |
| `/ident` | per-identity catalogs: token `tok-a` serves catalog `a`, `tok-b` serves `b`; anything else is rejected |

On a WebSocket the only credential is the `FT_HELLO` frame's `token` field; an
`Authorization` header or `?token=` on the upgrade is ignored. A rejected token gets
`HELLO_ERR` and the socket closes with the `WsReject` code (4401).

The mock OAuth AS is always mounted (never gated) and serves
`/.well-known/oauth-authorization-server`, `/jwks`, `/oauth/device`, `/oauth/token`.
Only implement it if you intend to run the `/oauth` tests.

## Catalog-name test hooks

The root, `/auth` and `/oauth` mounts serve the catalog named by the dial URL's
`?catalog=` (a missing one closes with 4400), and branch on that name:

| Catalog | Effect |
|---------|--------|
| `slowdb` | provider-backed catalog whose scan sleeps 0.5s, for overlap tests |
| `bridge_db` | bridge-backed catalog, for the network→bridge→table chain test |

## Running

```sh
uv run test-server                       # port 8099; the suites never start it
uv run pytest integration_tests/ -v
cd packages/npm && TEST_SERVER=http://localhost:8099 BACKEND=native bun run test
```

## See also

- [`n6k-network-protocol.md`](n6k-network-protocol.md) — the wire itself.
- [`n6k-server.md`](n6k-server.md) — the C++ target, including where it deliberately
  diverges from the reference.
