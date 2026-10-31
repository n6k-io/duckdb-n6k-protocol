# n6k DuckDB Extensions

DuckDB extensions for remote and in-process data access.

## Extensions

**n6k_server** — serves attached catalogs over the n6k protocol; see [docs/n6k-server.md](docs/n6k-server.md).

**n6k_client** — WebSocket bridge for querying remote servers via Arrow IPC streaming.

- Attach remote databases: `ATTACH 'n6k://host:port/path' AS db (TYPE n6k, token '...');`
- Credentials via DuckDB secrets (keeps the token out of SQL/logs):
  `CREATE SECRET (TYPE n6k, TOKEN '...', SCOPE 'host:port');` then attach without a token
- All attached-catalog traffic multiplexed over one WebSocket per ATTACH
- Plan pushdown (crossing)
- INSERT, UPDATE, DELETE, CREATE TABLE AS, views
- RPC table functions with dynamic binding
- WASM/browser support (Emscripten)
- Attach onto a WebSocket the host app already opened (browser): `await registerWebsocket(ws, token)` (sends the socket's one HELLO) then `ATTACH '' AS db (TYPE n6k, wsId '…')` (sends OPEN); see [n6k-network.md](docs/n6k-network.md#attaching-an-existing-websocket-wasmbrowser)
- Attach onto a connected socket fd the host owns (native): `SELECT * FROM n6k_connect(wsFd => <fileno>, token => '…')` then `ATTACH '' AS db (TYPE n6k, wsFd <fileno>)` — the fd-level analogue of `wsId`, see [n6k-network.md](docs/n6k-network.md#attaching-an-existing-socket-fd-native)

The in-process bridge and provider catalogs (`vcatbridge`,
`vcatprovider`) live in the sibling `duckdb-virtual-catalog` repo. The
Python package drives them; point `VIRTUAL_CATALOG_EXT_DIR` at that repo's
`build/release/extension` to use a local build.

## Authentication (n6k)

The bearer token can be supplied inline on the ATTACH, or — preferred — via a
DuckDB secret so it stays out of SQL text and logs (redacted in
`duckdb_secrets()`):

```sql
-- store the credential once (SCOPE is the host; scheme is ignored)
CREATE SECRET my_n6k (TYPE n6k, TOKEN 'your_token', SCOPE 'host:port');

-- attach with no token in the statement
ATTACH 'n6k://host:port' AS db (TYPE n6k);

-- rotate in place; the next ATTACH uses the new value (attached catalogs keep theirs, also on reconnect)
CREATE OR REPLACE SECRET my_n6k (TYPE n6k, TOKEN 'new_token', SCOPE 'host:port');
```

Precedence: an inline `token '…'` option wins; otherwise a matching `TYPE n6k`
secret is used; otherwise the connection is anonymous. `SCOPE` is matched as a
prefix of the host (`'host'` matches any port/path; omit it for a catch-all).

To force an **anonymous** connection — no token, skipping the secret lookup and
any `n6k_refresh` OAuth mint — pass `anonymous true`:

```sql
ATTACH 'n6k://host:port' AS db (TYPE n6k, anonymous true);
```

Useful for local/test servers that need no auth, and it prevents a stale
`n6k_refresh` secret from hijacking the attach (an unreachable issuer would
otherwise fail the connect during OAuth discovery). Mutually exclusive with the
inline `token` option.

For OAuth deployments, a `TYPE n6k_refresh` secret (native only) holds a durable
subject token and auto-mints/renews short-lived data-service JWTs via standard
OAuth discovery + RFC 8693 token exchange — see
[n6k-network.md](docs/n6k-network.md#oauth-standards-based-native).
`n6k_login` runs the interactive device flow and stores that secret for you:

```sql
-- opens a verification URL, waits for approval, stores a persistent n6k_refresh secret
SELECT * FROM n6k_login('https://auth.example', resource := 'data-service');
ATTACH 'n6k://data.example:443' AS db (TYPE n6k);  -- mints + renews automatically
```

`n6k_login` is native-only
(throws in the browser, where the host app owns the OAuth lifecycle).

## Connection readiness & timeouts (n6k)

A dial `ATTACH` sends the socket's `FT_HELLO {token}` and then
`FT_OPEN {ns, catalog}`, and returns once the server answers `OPEN_ACK`. A
rejected token (`HELLO_ERR`) or an unserved catalog (`OPEN_ERR`) fails the
`ATTACH` with the server's message. Handshake timeouts: [Lifecycle](docs/n6k-network-protocol.md#lifecycle).

## Building

Prerequisites: CMake, C++17 compiler, Python with `uv`.

```sh
git clone --recurse-submodules https://github.com/n6k-io/duckdb-n6k-protocol.git
# or, in an existing clone:
git submodule update --init --recursive

make release
```

Output:

```
build/release/extension/n6k_client/n6k_client.duckdb_extension
build/release/extension/n6k_server/n6k_server.duckdb_extension
```

## Testing

First start the test server (defaults to `:8099`) in another shell — the
integration and TypeScript suites connect to it:

```sh
uv run test-server
```

Then, per area (each block starts from the repo root):

`packages/python` — unit + bridge:

```sh
cd packages/python
uv run pytest
uv run pytest --external "postgres://user@host/db"  # also test an external DB (opt-in)
```

`integration_tests` — driver ⇄ server:

```sh
uv run pytest integration_tests
```

`packages/npm` — TypeScript driver:

```sh
cd packages/npm
TEST_SERVER=http://localhost:8099 BACKEND=native bun run test
bun integration-tests
```

## Golden state

Golden = every suite above passes, plus format / lint / types below.

C++ and protocol mirrors — these `make` targets need the venv (for `python`
plus the clang tools):

```sh
source .venv/bin/activate
make format-check    # clang-format (make format-fix to apply)
make tidy-check      # clang-tidy
make protocol-check  # generated protocol mirrors in sync
make protocol-gen    # regenerate the generated mirrors after editing protocol.py
```

The `make` targets call bare `python`, so the venv must be activated first —
without it you'll get `python: No such file or directory`.

`packages/python`:

```sh
cd packages/python
uv run black --check .
uv run flake8 .
uv run mypy . --strict
```

`packages/npm`:

```sh
cd packages/npm
bun run ci  # prettier + eslint + tsc + test
```

## Docs

- [n6k-network.md](docs/n6k-network.md) — `TYPE n6k` network extension: `ATTACH`, auth/secrets, `wsId`/`wsFd`, function registry.
- [n6k-network-protocol.md](docs/n6k-network-protocol.md) — the on-the-wire protocol for `TYPE n6k` (frame format, ops, lifecycle).
- [n6k-server.md](docs/n6k-server.md) — the `n6k_server` extension.
- [n6k-conformance.md](docs/n6k-conformance.md) — the conformance harness.
- [packages/python/docs/api.md](packages/python/docs/api.md) — Python API reference (`n6k_protocol` + `n6k_server`) (bridge helper, Provider, server framework).

## Python

```sh
cd packages/python && uv pip install -e .
```

```python
from virtual_catalog.bridge import Policy, bridge

bridge(source, target, "my_bridge", source_catalog="memory", policies=[
    Policy("main.users", "select", "true"),
    Policy("main.users", "insert", "true"),
    Policy("main.users", "update", "true"),
    Policy("main.users", "delete", "true"),
    Policy("main.logs", "select", "true"),
])

target.sql("SELECT * FROM my_bridge.main.users")
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for system design, data flow, and key file map.

## Relationship to Quack

[Quack](https://duckdb.org/2026/05/12/quack-remote-protocol) replaces one layer:
plain remote attach — read, write, wasm. Real overlap, and it's free in v2.0.

It can't replace anything above that, because Quack's connection is a request and
n6k's is a stateful, isolated session. Per-client sandboxed databases, streaming
RPCs, session identity, socket adoption — all downstream of that one difference.

So you can't swap the bottom layer out either: it's what supplies the semantics
the rest depends on.

Quack connects DuckDBs inside a trust boundary. n6k serves DuckDB to clients you
don't trust.
