# Build & Test

The USER runs builds — never run them yourself. Tell the user which one is needed:
- **`make release`**: native extension.
- **`make wasm`**: wasm extension, staged into `packages/npm/wasm/`. `make release` does not rebuild
  it, so a C++ change that reaches the wasm build needs `make wasm` too.
- After a wasm build, confirm the staged bundle is fresh before trusting a browser-threads run:
  grep the staged `.wasm` for a string unique to your change.

## Shell rules
- Always run `cd` in its own Bash command, never chained with `&&`.
- Bash cwd persists between calls — track where you are before running the next command.

## Formatting & Tidy (C++ only)
Activate the venv in the same command (shell state does not persist between calls):
```
source .venv/bin/activate && make format-fix
source .venv/bin/activate && make tidy-check
```

## Python lint / type-check
`mypy` must run from `packages/python/` (it resolves imports relative to CWD).
```
uv run black .
uv run flake8 .
cd packages/python && uv run mypy . --strict
```

## Tests (use uv, no sandbox)
```
uv run pytest packages/python
uv run pytest integration_tests/        # requires the test server running on :8099
```

## Bun tests (packages/npm) — NO SANDBOX
Use `bun run test`, never a bare `bun test` (the script isolates duckdb-wasm's extension cache).
`TEST_SERVER` and `BACKEND` must both be set.
```
cd packages/npm
TEST_SERVER=http://localhost:8099 BACKEND=native bun run test
TEST_SERVER=http://localhost:8099 BACKEND=browser-threads bun run test
TEST_SERVER=skip BACKEND=skip bun run test
```

## Protocol constants
`packages/python/src/n6k_protocol/protocol.py` is the single source of truth. After editing it, run
`make protocol-gen` and commit the regenerated mirrors; `make protocol-check` verifies them.

## Python duckdb version must match the extension build's duckdb version.
