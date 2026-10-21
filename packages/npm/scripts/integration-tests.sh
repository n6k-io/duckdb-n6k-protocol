#!/usr/bin/env bash
# Run the backend-parametrized tests across both supported backends in turn.
#
# Usage: bun integration-tests [bun-test-args...]
#   bun integration-tests
#   bun integration-tests src/__tests__/catalog-list.test.ts
#   bun integration-tests --bail src/__tests__/all-ops.test.ts
#
# Each backend runs in its own `bun test` process (BACKEND=native|browser-threads),
# so a wasm OOM in one doesn't take down the others. Backends whose prerequisites
# are missing (no :8099 server / no built wasm / no chromium) skip their tests
# cleanly. Exits non-zero if any backend fails. (coi-only: node-eh `wasm-node` and
# browser-eh `browser` were retired — no node runtime loads the wasm_threads
# extension; browser-threads is the wasm backend.)
#
# Requires the test server, named by TEST_SERVER (default http://localhost:8099).
# browser-threads also needs Chromium (install via `bunx playwright install
# chromium`, or set PLAYWRIGHT_BROWSERS_PATH for a nonstandard location).
set -uo pipefail

TEST_SERVER="${TEST_SERVER:-http://localhost:8099}"

fail=0
for backend in native browser-threads; do
  echo ""
  echo "=== BACKEND=$backend (TEST_SERVER=$TEST_SERVER) ==="
  # No HOME isolation: native has no wasm extension cache, and browser-threads
  # caches inside the ephemeral Playwright context, not ~/.duckdb. (A throwaway
  # HOME would also hide the Chromium install from browser-gate's lookup.)
  if ! BACKEND="$backend" TEST_SERVER="$TEST_SERVER" bun test "$@"; then
    fail=1
  fi
done

echo ""
echo "=== summary ==="
if [ "$fail" -ne 0 ]; then
  echo "FAIL — one or more backends failed"
  exit 1
fi
echo "PASS — all backends green"
