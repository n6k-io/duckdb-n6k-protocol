"""The `anonymous` ATTACH option: force a no-token connection and skip both the
TYPE n6k secret lookup and any n6k_refresh OAuth mint — at attach time and on
every subsequent request.

Runs against the always-on test server on :8099, which exposes auth modes by URL
prefix (see test_server/app.py):
  (root)  open / no auth
  /auth   static bearer token  (token == n6k-test-token)
We never spawn a server here.

Run:
    uv run pytest integration_tests/test_anonymous_attach.py -v
"""

import os
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
# The static token the /auth prefix requires (test_server/app.py AUTH_TOKEN).
AUTH_TOKEN = "n6k-test-token"


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip("Test server not running on port 8099")


def _connect():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


def test_anonymous_attaches_to_open_server():
    # The root mount needs no auth; anonymous connects and queries fine.
    c = _connect()
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db (TYPE n6k, anonymous true)")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


def test_anonymous_ignores_stale_refresh_secret():
    # A leftover n6k_refresh secret whose issuer is unreachable would otherwise
    # hijack an anonymous attach: the plain attach tries OAuth discovery against
    # the dead issuer and fails the connect. `anonymous true` skips minting
    # entirely, so the same attach connects to the open root mount.
    c = _connect()
    c.execute(
        "CREATE SECRET stale_r (TYPE n6k_refresh, SUBJECT_TOKEN 'x', "
        f"ISSUER 'http://127.0.0.1:1', SCOPE 'localhost:{SERVER_PORT}')"
    )
    # Plain attach: discovery against the dead issuer fails the connect.
    with pytest.raises(duckdb.Error):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db_plain (TYPE n6k)")
    # Anonymous attach: the refresh secret is never consulted, so it succeeds.
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db_anon (TYPE n6k, anonymous true)")
    assert c.execute("SELECT count(*) FROM db_anon.main.users").fetchone()[0] == 3


def test_anonymous_bypasses_valid_secret_so_auth_mount_rejects():
    # A valid TYPE n6k secret is present and would authenticate against the
    # token-gated /auth mount. `anonymous true` must skip it, so the connection
    # has no token and the /auth mount rejects — proving anonymous really
    # bypasses the secret (rather than the open root just accepting anything).
    c = _connect()
    c.execute(f"CREATE SECRET auth_s (TYPE n6k, TOKEN '{AUTH_TOKEN}', SCOPE 'localhost:{SERVER_PORT}')")
    # Sanity: without anonymous the secret authenticates against /auth.
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db_ok (TYPE n6k)")
    assert c.execute("SELECT count(*) FROM db_ok.main.users").fetchone()[0] == 3
    # With anonymous the secret is skipped → no token → /auth rejects.
    with pytest.raises(duckdb.Error):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db_anon (TYPE n6k, anonymous true)")


def test_anonymous_and_token_are_mutually_exclusive():
    c = _connect()
    with pytest.raises(duckdb.Error, match="mutually exclusive"):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}' AS db (TYPE n6k, anonymous true, token 'x')")
