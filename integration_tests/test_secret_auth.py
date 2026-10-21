"""End-to-end: a `TYPE n6k` secret supplies the bearer token to a
token-protected mount, so ATTACH needs no inline token. Also covers back-compat
(inline token), rejection without a credential, rotation via CREATE OR REPLACE,
and scheme-agnostic SCOPE matching. Phase 2/3 cover the OAuth refresh-secret mint
(discovery + RFC 8693 exchange) and the n6k_login device flow.

Targets the always-on auth mounts on the running :8099 test server (see
test_server/app.py): the `/auth` prefix requires the static AUTH_TOKEN and the
`/oauth` prefix requires an OAuth JWT minted by the AS mounted on the same
server. No server is spawned.

Run:
    uv run pytest integration_tests/test_secret_auth.py -v
"""

import os
import time
import urllib.error
import urllib.request

import duckdb
import jwt
import pytest

from _paths import EXT_PATH
from n6k_server.test_server.oauth import ACCEPTED_SUBJECT_TOKEN, AUDIENCE

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
# Static token the /auth prefix requires (test_server/app.py AUTH_TOKEN).
AUTH_TOKEN = "n6k-test-token"
# The AS that backs the /oauth verifier is mounted on the same server.
ISSUER = f"http://localhost:{SERVER_PORT}"


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


# ── Phase 1: TYPE n6k secret supplies a static bearer token to /auth ──────────


def test_secret_supplies_token():
    c = _connect()
    c.execute(f"CREATE SECRET s1 (TYPE n6k, TOKEN '{AUTH_TOKEN}', SCOPE 'localhost:{SERVER_PORT}')")
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db (TYPE n6k)")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


def test_attach_without_credentials_is_rejected():
    c = _connect()
    with pytest.raises(duckdb.Error):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db (TYPE n6k)")


def test_wrong_secret_token_is_rejected():
    c = _connect()
    c.execute(f"CREATE SECRET s_bad (TYPE n6k, TOKEN 'wrong-token', SCOPE 'localhost:{SERVER_PORT}')")
    with pytest.raises(duckdb.Error):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db (TYPE n6k)")


def test_inline_token_still_works():
    c = _connect()
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db (TYPE n6k, token '{AUTH_TOKEN}')")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


def test_create_or_replace_rotates():
    c = _connect()
    # Wrong token first → attach rejected.
    c.execute(f"CREATE SECRET s2 (TYPE n6k, TOKEN 'wrong-token', SCOPE 'localhost:{SERVER_PORT}')")
    with pytest.raises(duckdb.Error):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db_bad (TYPE n6k)")
    # Replace with the correct token → next attach reads it fresh and succeeds.
    c.execute(f"CREATE OR REPLACE SECRET s2 (TYPE n6k, TOKEN '{AUTH_TOKEN}', SCOPE 'localhost:{SERVER_PORT}')")
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/auth' AS db_ok (TYPE n6k)")
    assert c.execute("SELECT count(*) FROM db_ok.main.users").fetchone()[0] == 3


def test_scheme_agnostic_scope_option_form():
    c = _connect()
    # Bare-host SCOPE matches even when ATTACH supplies host/port via options
    # (no n6k:// path) — both sides are scheme-stripped before matching. The
    # `prefix` option targets the token-gated /auth mount.
    c.execute(f"CREATE SECRET s3 (TYPE n6k, TOKEN '{AUTH_TOKEN}', SCOPE 'localhost')")
    c.execute(f"ATTACH '' AS db (TYPE n6k, host 'localhost', port '{SERVER_PORT}', prefix '/auth')")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


# ── Phase 2: refresh secret → discovery + RFC 8693 exchange → attach /oauth ───


def test_refresh_secret_mints_and_authenticates():
    c = _connect()
    c.execute(
        f"CREATE SECRET r1 (TYPE n6k_refresh, SUBJECT_TOKEN '{ACCEPTED_SUBJECT_TOKEN}', "
        f"ISSUER '{ISSUER}', RESOURCE '{AUDIENCE}', SCOPE 'localhost:{SERVER_PORT}')"
    )
    # No access token anywhere → attach triggers discovery + token exchange, mints
    # a data-service JWT, and authenticates over the /oauth WS.
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS db (TYPE n6k)")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    # The minted JWT is cached as a (redacted) temporary TYPE n6k access secret.
    minted = c.execute("SELECT count(*) FROM duckdb_secrets() WHERE type = 'n6k'").fetchone()[0]
    assert minted == 1


def test_refresh_with_bad_subject_is_rejected():
    c = _connect()
    # The AS only exchanges the accepted subject token; a wrong one fails the
    # exchange, so the attach (which mints on demand) fails.
    c.execute(
        f"CREATE SECRET r2 (TYPE n6k_refresh, SUBJECT_TOKEN 'wrong-subject', "
        f"ISSUER '{ISSUER}', RESOURCE '{AUDIENCE}', SCOPE 'localhost:{SERVER_PORT}')"
    )
    with pytest.raises(duckdb.Error):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS db (TYPE n6k)")


def test_stale_access_token_triggers_remint():
    c = _connect()
    # An already-expired access JWT. The client only base64url-decodes `exp` (no
    # signature check), so any key/alg works to drive the near-expiry path.
    expired = jwt.encode({"aud": AUDIENCE, "exp": int(time.time()) - 10}, "x" * 32, algorithm="HS256")
    c.execute(f"CREATE SECRET a_stale (TYPE n6k, TOKEN '{expired}', SCOPE 'localhost:{SERVER_PORT}')")
    c.execute(
        f"CREATE SECRET r3 (TYPE n6k_refresh, SUBJECT_TOKEN '{ACCEPTED_SUBJECT_TOKEN}', "
        f"ISSUER '{ISSUER}', RESOURCE '{AUDIENCE}', SCOPE 'localhost:{SERVER_PORT}')"
    )
    # The stale access token is past expiry → the attach re-mints from the refresh
    # secret rather than sending the expired one.
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS db (TYPE n6k)")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


# ── Phase 3: n6k_login device flow → refresh secret → mint → attach /oauth ────


def test_n6k_login_device_flow_then_attach(tmp_path):
    c = _connect()
    # Persistent secret → isolate to a temp dir so we don't touch ~/.duckdb.
    c.execute(f"SET secret_directory = '{tmp_path}'")
    # Drives discovery → device authorization → poll-until-approved → stores a
    # persistent n6k_refresh secret (catch-all scope).
    status = c.execute(f"SELECT status FROM n6k_login('{ISSUER}', resource := '{AUDIENCE}')").fetchone()[0]
    assert status == "ok"
    # The stored refresh secret now mints on attach and authenticates.
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS db (TYPE n6k)")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
