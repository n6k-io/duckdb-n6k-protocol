"""End-to-end: a `TYPE n6k` secret supplies the bearer token to a
token-protected mount, so ATTACH needs no inline token. Also covers back-compat
(inline token), rejection without a credential, rotation via CREATE OR REPLACE,
and SCOPE matching. Phase 2/3 cover the OAuth refresh-secret mint
(discovery + RFC 8693 exchange) and the n6k_login device flow.

Targets the always-on auth mounts on the running :8099 test server (see
test_server/app.py): the `/auth` prefix requires the static AUTH_TOKEN and the
`/oauth` prefix requires an OAuth JWT minted by the AS mounted on the same
server. No server is spawned.

Run:
    uv run pytest integration_tests/test_secret_auth.py -v
"""

import asyncio
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import duckdb
import jwt
import pytest
import websockets

from _paths import EXT_PATH
from n6k_protocol.engine import pack_frame, unpack_frame
from n6k_protocol.protocol import FT_HELLO_ERR
from n6k_server.test_server.oauth import (
    ACCEPTED_REFRESH_TOKEN,
    ACCEPTED_SUBJECT_TOKEN,
    AUDIENCE,
    ROTATING_REFRESH_SEED,
    SHORT_JWT_TTL_SECONDS,
    SHORT_LIVED_REFRESH_TOKEN,
)

SERVER_PORT = 8099
SERVER_URL = f"http://localhost:{SERVER_PORT}"
# Static token the /auth prefix requires (test_server/app.py AUTH_TOKEN).
AUTH_TOKEN = "n6k-test-token"
# The AS that backs the /oauth verifier is mounted on the same server.
ISSUER = f"http://localhost:{SERVER_PORT}"
TOKEN_ENDPOINT = f"{ISSUER}/oauth/token"


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


def test_bare_host_scope_option_form():
    c = _connect()
    # Bare-host SCOPE matches any port and scheme even when ATTACH supplies host/port
    # via options (no n6k:// path). The `prefix` option targets the token-gated /auth mount.
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


# ── Phase 4: RFC 6749 §6 refresh-token grant → attach /oauth ──────────────────


def _create_refresh_secret(c, refresh_token, persistent=False):
    c.execute(
        f"CREATE {'PERSISTENT ' if persistent else ''}SECRET rt (TYPE n6k_refresh, "
        f"REFRESH_TOKEN '{refresh_token}', TOKEN_ENDPOINT '{TOKEN_ENDPOINT}', CLIENT_ID 'n6k-test', "
        f"RESOURCE '{AUDIENCE}', SCOPE 'localhost:{SERVER_PORT}')"
    )


def _attach_oauth_and_check(c):
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS db (TYPE n6k)")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


def test_refresh_grant_needs_no_subject_token_or_issuer():
    c = _connect()
    _create_refresh_secret(c, ACCEPTED_REFRESH_TOKEN)
    _attach_oauth_and_check(c)


def test_refresh_grant_without_new_token_keeps_stored_token(tmp_path):
    c = _connect()
    c.execute(f"SET secret_directory = '{tmp_path}'")
    _create_refresh_secret(c, ACCEPTED_REFRESH_TOKEN, persistent=True)
    _attach_oauth_and_check(c)
    assert ACCEPTED_REFRESH_TOKEN.encode() in (tmp_path / "rt.duckdb_secret").read_bytes()


def test_rotated_refresh_token_replaces_persistent_secret(tmp_path):
    secret_file = tmp_path / "rt.duckdb_secret"
    first = _connect()
    first.execute(f"SET secret_directory = '{tmp_path}'")
    _create_refresh_secret(first, ROTATING_REFRESH_SEED, persistent=True)
    assert ROTATING_REFRESH_SEED.encode() in secret_file.read_bytes()
    _attach_oauth_and_check(first)
    rotated_contents = secret_file.read_bytes()
    assert ROTATING_REFRESH_SEED.encode() not in rotated_contents

    # A fresh instance can only load the rotated single-use token from disk; it rotates again.
    second = _connect()
    second.execute(f"SET secret_directory = '{tmp_path}'")
    _attach_oauth_and_check(second)
    assert secret_file.read_bytes() != rotated_contents


def test_concurrent_attaches_share_one_rotation():
    c = _connect()
    _create_refresh_secret(c, ROTATING_REFRESH_SEED)
    _attach_oauth_and_check(c)
    minted_name = c.execute("SELECT name FROM duckdb_secrets() WHERE type = 'n6k'").fetchone()[0]

    # The stored refresh token is now single-use, so a thread reading it from a stale snapshot gets invalid_grant.
    for round_number in range(5):
        c.execute(f'DROP SECRET "{minted_name}"')
        cursors = [c.cursor() for _ in range(4)]
        barrier = threading.Barrier(len(cursors))

        def attach(index):
            barrier.wait()
            alias = f"db_{round_number}_{index}"
            cursors[index].execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS {alias} (TYPE n6k)")

        with ThreadPoolExecutor(max_workers=len(cursors)) as pool:
            for future in [pool.submit(attach, index) for index in range(len(cursors))]:
                future.result(timeout=30)


def test_invalid_grant_says_log_in_again():
    c = _connect()
    _create_refresh_secret(c, "unknown-refresh-token")
    with pytest.raises(duckdb.Error, match="log in again"):
        c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS db (TYPE n6k)")


@pytest.mark.parametrize(
    ("options", "missing"),
    [
        ("CLIENT_ID 'n6k-test'", "token_endpoint"),
        (f"TOKEN_ENDPOINT '{TOKEN_ENDPOINT}'", "client_id"),
    ],
)
def test_refresh_token_requires_endpoint_and_client_id(options, missing):
    c = _connect()
    with pytest.raises(duckdb.Error, match=f"requires {missing}"):
        c.execute(f"CREATE SECRET rt (TYPE n6k_refresh, REFRESH_TOKEN 'x', {options})")


def test_refresh_token_is_redacted():
    c = _connect()
    _create_refresh_secret(c, ACCEPTED_REFRESH_TOKEN)
    secret_string = c.execute("SELECT secret_string FROM duckdb_secrets() WHERE name = 'rt'").fetchone()[0]
    assert ACCEPTED_REFRESH_TOKEN not in secret_string


# ── Phase 5: a redial re-resolves a secret-sourced token ──────────────────────


def _wait_for_short_jwt_expiry():
    time.sleep(SHORT_JWT_TTL_SECONDS + 1)


def _close_all_server_sockets():
    urllib.request.urlopen(
        urllib.request.Request(f"{SERVER_URL}/debug/ws/close_all", method="POST"),
        timeout=2,
    ).read()


def test_query_after_drop_refreshes_expired_token():
    c = _connect()
    _create_refresh_secret(c, SHORT_LIVED_REFRESH_TOKEN)
    _attach_oauth_and_check(c)
    _wait_for_short_jwt_expiry()
    _close_all_server_sockets()
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3


def test_n6k_reconnect_refreshes_expired_token():
    c = _connect()
    _create_refresh_secret(c, SHORT_LIVED_REFRESH_TOKEN)
    conn_id = c.execute(f"SELECT * FROM n6k_connect(url => 'n6k://localhost:{SERVER_PORT}/oauth')").fetchone()[0]
    _wait_for_short_jwt_expiry()
    c.execute("SELECT * FROM n6k_reconnect(?)", [str(conn_id)]).fetchall()


def test_inline_token_is_not_refreshed_on_redial():
    body = urllib.parse.urlencode(
        {"grant_type": "refresh_token", "refresh_token": SHORT_LIVED_REFRESH_TOKEN, "resource": AUDIENCE}
    ).encode()
    with urllib.request.urlopen(urllib.request.Request(TOKEN_ENDPOINT, data=body), timeout=2) as response:
        inline_token = json.loads(response.read())["access_token"]

    c = _connect()
    # Would rescue the redial if the inline token were wrongly re-resolved from secrets.
    _create_refresh_secret(c, ACCEPTED_REFRESH_TOKEN)
    c.execute(f"ATTACH 'n6k://localhost:{SERVER_PORT}/oauth' AS db (TYPE n6k, TOKEN '{inline_token}')")
    assert c.execute("SELECT count(*) FROM db.main.users").fetchone()[0] == 3
    _wait_for_short_jwt_expiry()
    _close_all_server_sockets()
    with pytest.raises(duckdb.Error):
        c.execute("SELECT count(*) FROM db.main.users").fetchone()


# ── SCOPE sends a token only to the server it names ───────────────────────────


class TokenListener:
    """Records the token in every HELLO, then refuses it."""

    def __init__(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.tokens = []
        threading.Thread(target=lambda: asyncio.run(self._serve()), daemon=True).start()
        time.sleep(0.3)

    async def _handle(self, ws):
        try:
            header, _ = unpack_frame(await ws.recv())
            self.tokens.append(header.get("token"))
            await ws.send(pack_frame({"t": FT_HELLO_ERR, "exception_type": "x", "exception_message": "refused"}))
        except websockets.ConnectionClosed:
            pass

    async def _serve(self):
        async with websockets.serve(self._handle, "127.0.0.1", self.port):
            await asyncio.Future()


@pytest.fixture(scope="module")
def token_listener():
    return TokenListener()


def _isolated_connect():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true", "allow_persistent_secrets": "false"})
    c.load_extension("httpfs")
    c.load_extension(EXT_PATH)
    return c


def _token_sent(c, listener, url):
    listener.tokens.clear()
    with pytest.raises(duckdb.Error, match="refused"):
        c.execute(f"ATTACH '{url}' AS x (TYPE n6k)")
    assert len(listener.tokens) == 1
    return listener.tokens[0]


@pytest.mark.parametrize(
    "scope, url, expected",
    [
        ("127.0.0.1", "n6k://127.0.0.1:{port}", "tok"),
        ("127.0.0.1:{port}", "n6k://127.0.0.1:{port}", "tok"),
        ("n6k://127.0.0.1:{port}", "n6k://127.0.0.1:{port}", "tok"),
        ("127.0.0.1:{port}/app", "n6k://127.0.0.1:{port}/app", "tok"),
        ("127.0.0.1:{port}/app", "n6k://127.0.0.1:{port}/app/sub", "tok"),
        ("LOCALHOST", "n6k://localhost:{port}", "tok"),
        ("local", "n6k://localhost:{port}", ""),
        ("localhost:{short_port}", "n6k://localhost:{port}", ""),
        ("n6ks://127.0.0.1:{port}", "n6k://127.0.0.1:{port}", ""),
        ("127.0.0.1:{port}/app", "n6k://127.0.0.1:{port}/application", ""),
        ("127.0.0.1:{port}/app", "n6k://127.0.0.1:{port}/other", ""),
    ],
)
def test_scope_decides_which_server_gets_the_token(token_listener, scope, url, expected):
    port = token_listener.port
    values = {"port": port, "short_port": str(port)[:2]}
    c = _isolated_connect()
    c.execute(f"CREATE SECRET s (TYPE n6k, TOKEN 'tok', SCOPE '{scope.format(**values)}')")
    assert _token_sent(c, token_listener, url.format(**values)) == expected


def test_most_specific_scope_wins(token_listener):
    port = token_listener.port
    c = _isolated_connect()
    c.execute("CREATE SECRET broad (TYPE n6k, TOKEN 'broad', SCOPE '127.0.0.1')")
    c.execute(f"CREATE SECRET narrow (TYPE n6k, TOKEN 'narrow', SCOPE '127.0.0.1:{port}')")
    assert _token_sent(c, token_listener, f"n6k://127.0.0.1:{port}") == "narrow"


def test_omitted_scope_is_a_catch_all(token_listener):
    c = _isolated_connect()
    c.execute("CREATE SECRET s (TYPE n6k, TOKEN 'tok')")
    assert _token_sent(c, token_listener, f"n6k://127.0.0.1:{token_listener.port}") == "tok"


@pytest.mark.parametrize(
    "path, matches",
    [
        ("n6k://prod.example.com:80/", True),
        ("n6ks://prod.example.com:443/", True),
        ("n6k://prod.example.com.evil.net:80/", False),
        ("n6k://prod.example.community:80/", False),
    ],
)
def test_scope_stops_at_the_host_boundary(path, matches):
    c = _isolated_connect()
    c.execute("CREATE SECRET s (TYPE n6k, TOKEN 'tok', SCOPE 'prod.example.com')")
    assert bool(c.execute("SELECT * FROM which_secret(?, 'n6k')", [path]).fetchall()) is matches


def test_tls_scope_stores_only_the_tls_prefix():
    c = _isolated_connect()
    c.execute("CREATE SECRET s (TYPE n6k, TOKEN 'tok', SCOPE 'n6ks://prod.example.com:443')")
    assert c.execute("SELECT scope FROM duckdb_secrets() WHERE name = 's'").fetchone()[0] == [
        "n6ks://prod.example.com:443/"
    ]


@pytest.mark.parametrize(
    "url",
    [
        "n6k://prod.example.com@127.0.0.1:{port}",
        "n6k://127.0.0.1:99999",
        "n6k://127.0.0.1:{port}x",
    ],
)
@pytest.mark.parametrize("options", ["", ", anonymous true", ", token 't'"])
def test_a_malformed_url_is_refused_before_dialing(token_listener, url, options):
    token_listener.tokens.clear()
    c = _isolated_connect()
    with pytest.raises(duckdb.InvalidInputException, match="invalid URL"):
        c.execute(f"ATTACH '{url.format(port=token_listener.port)}' AS x (TYPE n6k{options})")
    assert token_listener.tokens == []


@pytest.mark.parametrize("scope", ["host/path", "bad@host", "host:99999"])
def test_a_malformed_scope_is_refused(scope):
    c = _isolated_connect()
    with pytest.raises(duckdb.Error, match="n6k"):
        c.execute(f"CREATE SECRET s (TYPE n6k, TOKEN 'tok', SCOPE '{scope}')")


def test_n6k_connect_url_with_scope_is_refused(token_listener):
    token_listener.tokens.clear()
    c = _isolated_connect()
    c.execute("CREATE SECRET s (TYPE n6k, TOKEN 'tok', SCOPE 'prod.example.com')")
    with pytest.raises(duckdb.BinderException, match="scope only applies to wsFd"):
        c.execute(
            f"SELECT * FROM n6k_connect(url => 'n6k://127.0.0.1:{token_listener.port}', scope => 'prod.example.com')"
        )
    assert token_listener.tokens == []
