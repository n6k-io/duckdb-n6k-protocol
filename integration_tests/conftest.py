"""Shared config for integration_tests.

These tests run against the already-running test server on :8099 (`uv run
test-server`), which exposes every auth mode by URL prefix — open at the root,
static-token under `/auth`, and OAuth-JWT under `/oauth`, with the OAuth AS
mounted on the same server. Tests pick a mode by attaching to
`n6k://localhost:8099/<mode>`; none of them spawn their own server.
"""

import duckdb
import pytest

from _targets import SERVER_PORT, server_is_up


@pytest.fixture(scope="session")
def server():
    """The running test server's port, or skip.

    Shared so a test file does not carry its own copy of the port number and the
    liveness probe. `_targets.SERVER_PORT` honours `N6K_TEST_PORT`, so pointing the
    suite at a server on another port needs no test edits.
    """
    if not server_is_up():
        pytest.skip(f"Test server not running on port {SERVER_PORT}")
    return SERVER_PORT


@pytest.fixture(scope="session", autouse=True)
def _install_httpfs():
    """Ensure httpfs is installed before any test loads it.

    Every test connection does `load_extension("httpfs")` (the n6k client's
    HTTP transport depends on it), which requires httpfs already installed in
    DuckDB's extension dir. install_extension writes to that shared dir, so one
    install per session lets a clean ~/.duckdb work — otherwise the first test
    fails with 'Extension "httpfs" not found. Install it first using "INSTALL
    httpfs".'
    """
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.install_extension("httpfs")
    finally:
        con.close()
