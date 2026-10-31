"""Install and load the DuckDB extensions this package drives.

``n6k_client`` is the client driver for ``ATTACH 'n6k://…'``; its storage type,
secret type and URL scheme are ``n6k``.
``n6k_server`` serves attached catalogs over the n6k protocol. ``n6k_testing``
is test-only and never published. ``vcatbridge`` and ``vcatprovider`` load via
``virtual_catalog.extension``.

Local builds resolve by name from ``$N6K_EXTENSION_DIR`` (this repo's
``build/release/extension``); unset, they install from the GCS bucket.
Connections must allow unsigned extensions.
"""

import logging

import duckdb
from virtual_catalog.extension import EXTENSION_REPO, local_extension_path

log = logging.getLogger(__name__)

EXTENSION_DIR_ENV = "N6K_EXTENSION_DIR"


def _load_local_or_install(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    local = local_extension_path(name, EXTENSION_DIR_ENV)
    if local:
        log.debug("loading duckdb extension %r from local build %s", name, local)
        conn.load_extension(local)
    else:
        log.debug("installing duckdb extension %r from %s", name, EXTENSION_REPO)
        conn.install_extension(name, repository_url=EXTENSION_REPO)
        conn.load_extension(name)


def load_n6k(conn: duckdb.DuckDBPyConnection) -> None:
    _load_local_or_install(conn, "n6k_client")


def load_n6k_server(conn: duckdb.DuckDBPyConnection) -> None:
    _load_local_or_install(conn, "n6k_server")


def load_n6k_testing(conn: duckdb.DuckDBPyConnection) -> None:
    """Load the test-only extension (fixtures and SQL-builder keyholes).

    ``n6k_testing`` is never published to ``EXTENSION_REPO``, so this only
    resolves from a local build via ``$N6K_EXTENSION_DIR``.
    """
    _load_local_or_install(conn, "n6k_testing")
