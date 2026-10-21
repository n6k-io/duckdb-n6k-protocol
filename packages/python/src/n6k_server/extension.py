"""Install and load the DuckDB extensions this package drives.

``n6k_client`` is the client driver for ``ATTACH 'n6k://…'`` (only the extension is
renamed; the storage type, secret type and URL scheme are still ``n6k``).
``n6k_server`` serves attached catalogs over the n6k protocol. ``n6k_testing``
is test-only and never published. ``virtual_catalog_bridge`` and
``virtual_catalog_provider`` come from the sibling ``duckdb-virtual-catalog`` repo.

Local builds resolve by name from ``$N6K_EXTENSION_DIR`` (this repo's
``build/release/extension``) and ``$VIRTUAL_CATALOG_EXT_DIR`` (the sibling repo's);
unset, they install from the GCS bucket. Connections must allow unsigned extensions.
"""

import logging
import os

import duckdb

log = logging.getLogger(__name__)

EXTENSION_REPO = "https://storage.googleapis.com/n6k-duckdb-release"
EXTENSION_DIR_ENV = "N6K_EXTENSION_DIR"
VIRTUAL_CATALOG_EXT_DIR_ENV = "VIRTUAL_CATALOG_EXT_DIR"

VIRTUAL_CATALOG_BRIDGE = "virtual_catalog_bridge"
VIRTUAL_CATALOG_PROVIDER = "virtual_catalog_provider"

_EXTENSION_DIR_ENVS = {
    VIRTUAL_CATALOG_BRIDGE: VIRTUAL_CATALOG_EXT_DIR_ENV,
    VIRTUAL_CATALOG_PROVIDER: VIRTUAL_CATALOG_EXT_DIR_ENV,
}


def local_extension_path(name: str) -> str | None:
    base = os.environ.get(_EXTENSION_DIR_ENVS.get(name, EXTENSION_DIR_ENV))
    if not base:
        return None
    path = os.path.join(base, name, f"{name}.duckdb_extension")
    return path if os.path.exists(path) else None


def _load_local_or_install(conn: duckdb.DuckDBPyConnection, name: str) -> None:
    local = local_extension_path(name)
    if local:
        log.debug("loading duckdb extension %r from local build %s", name, local)
        conn.load_extension(local)
    else:
        log.debug("installing duckdb extension %r from %s", name, EXTENSION_REPO)
        conn.install_extension(name, repository_url=EXTENSION_REPO)
        conn.load_extension(name)


def load_virtual_catalog_bridge(conn: duckdb.DuckDBPyConnection) -> None:
    _load_local_or_install(conn, VIRTUAL_CATALOG_BRIDGE)


def load_virtual_catalog_provider(conn: duckdb.DuckDBPyConnection) -> None:
    _load_local_or_install(conn, VIRTUAL_CATALOG_PROVIDER)


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
