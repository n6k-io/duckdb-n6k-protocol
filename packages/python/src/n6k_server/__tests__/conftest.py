"""Fixtures for tests co-located with the n6k-duckdb package.

CLI options (`--from-bucket`, `--external`) and the package-wide
`N6K_EXTENSION_DIR` pin are in the rootdir conftest at
`packages/python/conftest.py`.
"""

from urllib.parse import urlparse

import duckdb
import pytest

from n6k_server.extension import load_n6k, load_n6k_testing


@pytest.fixture
def n6k_con():
    """In-memory connection with the `n6k` and `n6k_testing` extensions loaded.

    The SQL builders live in C++ (src/common/n6k_sql_builder.cpp) and reach
    Python only through the `n6k_testing_build_*_sql` scalars, which the
    unpublished `n6k_testing` extension registers, so testing them means running
    them.
    """
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_n6k(con)
    load_n6k_testing(con)
    yield con
    con.close()


def _parse_external_url(url):
    """Parse an external DB URL into (db_type, attach_string, schema).

    Supports:
        postgres://user@host/dbname
        mysql://user@host/dbname
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme in ("postgres", "postgresql"):
        db_type = "postgres"
        parts = []
        if parsed.hostname:
            parts.append(f"host={parsed.hostname}")
        if parsed.port:
            parts.append(f"port={parsed.port}")
        if parsed.path and parsed.path.strip("/"):
            parts.append(f"dbname={parsed.path.strip('/')}")
        if parsed.username:
            parts.append(f"user={parsed.username}")
        if parsed.password:
            parts.append(f"password={parsed.password}")
        attach_string = " ".join(parts)
        schema = "public"
    elif scheme == "mysql":
        db_type = "mysql"
        parts = []
        if parsed.hostname:
            parts.append(f"host={parsed.hostname}")
        if parsed.port:
            parts.append(f"port={parsed.port}")
        db_name = parsed.path.strip("/") if parsed.path else "test"
        parts.append(f"database={db_name}")
        if parsed.username:
            parts.append(f"user={parsed.username}")
        if parsed.password:
            parts.append(f"password={parsed.password}")
        attach_string = " ".join(parts)
        schema = db_name
    else:
        raise ValueError(f"Unsupported external DB scheme: {scheme}")
    return db_type, attach_string, schema


@pytest.fixture
def external_db(request):
    """Fixture providing a DuckDB connection with an external DB attached.

    Yields a dict with: type, attach_string, catalog, schema, source.
    Cleans up any tables created during the test.
    """
    url = request.config.getoption("--external")
    if not url:
        pytest.skip("--external not provided")

    db_type, attach_string, schema = _parse_external_url(url)
    catalog = "ext"

    source = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    source.execute(f"ATTACH '{attach_string}' AS {catalog} (TYPE {db_type})")

    tables_created = []

    def create_table(name, ddl):
        """Create a table on the external DB and track it for cleanup."""
        qualified = f'"{catalog}"."{schema}"."{name}"'
        source.execute(f"DROP TABLE IF EXISTS {qualified}")
        source.execute(f"CREATE TABLE {qualified}({ddl})")
        tables_created.append(qualified)
        return qualified

    info = {
        "type": db_type,
        "attach_string": attach_string,
        "catalog": catalog,
        "schema": schema,
        "source": source,
        "create_table": create_table,
    }

    yield info

    # Cleanup
    for qualified in tables_created:
        try:
            source.execute(f"DROP TABLE IF EXISTS {qualified}")
        except Exception:
            pass
    source.close()
