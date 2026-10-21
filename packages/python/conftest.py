"""Rootdir conftest: register CLI options used by tests under src/*/__tests__/.

Pytest requires `pytest_addoption` / `pytest_configure` hooks to live at or
above the rootdir. Our fixtures and most per-test setup live alongside the
code they exercise (see `src/n6k_server/__tests__/conftest.py`).

Extension resolution is the exception and lives here: `DuckDBHandler` builds
every statement through the `n6k_testing` extension's `n6k_testing_build_*_sql`
scalars, so any
test that touches a handler needs one — including the ones under
`src/n6k_server/server_fastapi/__tests__/`. Pinning it package-wide also keeps
those tests off the network, which a GCS-bucket fallback would not.
"""

import os
from collections.abc import Iterator

import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.fixtures import FixtureRequest
from dotenv import load_dotenv

from n6k_server.extension import VIRTUAL_CATALOG_EXT_DIR_ENV

HERE = os.path.dirname(__file__)
# From this file: packages/python/conftest.py → repo root is 2 directories up.
EXT_DIR = os.path.realpath(os.path.join(HERE, "..", "..", "build", "release", "extension"))

load_dotenv(os.path.join(HERE, ".env"))


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        "--from-bucket",
        action="store_true",
        default=False,
        help="Load extension from GCS bucket instead of local build",
    )
    parser.addoption(
        "--external",
        default=None,
        help="External DB URL for integration tests (e.g. 'postgres://sean@localhost/n6k_test')",
    )


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "external: requires --external flag with a database URL")


@pytest.fixture(autouse=True)
def _extension_source(request: FixtureRequest) -> Iterator[None]:
    from_bucket = request.config.getoption("--from-bucket")
    vcat_dir = os.environ.get(VIRTUAL_CATALOG_EXT_DIR_ENV)
    if from_bucket:
        os.environ.pop("N6K_EXTENSION_DIR", None)
        os.environ.pop(VIRTUAL_CATALOG_EXT_DIR_ENV, None)
    else:
        if not os.path.isdir(EXT_DIR):
            pytest.skip(f"Extensions not built: {EXT_DIR}")
        if not vcat_dir:
            pytest.skip(f"{VIRTUAL_CATALOG_EXT_DIR_ENV} not set (packages/python/.env)")
        if not os.path.isdir(vcat_dir):
            pytest.skip(f"virtual_catalog extensions not built: {vcat_dir}")
        os.environ["N6K_EXTENSION_DIR"] = EXT_DIR
    yield
    os.environ.pop("N6K_EXTENSION_DIR", None)
    if from_bucket and vcat_dir:
        os.environ[VIRTUAL_CATALOG_EXT_DIR_ENV] = vcat_dir
