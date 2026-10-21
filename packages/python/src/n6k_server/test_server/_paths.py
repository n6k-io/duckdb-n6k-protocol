"""Pin the test server to the locally-built extensions.

``n6k_server.extension`` resolves builds from ``N6K_EXTENSION_DIR`` and otherwise
installs from the GCS bucket — wrong for this test server, which must run the
fresh ``build/release`` artifacts, not whatever the released bucket holds. So we
point ``N6K_EXTENSION_DIR`` at the local build (unless the caller already set it),
failing loudly if it isn't built. ``VIRTUAL_CATALOG_EXT_DIR`` is the sibling repo's
build and comes from ``packages/python/.env``.
"""

import os

from dotenv import load_dotenv

from n6k_server.extension import VIRTUAL_CATALOG_EXT_DIR_ENV

# this file: .../packages/python/src/n6k_server/test_server/_paths.py
HERE = os.path.dirname(__file__)
REPO_ROOT = os.path.realpath(os.path.join(HERE, "..", "..", "..", "..", ".."))
EXTENSION_DIR = os.path.join(REPO_ROOT, "build", "release", "extension")
DOTENV = os.path.join(REPO_ROOT, "packages", "python", ".env")


def pin_local_extension() -> None:
    load_dotenv(DOTENV)
    vcat_dir = os.environ.get(VIRTUAL_CATALOG_EXT_DIR_ENV)
    if not vcat_dir:
        raise FileNotFoundError(f"{VIRTUAL_CATALOG_EXT_DIR_ENV} not set; add it to {DOTENV}")
    if not os.path.isdir(vcat_dir):
        raise FileNotFoundError(f"virtual_catalog extensions not built at {vcat_dir}. Run `make release` there.")
    if os.environ.get("N6K_EXTENSION_DIR"):
        return
    if not os.path.isdir(EXTENSION_DIR):
        raise FileNotFoundError(
            f"n6k extensions not built at {EXTENSION_DIR}. "
            "Run `make release`, or set N6K_EXTENSION_DIR to an existing build."
        )
    os.environ["N6K_EXTENSION_DIR"] = EXTENSION_DIR
