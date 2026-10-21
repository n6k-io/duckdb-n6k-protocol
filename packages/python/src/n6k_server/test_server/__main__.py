"""CLI entry point: `python -m n6k_server.test_server --port 8099`."""

import argparse
import logging.config
import os
from typing import Any

import uvicorn

from n6k_server.test_server._paths import EXTENSION_DIR, REPO_ROOT
from n6k_server.test_server.app import app

# Watched for auto-reload. The Python sources are the obvious half; the built
# extensions are the other half, because the server loads them into its own
# process at startup — after `make release` the running worker still holds the
# OLD .duckdb_extension, so a rebuilt function looks "missing" until a restart.
_PY_SRC = os.path.join(REPO_ROOT, "packages", "python", "src")
_RELOAD_INCLUDES = ["*.py", "*.duckdb_extension"]


def _build_log_config(log_path: str) -> dict[str, Any]:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"},
        },
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "filename": log_path,
                "mode": "a",
                "formatter": "default",
            },
            "console": {"class": "logging.StreamHandler", "formatter": "default"},
        },
        "root": {"level": "INFO", "handlers": ["file", "console"]},
    }


def _require_watchfiles() -> None:
    """Fail loudly if the reloader can't actually honour `reload_includes`.

    Without `watchfiles`, uvicorn falls back to `StatReload`, which hardcodes
    `rglob("*.py")` and ignores includes/excludes (it logs a warning and carries
    on). Python edits would still reload, so the breakage is silent: extension
    rebuilds would quietly stop being picked up, which is the exact failure this
    watch exists to prevent.
    """
    try:
        import watchfiles  # noqa: F401
    except ImportError as exc:
        raise SystemExit(
            "test-server --reload needs `watchfiles` to watch built extensions "
            "(uvicorn's fallback reloader only ever watches *.py). "
            "Install it (`uv sync`) or run with --no-reload."
        ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m n6k_server.test_server", description="n6k protocol test server")
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument(
        "--no-reload",
        dest="reload",
        action="store_false",
        help="disable uvicorn auto-reload (on by default)",
    )
    parser.set_defaults(reload=True)
    parser.add_argument(
        "--log-file",
        default=os.environ.get("N6K_SERVER_LOG", "scratch/outputs/n6k_server.log"),
        help="path to server log file (default: scratch/outputs/n6k_server.log)",
    )
    args = parser.parse_args()

    print(f"n6k test server on :{args.port}")

    os.makedirs(os.path.dirname(os.path.abspath(args.log_file)), exist_ok=True)
    log_config = _build_log_config(args.log_file)
    logging.config.dictConfig(log_config)

    if args.reload:
        _require_watchfiles()
        reload_dirs = [_PY_SRC]
        if os.path.isdir(EXTENSION_DIR):
            reload_dirs.append(EXTENSION_DIR)
        else:
            print(f"warning: {EXTENSION_DIR} not built — reload will not pick up extension rebuilds")
        uvicorn.run(
            "n6k_server.test_server.app:app",
            host="0.0.0.0",
            port=args.port,
            reload=True,
            reload_dirs=reload_dirs,
            reload_includes=_RELOAD_INCLUDES,
            log_config=log_config,
        )
    else:
        uvicorn.run(app, host="0.0.0.0", port=args.port, log_config=log_config)


if __name__ == "__main__":
    main()
