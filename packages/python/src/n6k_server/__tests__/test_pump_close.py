import asyncio
import gc
import logging
import threading
import time
import weakref
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest

from n6k_server.pump import _Callbacks, _close_all


def _is_closed(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("SELECT 1")
    except duckdb.ConnectionException:
        return True
    return False


def test_on_close_runs_once_after_cursors_and_before_con_closes() -> None:
    con = duckdb.connect()
    cur = con.cursor()
    seen: list[tuple[bool, bool]] = []

    def on_close(c: duckdb.DuckDBPyConnection) -> None:
        assert c is con
        seen.append((_is_closed(cur), _is_closed(con)))

    _close_all([cur], con, on_close)

    assert seen == [(True, False)]
    assert _is_closed(con)


def test_a_raising_on_close_is_logged_and_con_still_closes(caplog: pytest.LogCaptureFixture) -> None:
    con = duckdb.connect()

    def on_close(_: duckdb.DuckDBPyConnection) -> None:
        raise RuntimeError("boom")

    with caplog.at_level(logging.ERROR, logger="n6k_server.pump"):
        _close_all([], con, on_close)

    assert _is_closed(con)
    assert "on_close failed" in caplog.text


def test_no_on_close_closes_con() -> None:
    con = duckdb.connect()
    _close_all([], con, None)
    assert _is_closed(con)


def test_a_close_refused_by_a_running_query_finishes_once_it_returns() -> None:
    con = duckdb.connect()
    entered = threading.Event()
    release = threading.Event()

    def block() -> int:
        entered.set()
        release.wait()
        return 1

    con.create_function("block", block, [], "BIGINT", side_effects=True)
    runner = threading.Thread(target=lambda: con.execute("SELECT block()"), daemon=True)
    runner.start()
    assert entered.wait(10)
    closes: list[duckdb.DuckDBPyConnection] = []

    try:
        _close_all([], con, closes.append)
    finally:
        release.set()
    runner.join(10)
    deadline = time.monotonic() + 10
    while True:
        try:
            if _is_closed(con):
                break
        except duckdb.ResourceInUseException:
            pass
        assert time.monotonic() < deadline
        time.sleep(0.02)
    assert closes == [con]


def test_closing_callbacks_lets_gc_collect_them_while_con_is_open() -> None:
    async def run() -> None:
        con = duckdb.connect()
        text: Any = SimpleNamespace(capabilities=())
        handle: Any = None
        callbacks = _Callbacks(None, con, lambda *_: [], {}, handle, text, None, lambda *_: None)
        callbacks.create_functions()
        ref = weakref.ref(callbacks)

        callbacks.close()
        del callbacks
        gc.collect()

        assert ref() is None
        con.close()

    asyncio.run(run())
