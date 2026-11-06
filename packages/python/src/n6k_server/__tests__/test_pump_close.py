import logging
import threading

import duckdb
import pytest

from n6k_server.pump import _close_all


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


@pytest.mark.xfail(strict=True, reason="a refused con.close() is never retried")
def test_a_close_refused_by_a_running_query_closes_con_once_it_returns() -> None:
    con = duckdb.connect()
    entered = threading.Event()
    release = threading.Event()
    on_close_calls: list[duckdb.DuckDBPyConnection] = []
    errors: list[Exception] = []
    queries: list[threading.Thread] = []

    def block() -> int:
        entered.set()
        release.wait()
        return 1

    def start_query(c: duckdb.DuckDBPyConnection) -> None:
        on_close_calls.append(c)
        c.create_function("block", block, [], "BIGINT", side_effects=True)
        query = threading.Thread(target=lambda: c.execute("SELECT block()"), daemon=True)
        queries.append(query)
        query.start()
        assert entered.wait(10)

    def close() -> None:
        try:
            _close_all([], con, start_query)
        except Exception as error:  # noqa: BLE001 - reported by the assert below
            errors.append(error)

    closer = threading.Thread(target=close, daemon=True)
    closer.start()
    closer.join(1)
    release.set()
    closer.join(10)
    queries[0].join(10)

    assert not closer.is_alive()
    assert errors == []
    assert on_close_calls == [con]
    assert _is_closed(con)
