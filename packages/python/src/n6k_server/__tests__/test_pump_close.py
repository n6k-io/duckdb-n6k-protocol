import logging

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
