"""Parallel crossing reads: every thread pulls from one request, and order survives.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_parallel_reads.py -v
"""

import os
import time
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_URL = "http://localhost:8099"
ROWS = 100_003  # prime, so (i * 7919) % ROWS is a permutation of the row numbers


@pytest.fixture(autouse=True)
def skip_if_no_extension():
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


@pytest.fixture(autouse=True)
def check_server():
    try:
        urllib.request.urlopen(f"{SERVER_URL}/debug/counts", timeout=2)
    except (urllib.error.URLError, ConnectionRefusedError, OSError):
        pytest.skip(f"test server not running on {SERVER_URL}")


@pytest.fixture
def con():
    c = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    c.load_extension("httpfs")
    c.execute(f"LOAD '{EXT_PATH}'")
    c.execute("ATTACH 'n6k://localhost:8099' AS db (TYPE n6k)")
    c.execute("SET threads = 4")
    return c


def _exec(con, sql: str):
    con.execute("SELECT * FROM n6k_catalog_exec('db', ?)", [sql]).fetchall()


@pytest.fixture
def big(con):
    name = f"pr_{int(time.time() * 1000)}"
    _exec(
        con,
        f'CREATE TABLE db.main."{name}" AS SELECT i AS seq, (i * 7919) % {ROWS} AS k FROM range({ROWS}) t(i)',
    )
    con.execute("SELECT * FROM n6k_invalidate_cache('db', 'main')").fetchall()
    yield name
    _exec(con, f'DROP TABLE IF EXISTS db.main."{name}"')


def _crossed(con, sql: str):
    plan = "\n".join(r[1] for r in con.execute(f"EXPLAIN {sql}").fetchall())
    assert "CROSSING_READ" in plan, plan


def test_scan_keeps_insertion_order(con, big):
    sql = f'SELECT seq FROM db.main."{big}"'
    _crossed(con, sql)
    assert [r[0] for r in con.execute(sql).fetchall()] == list(range(ROWS))


def test_ordered_scan_matches_one_thread(con, big):
    sql = f'SELECT k, seq FROM db.main."{big}" ORDER BY k'
    _crossed(con, sql)
    parallel = con.execute(sql).fetchall()
    con.execute("SET threads = 1")
    assert parallel == con.execute(sql).fetchall()
    assert [r[0] for r in parallel] == list(range(ROWS))


def test_ordered_scan_without_insertion_order(con, big):
    con.execute("SET preserve_insertion_order = false")
    sql = f'SELECT k FROM db.main."{big}" ORDER BY k DESC'
    _crossed(con, sql)
    assert [r[0] for r in con.execute(sql).fetchall()] == list(reversed(range(ROWS)))


def test_ctas_keeps_insertion_order(con, big):
    con.execute(f'CREATE TABLE near_copy AS SELECT seq FROM db.main."{big}"')
    assert [r[0] for r in con.execute("SELECT seq FROM near_copy").fetchall()] == list(range(ROWS))


def test_copy_keeps_insertion_order(con, big, tmp_path):
    out = tmp_path / "seq.csv"
    con.execute(f"COPY (SELECT seq FROM db.main.\"{big}\") TO '{out}' (HEADER false)")
    assert [int(line) for line in out.read_text().split()] == list(range(ROWS))


def test_aggregate_over_parallel_scan(con, big):
    sql = f'SELECT count(*), sum(seq), sum(k) FROM (SELECT seq, k FROM db.main."{big}" WHERE seq >= 0)'
    total = ROWS * (ROWS - 1) // 2
    assert con.execute(sql).fetchall() == [(ROWS, total, total)]


def test_limit_above_a_local_join_stops_cleanly(con, big):
    sql = f'SELECT b.seq FROM db.main."{big}" b JOIN range({ROWS}) l(r) ON b.seq = l.r LIMIT 10'
    assert len(con.execute(sql).fetchall()) == 10
    assert con.execute(f'SELECT count(*) FROM db.main."{big}"').fetchall() == [(ROWS,)]


def test_error_mid_stream_raises(con, big):
    sql = f"SELECT seq, CASE WHEN seq = {ROWS - 5} THEN error('boom') ELSE seq END FROM db.main.\"{big}\""
    with pytest.raises(duckdb.Error, match="boom"):
        con.execute(sql).fetchall()
