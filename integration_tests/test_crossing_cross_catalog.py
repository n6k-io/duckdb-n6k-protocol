"""Crossing across catalogs: attaches riding one wsFd socket fold into one plan.

Each attach's alias differs from the catalog it opens, so a plan that named a table by alias, or
bound it in the wrong catalog, reads the wrong `marker` row.

Run:
    uv run pytest integration_tests/test_crossing_cross_catalog.py -v
"""

import os
import uuid
from collections.abc import Iterator

import duckdb
import pytest

from _paths import EXT_PATH
from test_wsfd_attach import SERVER_PORT, _connect, _n6k_connect, _open_pumped

CROSSED_MARKERS = "SELECT x.catalog, y.catalog FROM x1.main.marker x JOIN y1.main.marker y ON x.catalog <> y.catalog"


@pytest.fixture(autouse=True)
def skip_if_no_extension() -> None:
    if not os.path.exists(EXT_PATH):
        pytest.skip(f"Extension not built: {EXT_PATH}")


@pytest.fixture
def shared(server: int) -> Iterator[duckdb.DuckDBPyConnection]:
    c = _connect()
    py_sock, ext_sock, ws, _t_in = _open_pumped(f"ws://localhost:{SERVER_PORT}/tuned/ws?catalogs=a,b")
    try:
        _n6k_connect(c, ext_sock.fileno())
        c.execute(f"ATTACH '' AS x1 (TYPE n6k, wsFd {ext_sock.fileno()}, CATALOG 'a')")
        c.execute(f"ATTACH '' AS y1 (TYPE n6k, wsFd {ext_sock.fileno()}, CATALOG 'b')")
        yield c
    finally:
        c.close()
        ext_sock.close()
        ws.close()
        py_sock.close()


def _crossing_reads(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    plan = "\n".join(r[1] for r in con.execute(f"EXPLAIN {sql}").fetchall())
    return plan.count("CROSSING_READ")


def test_a_join_across_catalogs_is_one_read_of_each_catalog(shared: duckdb.DuckDBPyConnection) -> None:
    assert _crossing_reads(shared, CROSSED_MARKERS) == 1
    assert shared.execute(CROSSED_MARKERS).fetchall() == [("a", "b")]


def test_a_view_across_catalogs_reads_each_catalog(shared: duckdb.DuckDBPyConnection) -> None:
    view = "v_" + uuid.uuid4().hex[:8]
    shared.execute(f"CREATE VIEW x1.main.{view} AS {CROSSED_MARKERS}")
    assert shared.execute(f"SELECT * FROM x1.main.{view}").fetchall() == [("a", "b")]


def test_a_view_whose_body_is_only_in_the_other_catalog(shared: duckdb.DuckDBPyConnection) -> None:
    view = "v_" + uuid.uuid4().hex[:8]
    shared.execute(f"CREATE VIEW x1.main.{view} AS SELECT catalog FROM y1.main.marker")
    assert shared.execute(f"SELECT * FROM x1.main.{view}").fetchall() == [("b",)]


def test_a_join_sees_a_write_to_either_catalog_in_the_transaction(shared: duckdb.DuckDBPyConnection) -> None:
    shared.execute("BEGIN")
    assert shared.execute(CROSSED_MARKERS + " ORDER BY 2").fetchall() == [("a", "b")]
    shared.execute("INSERT INTO y1.main.marker VALUES ('new')")
    assert shared.execute(CROSSED_MARKERS + " ORDER BY 2").fetchall() == [("a", "b"), ("a", "new")]
    shared.execute("COMMIT")
    assert shared.execute(CROSSED_MARKERS + " ORDER BY 2").fetchall() == [("a", "b"), ("a", "new")]


def test_an_insert_from_a_join_across_catalogs(shared: duckdb.DuckDBPyConnection) -> None:
    shared.execute(
        "INSERT INTO x1.main.marker SELECT y.catalog FROM y1.main.marker y JOIN x1.main.marker x "
        "ON x.catalog <> y.catalog"
    )
    assert shared.execute("SELECT catalog FROM x1.main.marker ORDER BY 1").fetchall() == [("a",), ("b",)]


def test_attaches_on_separate_sockets_read_each_apart(server: int) -> None:
    c = _connect()
    try:
        for alias, catalog in (("p1", "db"), ("p2", "other")):
            c.execute(f"ATTACH 'n6k://localhost:{server}' AS {alias} (TYPE n6k, CATALOG '{catalog}')")
        sql = "SELECT count(*) FROM p1.main.users u JOIN p2.main.users v ON u.id = v.id"
        assert _crossing_reads(c, sql) == 2
        assert c.execute(sql).fetchone() == (3,)
    finally:
        c.close()
