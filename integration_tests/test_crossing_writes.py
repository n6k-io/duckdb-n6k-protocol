"""Crossing writes: OP_WRITE_PLAN with seam rows or a shipped feed.

Requires the test server running on :8099.

Run:
    uv run pytest integration_tests/test_crossing_writes.py -v
"""

import os
import time
import urllib.error
import urllib.request

import duckdb
import pytest

from _paths import EXT_PATH

SERVER_URL = "http://localhost:8099"


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
    return c


def _exec(con, sql: str):
    con.execute("SELECT * FROM n6k_catalog_exec('db', ?)", [sql]).fetchall()


@pytest.fixture
def keyed(con):
    name = f"cw_{int(time.time() * 1000)}"
    _exec(con, f'CREATE TABLE db.main."{name}"(id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)')
    _exec(con, f"INSERT INTO db.main.\"{name}\" VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35)")
    con.execute("SELECT * FROM n6k_invalidate_cache('db', 'main')").fetchall()
    yield name
    _exec(con, f'DROP TABLE IF EXISTS db.main."{name}"')


def _rows(con, name):
    return con.execute(f'SELECT id, name, age FROM db.main."{name}" ORDER BY id').fetchall()


def test_update_by_key_runs_on_the_server(con, keyed):
    affected = con.execute(f"UPDATE db.main.\"{keyed}\" SET name = 'updated' WHERE id = 2").fetchall()
    assert affected == [(1,)]
    assert _rows(con, keyed) == [(1, "alice", 30), (2, "updated", 25), (3, "charlie", 35)]


def test_update_with_expression(con, keyed):
    con.execute(f'UPDATE db.main."{keyed}" SET age = age + 10 WHERE id = 3')
    assert _rows(con, keyed)[2] == (3, "charlie", 45)


def test_update_with_local_predicate(con, keyed):
    affected = con.execute(f"UPDATE db.main.\"{keyed}\" SET age = 0 WHERE regexp_matches(name, '^[ab]')").fetchall()
    assert affected == [(2,)]
    assert _rows(con, keyed) == [(1, "alice", 0), (2, "bob", 0), (3, "charlie", 35)]


def test_delete_by_key(con, keyed):
    affected = con.execute(f'DELETE FROM db.main."{keyed}" WHERE id = 1').fetchall()
    assert affected == [(1,)]
    assert _rows(con, keyed) == [(2, "bob", 25), (3, "charlie", 35)]


def test_delete_with_local_predicate(con, keyed):
    affected = con.execute(f"DELETE FROM db.main.\"{keyed}\" WHERE regexp_matches(name, 'li')").fetchall()
    assert affected == [(2,)]
    assert _rows(con, keyed) == [(2, "bob", 25)]


def test_insert_select_from_served_table(con, keyed):
    affected = con.execute(
        f'INSERT INTO db.main."{keyed}" SELECT id + 10, name, age FROM db.main."{keyed}" WHERE id < 3'
    ).fetchall()
    assert affected == [(2,)]
    assert [r[0] for r in _rows(con, keyed)] == [1, 2, 3, 11, 12]


def test_insert_values(con, keyed):
    affected = con.execute(f"INSERT INTO db.main.\"{keyed}\" VALUES (4, 'dave', 40), (5, 'eve', 50)").fetchall()
    assert affected == [(2,)]
    assert len(_rows(con, keyed)) == 5


def test_insert_returning(con, keyed):
    rows = con.execute(f"INSERT INTO db.main.\"{keyed}\" VALUES (6, 'fay', 60) RETURNING id, name").fetchall()
    assert rows == [(6, "fay")]


def test_update_returning(con, keyed):
    rows = con.execute(f'UPDATE db.main."{keyed}" SET age = 99 WHERE id = 1 RETURNING id, age').fetchall()
    assert rows == [(1, 99)]


def test_merge_into(con, keyed):
    con.execute("CREATE TABLE incoming(id INTEGER, name VARCHAR, age INTEGER)")
    con.execute("INSERT INTO incoming VALUES (2, 'bob2', 26), (7, 'gus', 70)")
    con.execute(
        f'MERGE INTO db.main."{keyed}" AS t USING incoming AS i ON t.id = i.id '
        "WHEN MATCHED THEN UPDATE SET name = i.name, age = i.age "
        "WHEN NOT MATCHED THEN INSERT VALUES (i.id, i.name, i.age)"
    )
    assert _rows(con, keyed) == [(1, "alice", 30), (2, "bob2", 26), (3, "charlie", 35), (7, "gus", 70)]


def test_keyless_update_is_refused(con):
    with pytest.raises(duckdb.BinderException, match="no key"):
        con.execute("UPDATE db.main.users SET age = 1 WHERE id = 1")
