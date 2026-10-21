"""Which catalogs `CALL n6k_serve_*('*')` picks.

The rule lives in C++ (`n6k::ResolveServedCatalogs`) and most of it is asserted in
`test/sql/n6k_server.test`. What that runner cannot express is a connection whose
*startup* database is a file: it always opens in memory. That case is the whole
reason the rule tests for in-memory rather than just "the startup database", so it
is pinned here, where the connection can be opened against a path.

`n6k_testing_served_catalogs('*')` resolves the same list and returns it instead of
serving it — the serve functions block once bound.
"""

import duckdb
import pytest

from n6k_server.extension import load_n6k_server, load_n6k_testing


def _served(con):
    return sorted(row[0] for row in con.execute("SELECT catalog FROM n6k_testing_served_catalogs('*')").fetchall())


@pytest.fixture
def file_db(tmp_path):
    """A connection whose startup database is a file, not the throwaway `memory`."""
    con = duckdb.connect(str(tmp_path / "prod.db"), config={"allow_unsigned_extensions": "true"})
    load_n6k_server(con)
    load_n6k_testing(con)
    yield con
    con.close()


def test_a_file_startup_database_is_served_on_its_own(file_db):
    assert _served(file_db) == ["prod"]


def test_a_file_startup_database_survives_an_attach(file_db):
    """`duckdb.connect("/data/prod.db")` asked for `prod`, so attaching does not drop it.

    Only the in-memory database a connection carries without asking is dropped. A
    host that opened a file and then attached a second catalog wants both served —
    and it accepts the multiplexed wire that comes with them, having asked for two.
    """
    file_db.execute("ATTACH ':memory:' AS extra")
    assert _served(file_db) == ["extra", "prod"]


def test_an_in_memory_startup_database_is_dropped_once_something_is_attached():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    load_n6k_server(con)
    load_n6k_testing(con)
    try:
        con.execute("ATTACH ':memory:' AS db")
        assert _served(con) == ["db"]
    finally:
        con.close()
