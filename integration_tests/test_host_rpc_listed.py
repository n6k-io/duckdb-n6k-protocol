"""A host RPC (`register_rpc_streams`, as n6k-pykit's `@rpc` uses) is listed and callable on an attached catalog.

Run:
    uv run pytest integration_tests/test_host_rpc_listed.py -v
"""

import duckdb
import pytest

from _paths import EXT_PATH


@pytest.mark.parametrize("catalog", ["db", "slowdb"])
def test_host_rpc_is_listed_and_callable(server: int, catalog: str) -> None:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.load_extension("httpfs")
        con.load_extension(EXT_PATH)
        con.execute(f"ATTACH 'n6k://127.0.0.1:{server}' AS a (TYPE n6k, CATALOG '{catalog}')")
        listed = con.execute(
            "SELECT schema_name, function_name, varargs FROM duckdb_functions() "
            "WHERE database_name = 'a' AND function_name LIKE 'py_%' ORDER BY ALL"
        ).fetchall()
        assert listed == [
            ("main", "py_echo", "ANY"),
            ("main", "py_sum_table", "ANY"),
            ("main", "py_ticker", "ANY"),
            ("main", "py_ticker_closes", "ANY"),
        ]
        assert con.execute("SELECT * FROM a.py_echo('hello')").fetchall() == [("hello",)]
        assert con.execute("SELECT * FROM a.main.py_ticker(3)").fetchall() == [(0,), (1,), (2,)]
        with pytest.raises(duckdb.CatalogException, match="does not exist"):
            con.execute("SELECT * FROM a.py_nope(1)")
    finally:
        con.close()
