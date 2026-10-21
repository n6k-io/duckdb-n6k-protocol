"""Integration tests for bridge writes against external databases (postgres, mysql).

Run with: uv run pytest packages/python/src/n6k_server/__tests__/test_bridge_external.py \
  --external 'postgres://<user>@<host>/<db>'
Skip automatically when --external is not provided.
"""

import duckdb
import pytest

from n6k_server.bridge import Policy, bridge

pytestmark = pytest.mark.external


def _select(table):
    return [Policy(table, "select", "true")]


def _all_verbs(table):
    return [
        Policy(table, "select", "true"),
        Policy(table, "insert", "true"),
        Policy(table, "update", "true"),
        Policy(table, "delete", "true"),
    ]


def _bridge_external(external_db, table_name, grants=_all_verbs, primary_keys=None):
    """Bridge one source table into a fresh bridge catalog on the source connection
    and return (conn, qualified).

    Source == target on purpose: bridging across two connections trips the
    per-.so load-nonce check ("different copies of the virtual_catalog extension").
    """
    conn = external_db["source"]
    catalog = f"brdg_{table_name}"
    schema = external_db["schema"]
    bridge(
        conn,
        conn,
        catalog,
        source_catalog=external_db["catalog"],
        policies=[
            *(Policy(f"{schema}.{t}", "primary_key", list(cols)) for t, cols in (primary_keys or {}).items()),
            *grants(f"{schema}.{table_name}"),
        ],
    )
    qualified = f'"{catalog}"."{schema}"."{table_name}"'
    return conn, qualified


def test_update_integer_pk(external_db):
    external_db["create_table"]("bridge_int_pk", "id INTEGER PRIMARY KEY, val INTEGER")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_int_pk"'
    src.execute(f"INSERT INTO {q} VALUES (1, 100), (2, 200)")

    target, tq = _bridge_external(external_db, "bridge_int_pk")
    target.sql(f"UPDATE {tq} SET val = 999 WHERE id = 1")

    result = src.execute(f"SELECT val FROM {q} WHERE id = 1").fetchone()
    assert result == (999,)


def test_delete_integer_pk(external_db):
    external_db["create_table"]("bridge_int_del", "id INTEGER PRIMARY KEY, val INTEGER")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_int_del"'
    src.execute(f"INSERT INTO {q} VALUES (1, 10), (2, 20), (3, 30)")

    target, tq = _bridge_external(external_db, "bridge_int_del")
    target.sql(f"DELETE FROM {tq} WHERE id = 2")

    result = src.execute(f"SELECT id FROM {q} ORDER BY id").fetchall()
    assert result == [(1,), (3,)]


def test_update_varchar_pk(external_db):
    if external_db["type"] == "mysql":
        pytest.skip("MySQL maps VARCHAR to TEXT which can't be a PK via DuckDB CREATE TABLE")
    external_db["create_table"]("bridge_varchar_pk", "code VARCHAR(50) PRIMARY KEY, val INTEGER")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_varchar_pk"'
    src.execute(f"INSERT INTO {q} VALUES ('abc', 10), ('def', 20)")

    target, tq = _bridge_external(external_db, "bridge_varchar_pk")
    target.sql(f"UPDATE {tq} SET val = 77 WHERE code = 'abc'")

    result = src.execute(f"SELECT val FROM {q} WHERE code = 'abc'").fetchone()
    assert result == (77,)


def test_delete_varchar_pk(external_db):
    if external_db["type"] == "mysql":
        pytest.skip("MySQL maps VARCHAR to TEXT which can't be a PK via DuckDB CREATE TABLE")
    external_db["create_table"]("bridge_varchar_del", "code VARCHAR(50) PRIMARY KEY, val INTEGER")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_varchar_del"'
    src.execute(f"INSERT INTO {q} VALUES ('abc', 10), ('def', 20), ('ghi', 30)")

    target, tq = _bridge_external(external_db, "bridge_varchar_del")
    target.sql(f"DELETE FROM {tq} WHERE code = 'def'")

    result = src.execute(f"SELECT code FROM {q} ORDER BY code").fetchall()
    assert result == [("abc",), ("ghi",)]


def test_insert_through_bridge(external_db):
    external_db["create_table"]("bridge_insert", "id INTEGER PRIMARY KEY, name VARCHAR(100)")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_insert"'

    target, tq = _bridge_external(external_db, "bridge_insert")
    target.sql(f"INSERT INTO {tq} VALUES (1, 'hello')")

    result = src.execute(f"SELECT id, name FROM {q}").fetchall()
    assert result == [(1, "hello")]


def test_update_expression(external_db):
    external_db["create_table"]("bridge_expr", "id INTEGER PRIMARY KEY, counter INTEGER")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_expr"'
    src.execute(f"INSERT INTO {q} VALUES (1, 10), (2, 20)")

    target, tq = _bridge_external(external_db, "bridge_expr")
    target.sql(f"UPDATE {tq} SET counter = counter + 5 WHERE id = 1")

    result = src.execute(f"SELECT counter FROM {q} WHERE id = 1").fetchone()
    assert result == (15,)


def test_composite_pk_update(external_db):
    external_db["create_table"]("bridge_cpk", "a INTEGER, b INTEGER, val VARCHAR(50), PRIMARY KEY(a, b)")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_cpk"'
    src.execute(f"INSERT INTO {q} VALUES (1, 1, 'orig'), (1, 2, 'orig')")

    target, tq = _bridge_external(external_db, "bridge_cpk")
    target.sql(f"UPDATE {tq} SET val = 'changed' WHERE a = 1 AND b = 1")

    result = src.execute(f"SELECT val FROM {q} WHERE a = 1 AND b = 1").fetchone()
    assert result == ("changed",)


def test_composite_pk_delete(external_db):
    external_db["create_table"]("bridge_cpk_del", "a INTEGER, b INTEGER, val VARCHAR(50), PRIMARY KEY(a, b)")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_cpk_del"'
    src.execute(f"INSERT INTO {q} VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')")

    target, tq = _bridge_external(external_db, "bridge_cpk_del")
    target.sql(f"DELETE FROM {tq} WHERE a = 1 AND b = 2")

    result = src.execute(f"SELECT a, b FROM {q} ORDER BY a, b").fetchall()
    assert result == [(1, 1), (2, 1)]


def test_pk_auto_discovery(external_db):
    external_db["create_table"]("bridge_auto_pk", "id INTEGER PRIMARY KEY, val INTEGER")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_auto_pk"'
    src.execute(f"INSERT INTO {q} VALUES (1, 10)")

    # No primary_keys override — should auto-discover
    target, tq = _bridge_external(external_db, "bridge_auto_pk")
    target.sql(f"UPDATE {tq} SET val = 99 WHERE id = 1")

    result = src.execute(f"SELECT val FROM {q} WHERE id = 1").fetchone()
    assert result == (99,)


def test_no_pk_refuses_keyed_writes_on_external(external_db):
    external_db["create_table"]("bridge_no_pk", "x INTEGER, y INTEGER")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_no_pk"'

    target, tq = _bridge_external(external_db, "bridge_no_pk")
    target.sql(f"INSERT INTO {tq} VALUES (1, 10)")
    assert src.execute(f"SELECT x, y FROM {q}").fetchall() == [(1, 10)]
    with pytest.raises(duckdb.BinderException, match="has no key"):
        target.sql(f"UPDATE {tq} SET y = 99 WHERE x = 1")
    with pytest.raises(duckdb.BinderException, match="has no key"):
        target.sql(f"DELETE FROM {tq} WHERE x = 1")


def test_join_optional_filter_pushdown(external_db):
    """Regression: a hash join over bridged tables must not emit non-SQL filters.

    DuckDB pushes a hash join's dynamic min/max into the build-side scan as an
    OPTIONAL_FILTER. A BuildBridgeSQL that ToString()s every pushed filter
    blindly serializes the optional to "optional: id>=1 AND optional: id<=12"
    — invalid SQL the source rejects ("syntax error at or near \\":\\""). An
    external (stats-less) source is required: an in-memory source exposes column
    stats, so DuckDB pushes an equivalent *static* range filter that masks the
    bug. The fix serializes only SQL-safe leaf/conjunction types and unwraps
    OPTIONAL_FILTER, so the bridged result must match the direct source result.
    """
    external_db["create_table"]("bridge_join_users", "id INTEGER PRIMARY KEY, country VARCHAR(8)")
    external_db["create_table"]("bridge_join_orders", "id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER")
    src = external_db["source"]
    cat, sch = external_db["catalog"], external_db["schema"]
    uq = f'"{cat}"."{sch}"."bridge_join_users"'
    oq = f'"{cat}"."{sch}"."bridge_join_orders"'
    src.execute(f"INSERT INTO {uq} SELECT g, CASE WHEN g % 2 = 0 THEN 'US' ELSE 'CA' END FROM range(1, 13) t(g)")
    src.execute(f"INSERT INTO {oq} SELECT g, (g % 12) + 1, g * 10 FROM range(1, 201) t(g)")

    # Ground truth: the same aggregate straight off the source.
    expected = src.sql(
        f"SELECT u.country, sum(o.amount) FROM {oq} o "
        f"JOIN {uq} u ON u.id = o.user_id GROUP BY u.country ORDER BY u.country"
    ).fetchall()

    # Source == target sidesteps the cross-connection load-nonce check.
    bridge(
        src,
        src,
        "jbr",
        source_catalog=cat,
        policies=_select(f"{sch}.bridge_join_users") + _select(f"{sch}.bridge_join_orders"),
    )

    rows = src.sql(
        f'SELECT u.country, sum(o.amount) FROM jbr."{sch}".bridge_join_orders o '
        f'JOIN jbr."{sch}".bridge_join_users u ON u.id = o.user_id '
        "GROUP BY u.country ORDER BY u.country"
    ).fetchall()
    assert rows == expected


def test_read_through_bridge(external_db):
    external_db["create_table"]("bridge_read", "id INTEGER PRIMARY KEY, name VARCHAR(100)")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_read"'
    src.execute(f"INSERT INTO {q} VALUES (1, 'alice'), (2, 'bob')")

    target, tq = _bridge_external(external_db, "bridge_read", grants=_select)

    result = target.sql(f"SELECT id, name FROM {tq} ORDER BY id").fetchall()
    assert result == [(1, "alice"), (2, "bob")]


def test_multi_column_update(external_db):
    external_db["create_table"]("bridge_multi", "id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR(50)")
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_multi"'
    src.execute(f"INSERT INTO {q} VALUES (1, 10, 'old')")

    target, tq = _bridge_external(external_db, "bridge_multi")
    target.sql(f"UPDATE {tq} SET a = 99, b = 'new' WHERE id = 1")

    result = src.execute(f"SELECT a, b FROM {q} WHERE id = 1").fetchone()
    assert result == (99, "new")


def test_sequential_scans_no_segfault(external_db):
    """Multiple sequential scans on a readwrite bridge table must not segfault.

    Regression test: the first scan consumed the Arrow stream in the cached
    BridgeTableCatalogEntry. The second scan hit the stale/released Arrow
    array, causing a segfault (or "released array passed" error).
    """
    external_db["create_table"](
        "bridge_segfault",
        "id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount NUMERIC(10,2) NOT NULL, status TEXT NOT NULL",
    )
    src = external_db["source"]
    q = f'"{external_db["catalog"]}"."{external_db["schema"]}"."bridge_segfault"'
    src.execute(f"INSERT INTO {q} VALUES (1, 10, 29.99, 'pending'), (2, 20, 49.99, 'shipped')")

    target, tq = _bridge_external(external_db, "bridge_segfault")

    r1 = target.sql(f"SELECT * FROM {tq} ORDER BY id").fetchall()
    assert len(r1) == 2

    # Second scan: projected columns — this segfaulted before the fix
    r2 = target.sql(f"SELECT id, amount FROM {tq} ORDER BY id").fetchall()
    assert len(r2) == 2
    assert r2[0][0] == 1
