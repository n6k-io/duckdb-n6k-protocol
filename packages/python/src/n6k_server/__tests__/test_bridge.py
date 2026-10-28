import duckdb
import pytest

from n6k_server.bridge import Policy, bridge, unbridge
from n6k_server.extension import load_n6k, load_vcatbridge

CONN_CONFIG: dict[str, str | bool | int | float | list[str]] = {"allow_unsigned_extensions": "true"}

T = '"workspace"."main"."t"'


def _target() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(config=CONN_CONFIG)
    load_vcatbridge(conn)
    return conn


def _source(*statements: str) -> duckdb.DuckDBPyConnection:
    source = duckdb.connect(config=CONN_CONFIG)
    for statement in statements:
        source.sql(statement)
    return source


def _bridge(source, target, policies, **kwargs):
    return bridge(source, target, "workspace", source_catalog="memory", policies=policies, **kwargs)


def _select(table):
    return [Policy(table, "select", "true")]


def _all_verbs(table):
    return [
        Policy(table, "select", "true"),
        Policy(table, "insert", "true"),
        Policy(table, "update", "true"),
        Policy(table, "delete", "true"),
    ]


def test_read_through_bridge():
    source = _source("CREATE TABLE t AS SELECT i, i * 2 AS doubled FROM range(100) t(i)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    assert target.sql(f"SELECT count(*) FROM {T}").fetchone() == (100,)


def test_non_permitted_table_blocked():
    source = _source("CREATE TABLE allowed(x INTEGER)", "CREATE TABLE secret(x INTEGER)")
    target = _target()
    _bridge(source, target, _select("main.allowed"))

    target.sql('SELECT * FROM "workspace"."main".allowed')
    with pytest.raises(duckdb.CatalogException):
        target.sql('SELECT * FROM "workspace"."main".secret')


def test_readwrite_insert():
    source = _source("CREATE TABLE t(x INTEGER PRIMARY KEY, y VARCHAR)")
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    target.sql(f"INSERT INTO {T} VALUES (1, 'hello')")
    assert source.sql("SELECT x, y FROM t").fetchall() == [(1, "hello")]


def test_read_only_blocks_insert():
    source = _source("CREATE TABLE t(x INTEGER)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    with pytest.raises(duckdb.Error):
        target.sql(f"INSERT INTO {T} VALUES (1)")


def test_readwrite_update():
    source = _source(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR)", "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')"
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    target.sql(f"UPDATE {T} SET name = 'updated' WHERE id = 1")
    assert source.sql("SELECT name FROM t WHERE id = 1").fetchone() == ("updated",)


def test_readwrite_delete():
    source = _source(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR)",
        "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    target.sql(f"DELETE FROM {T} WHERE id = 2")
    assert source.sql("SELECT id, name FROM t ORDER BY id").fetchall() == [(1, "alice"), (3, "charlie")]


def test_read_only_blocks_update():
    source = _source("CREATE TABLE t(id INTEGER)", "INSERT INTO t VALUES (1)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    with pytest.raises(duckdb.Error):
        target.sql(f"UPDATE {T} SET id = 2 WHERE id = 1")


def test_read_only_blocks_delete():
    source = _source("CREATE TABLE t(id INTEGER)", "INSERT INTO t VALUES (1)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    with pytest.raises(duckdb.Error):
        target.sql(f"DELETE FROM {T} WHERE id = 1")


def test_filter_pushdown():
    source = _source("CREATE TABLE t AS SELECT i FROM range(1000) t(i)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    result = target.sql(f"SELECT * FROM {T} WHERE i > 995").fetchall()
    assert len(result) == 4
    assert all(row[0] > 995 for row in result)


def test_live_data_visibility():
    source = _source("CREATE TABLE t(x INTEGER)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    assert target.sql(f"SELECT count(*) FROM {T}").fetchone() == (0,)
    source.sql("INSERT INTO t VALUES (1), (2), (3)")
    assert target.sql(f"SELECT count(*) FROM {T}").fetchone() == (3,)


def test_empty_policies_raises():
    with pytest.raises(duckdb.Error, match="policies must be provided"):
        _bridge(_source(), _target(), [])


def test_source_schema_lands_in_same_target_schema():
    source = _source("CREATE SCHEMA mysrc", "CREATE TABLE mysrc.t(x INTEGER)", "INSERT INTO mysrc.t VALUES (42)")
    target = _target()
    _bridge(source, target, _select("mysrc.t"))

    assert target.sql('SELECT * FROM "workspace"."mysrc".t').fetchone() == (42,)


def test_two_source_schemas_in_one_bridge():
    source = _source(
        "CREATE SCHEMA other",
        "CREATE TABLE t(x INTEGER)",
        "INSERT INTO t VALUES (1)",
        "CREATE TABLE other.t(x INTEGER)",
        "INSERT INTO other.t VALUES (2)",
    )
    target = _target()
    _bridge(source, target, _select("main.t") + _select("other.t"))

    assert target.sql(f"SELECT x FROM {T}").fetchone() == (1,)
    assert target.sql('SELECT x FROM "workspace"."other".t').fetchone() == (2,)


def test_insert_select_roundtrip():
    source = _source("CREATE TABLE t(x INTEGER PRIMARY KEY)")
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    target.sql(f"INSERT INTO {T} VALUES (10), (20), (30)")
    assert target.sql(f"SELECT * FROM {T} ORDER BY x").fetchall() == [(10,), (20,), (30,)]


def test_readwrite_update_expression():
    source = _source("CREATE TABLE t(id INTEGER PRIMARY KEY, counter INTEGER)", "INSERT INTO t VALUES (1, 10), (2, 20)")
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    target.sql(f"UPDATE {T} SET counter = counter + 5 WHERE id = 1")
    assert source.sql("SELECT counter FROM t WHERE id = 1").fetchone() == (15,)


def test_table_name_with_comma():
    source = _source(
        'CREATE TABLE "my,table"(x INTEGER)', 'INSERT INTO "my,table" VALUES (1)', "CREATE TABLE secret(x INTEGER)"
    )
    target = _target()
    _bridge(source, target, _select("main.my,table"))

    assert target.sql('SELECT * FROM "workspace"."main"."my,table"').fetchone() == (1,)
    with pytest.raises(duckdb.CatalogException):
        target.sql('SELECT * FROM "workspace"."main".secret')


def test_table_name_with_colon():
    source = _source('CREATE TABLE "a:b"(x INTEGER)', 'INSERT INTO "a:b" VALUES (42)')
    target = _target()
    _bridge(source, target, _select("main.a:b"))

    assert target.sql('SELECT * FROM "workspace"."main"."a:b"').fetchone() == (42,)


def test_nonexistent_source_table_rejected():
    with pytest.raises(duckdb.Error):
        _bridge(_source(), _target(), _select("main.nonexistent"))


def test_bridge_name_already_attached_rejected():
    source = _source("CREATE TABLE t(x INTEGER)")
    target = _target()
    with pytest.raises(duckdb.Error):
        bridge(source, target, "memory", source_catalog="memory", policies=_select("main.t"))


def test_unbridge_detaches_catalog():
    source = _source("CREATE TABLE t(x INTEGER)")
    target = _target()
    _bridge(source, target, _select("main.t"))
    target.sql(f"SELECT * FROM {T}")

    unbridge(target, "workspace")
    with pytest.raises(duckdb.BinderException, match='Catalog "workspace" does not exist'):
        target.sql(f"SELECT * FROM {T}")

    _bridge(source, target, _select("main.t"))
    assert target.sql(f"SELECT count(*) FROM {T}").fetchone() == (0,)


def test_create_view_over_bridge_table_works():
    source = _source("CREATE TABLE t(id INT, active BOOL)", "INSERT INTO t VALUES (1, true), (2, false)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    target.sql(f'CREATE OR REPLACE VIEW "workspace"."main"."active_t" AS SELECT * FROM {T} WHERE active = true')
    assert target.sql('SELECT id FROM "workspace"."main"."active_t"').fetchall() == [(1,)]


def test_readwrite_no_pk_refuses_keyed_writes():
    source = _source("CREATE TABLE nopk(x INTEGER, y INTEGER)", "INSERT INTO nopk VALUES (1, 10)")
    target = _target()
    _bridge(source, target, _all_verbs("main.nopk"))

    target.sql('INSERT INTO "workspace"."main".nopk VALUES (2, 20)')
    assert source.sql("SELECT count(*) FROM nopk").fetchone() == (2,)
    with pytest.raises(duckdb.BinderException, match="has no key"):
        target.sql('UPDATE "workspace"."main".nopk SET y = 99 WHERE x = 1')
    with pytest.raises(duckdb.BinderException, match="has no key"):
        target.sql('DELETE FROM "workspace"."main".nopk WHERE x = 1')


def test_readwrite_update_manual_pk_override():
    source = _source("CREATE TABLE nopk(x INTEGER, y INTEGER)", "INSERT INTO nopk VALUES (1, 10), (2, 20)")
    target = _target()
    _bridge(source, target, [Policy("main.nopk", "primary_key", ["x"]), *_all_verbs("main.nopk")])

    target.sql('UPDATE "workspace"."main".nopk SET y = 99 WHERE x = 1')
    assert source.sql("SELECT y FROM nopk WHERE x = 1").fetchone() == (99,)


def test_composite_pk_update():
    source = _source(
        "CREATE TABLE cpk(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a, b))",
        "INSERT INTO cpk VALUES (1, 1, 'orig'), (1, 2, 'orig')",
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.cpk"))

    target.sql('UPDATE "workspace"."main".cpk SET val = \'changed\' WHERE a = 1 AND b = 1')
    assert source.sql("SELECT val FROM cpk WHERE a = 1 AND b = 1").fetchone() == ("changed",)


def test_composite_pk_delete():
    source = _source(
        "CREATE TABLE cpk(a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a, b))",
        "INSERT INTO cpk VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')",
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.cpk"))

    target.sql('DELETE FROM "workspace"."main".cpk WHERE a = 1 AND b = 2')
    assert source.sql("SELECT a, b FROM cpk ORDER BY a, b").fetchall() == [(1, 1), (2, 1)]


def test_varchar_pk_update():
    source = _source(
        "CREATE TABLE vt(code VARCHAR PRIMARY KEY, val INTEGER)", "INSERT INTO vt VALUES ('abc', 10), ('def', 20)"
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.vt"))

    target.sql('UPDATE "workspace"."main".vt SET val = 99 WHERE code = \'abc\'')
    assert source.sql("SELECT val FROM vt WHERE code = 'abc'").fetchone() == (99,)


def test_varchar_pk_delete():
    source = _source(
        "CREATE TABLE vt(code VARCHAR PRIMARY KEY, val INTEGER)",
        "INSERT INTO vt VALUES ('abc', 10), ('def', 20), ('ghi', 30)",
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.vt"))

    target.sql('DELETE FROM "workspace"."main".vt WHERE code = \'def\'')
    assert source.sql("SELECT code FROM vt ORDER BY code").fetchall() == [("abc",), ("ghi",)]


def test_varchar_pk_with_single_quote_delete():
    source = _source(
        "CREATE TABLE qt(code VARCHAR PRIMARY KEY, val INTEGER)", "INSERT INTO qt VALUES ('it''s', 10), ('fine', 20)"
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.qt"))

    target.sql("DELETE FROM \"workspace\".\"main\".qt WHERE code = 'it''s'")
    assert source.sql("SELECT code FROM qt").fetchall() == [("fine",)]


def test_table_name_with_single_quote_pk_discovery():
    source = _source(
        """CREATE TABLE "tab'le"(id INTEGER PRIMARY KEY, val TEXT)""", """INSERT INTO "tab'le" VALUES (1, 'hello')"""
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.tab'le"))

    target.sql("""UPDATE "workspace"."main"."tab'le" SET val = 'updated' WHERE id = 1""")
    assert source.sql("""SELECT val FROM "tab'le" WHERE id = 1""").fetchone() == ("updated",)


def test_filter_value_containing_from():
    source = _source(
        "CREATE TABLE ft(code VARCHAR PRIMARY KEY, val INTEGER)",
        "INSERT INTO ft VALUES ('SELECT x FROM y', 10), ('normal', 20)",
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.ft"))

    target.sql("""UPDATE "workspace"."main".ft SET val = 99 WHERE code = 'SELECT x FROM y'""")
    assert source.sql("SELECT val FROM ft WHERE code = 'SELECT x FROM y'").fetchone() == (99,)


def test_read_only_table_without_pk_is_fine():
    source = _source("CREATE TABLE nopk(x INTEGER)", "INSERT INTO nopk VALUES (1)")
    target = _target()
    _bridge(source, target, _select("main.nopk"))

    assert target.sql('SELECT x FROM "workspace"."main".nopk').fetchone() == (1,)


def test_readwrite_sequential_scans():
    source = _source(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)",
        "INSERT INTO t VALUES (1, 'alice', 100), (2, 'bob', 200)",
    )
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    assert len(target.sql(f"SELECT * FROM {T} ORDER BY id").fetchall()) == 2
    assert target.sql(f"SELECT id, score FROM {T} ORDER BY id").fetchall() == [(1, 100), (2, 200)]


def test_insert_partial_column_list():
    source = _source("CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)")
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    target.sql(f"INSERT INTO {T} (id, name) VALUES (1, 'alice')")
    assert source.sql("SELECT id, name, score FROM t").fetchall() == [(1, "alice", None)]


def test_insert_reordered_column_list():
    source = _source("CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)")
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    target.sql(f"INSERT INTO {T} (score, id, name) VALUES (42, 1, 'alice')")
    assert source.sql("SELECT id, name, score FROM t").fetchall() == [(1, "alice", 42)]


def test_drop_bridge_table_rejected():
    source = _source("CREATE TABLE t(x INTEGER)")
    target = _target()
    _bridge(source, target, _select("main.t"))

    with pytest.raises(duckdb.Error):
        target.execute(f"DROP TABLE {T}")
    assert target.sql(f"SELECT count(*) FROM {T}").fetchone() == (0,)


def test_alter_bridge_table_rejected():
    source = _source("CREATE TABLE t(id INTEGER PRIMARY KEY, val VARCHAR)")
    target = _target()
    _bridge(source, target, _all_verbs("main.t"))

    with pytest.raises(duckdb.Error):
        target.execute(f"ALTER TABLE {T} ADD COLUMN extra INTEGER")


def _permissions(target, **filters):
    parts = ["'workspace'"] + [f'"{k}" := {v!r}' for k, v in filters.items()]
    rows = target.sql(
        "SELECT name, kind, verbs, primary_key " f"FROM n6k_table_permissions({', '.join(parts)}) ORDER BY name"
    ).fetchall()
    # Each row's verbs as (writes insert/update/delete, alters).
    return [
        (name, kind, {"insert", "update", "delete"} <= set(verbs), "alter" in verbs, pk)
        for name, kind, verbs, pk in rows
    ]


def test_table_permissions_bridge_capabilities():
    source = _source("CREATE TABLE rw(id INTEGER PRIMARY KEY, v VARCHAR)", "CREATE TABLE ro(x INTEGER)")
    target = _target()
    load_n6k(target)
    _bridge(source, target, _all_verbs("main.rw") + _select("main.ro"))

    assert _permissions(target, schema="main") == [
        ("ro", "bridge_read", False, False, []),
        ("rw", "bridge_readwrite", True, False, ["id"]),
    ]


def test_table_permissions_native_and_bridge_coexist():
    source = _source("CREATE TABLE bt(x INTEGER)")
    target = _target()
    load_n6k(target)
    _bridge(source, target, _select("main.bt"))
    target.execute('CREATE SCHEMA "workspace"."local"')
    target.execute('CREATE TABLE "workspace"."local".nt(y INTEGER)')

    assert _permissions(target) == [
        ("bt", "bridge_read", False, False, []),
        ("nt", "native_table", True, True, []),
    ]


def test_table_permissions_table_filter():
    source = _source("CREATE TABLE rw(id INTEGER PRIMARY KEY, v VARCHAR)", "CREATE TABLE ro(x INTEGER)")
    target = _target()
    load_n6k(target)
    _bridge(source, target, _all_verbs("main.rw") + _select("main.ro"))

    assert [r[:2] for r in _permissions(target, schema="main", table="rw")] == [("rw", "bridge_readwrite")]


def _bridged_struct_source():
    source = _source("CREATE TABLE t AS SELECT i, {'x': i, 'y': i * 2} AS s FROM range(10) t(i)")
    target = _target()
    _bridge(source, target, _select("main.t"))
    return source, target


def test_struct_field_filter_still_correct():
    _source_conn, target = _bridged_struct_source()
    assert target.sql(f"SELECT count(*) FROM {T} WHERE s.x = 3").fetchone() == (1,)


def test_pushed_filter_applies():
    _source_conn, target = _bridged_struct_source()
    assert target.sql(f"SELECT count(*) FROM {T} WHERE i = 3").fetchone() == (1,)
    assert target.sql(f"SELECT count(*) FROM {T} WHERE i >= 7").fetchone() == (3,)
    assert target.sql(f"SELECT count(*) FROM {T}").fetchone() == (10,)


# ---------------------------------------------------------------------------
# Transactions.
#
# Every case runs twice through _both: once against a native table on the target
# (memory.main.nat_t) and once against the bridge table, asserting the same rows
# and the same exception class. The native table is the oracle -- a divergence is
# a bug, and being stricter than DuckDB counts as one.
#
# Connection B is target.cursor(), a second ClientContext on the SAME target
# instance, so the native and the bridge case both see two real transactions.
# ---------------------------------------------------------------------------

NATIVE = '"memory"."main"."nat_t"'
BRIDGED = T


def _txn_fixture():
    source = _source("CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR)", "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    target = _target()
    target.execute("CREATE TABLE memory.main.nat_t(id INTEGER PRIMARY KEY, v VARCHAR)")
    target.execute("INSERT INTO memory.main.nat_t VALUES (1, 'a'), (2, 'b')")
    _bridge(source, target, _all_verbs("main.t"))
    return source, target, target.cursor()


def _both(body):
    for table in (NATIVE, BRIDGED):
        _source_conn, a, b = _txn_fixture()
        try:
            body(a, b, table)
        finally:
            for conn in (a, b):
                try:
                    conn.execute("ROLLBACK")
                except duckdb.Error:
                    pass


def _rows(conn, table):
    return conn.sql(f"SELECT id, v FROM {table} ORDER BY id").fetchall()


def test_txn_uncommitted_write_invisible_to_other_connection():
    def body(a, b, table):
        a.execute("BEGIN")
        a.execute(f"INSERT INTO {table} VALUES (3, 'c')")
        assert _rows(a, table) == [(1, "a"), (2, "b"), (3, "c")]
        assert _rows(b, table) == [(1, "a"), (2, "b")]
        a.execute("COMMIT")
        assert _rows(b, table) == [(1, "a"), (2, "b"), (3, "c")]

    _both(body)


def test_txn_repeatable_read():
    def body(a, b, table):
        a.execute("BEGIN")
        assert _rows(a, table) == [(1, "a"), (2, "b")]
        b.execute(f"INSERT INTO {table} VALUES (3, 'c')")
        assert _rows(a, table) == [(1, "a"), (2, "b")]
        a.execute("COMMIT")
        assert _rows(a, table) == [(1, "a"), (2, "b"), (3, "c")]

    _both(body)


def test_txn_rollback_leaves_other_connections_writes_alone():
    def body(a, b, table):
        b.execute(f"INSERT INTO {table} VALUES (3, 'c')")
        a.execute("BEGIN")
        a.execute(f"INSERT INTO {table} VALUES (4, 'd')")
        a.execute("ROLLBACK")
        assert _rows(a, table) == [(1, "a"), (2, "b"), (3, "c")]

    _both(body)


def test_txn_write_write_conflict_fails_fast():
    def body(a, b, table):
        a.execute("BEGIN")
        a.execute(f"UPDATE {table} SET v = 'from_a' WHERE id = 1")
        b.execute("BEGIN")
        with pytest.raises(duckdb.TransactionException):
            b.execute(f"UPDATE {table} SET v = 'from_b' WHERE id = 1")

    _both(body)


def test_txn_constraint_error_aborts_transaction():
    def body(a, _b, table):
        a.execute("BEGIN")
        with pytest.raises(duckdb.Error):
            a.execute(f"INSERT INTO {table} VALUES (1, 'dup')")
        with pytest.raises(duckdb.Error, match="Current transaction is aborted"):
            a.execute(f"SELECT count(*) FROM {table}")
        a.execute("ROLLBACK")
        assert _rows(a, table) == [(1, "a"), (2, "b")]

    _both(body)


def test_txn_binder_error_does_not_abort_transaction():
    def body(a, _b, table):
        a.execute("BEGIN")
        with pytest.raises(duckdb.BinderException):
            a.execute(f"SELECT no_such_column FROM {table}")
        a.execute(f"INSERT INTO {table} VALUES (3, 'c')")
        a.execute("COMMIT")
        assert _rows(a, table) == [(1, "a"), (2, "b"), (3, "c")]

    _both(body)


def test_txn_self_join_inside_transaction():
    _source_conn, target, _b = _txn_fixture()
    target.execute("BEGIN")
    for table in (NATIVE, BRIDGED):
        rows = target.sql(f"SELECT x.id, y.id FROM {table} x JOIN {table} y USING (v) ORDER BY x.id").fetchall()
        assert rows == [(1, 1), (2, 2)]
    target.execute("COMMIT")


# ---------------------------------------------------------------------------
# Single-writer rule.
#
# Each duckdb.connect() is its own DatabaseInstance, so two sources bridged into
# one target is a genuine two-WAL transaction -- the only shape in which the rule
# can fire.
# ---------------------------------------------------------------------------


def _two_source_target():
    sources = []
    target = _target()
    target.execute("CREATE TABLE memory.main.nat_t(id INTEGER PRIMARY KEY, v VARCHAR)")
    for catalog in ("src_a", "src_b"):
        source = _source("CREATE TABLE t(id INTEGER PRIMARY KEY, v VARCHAR)", "INSERT INTO t VALUES (1, 'a')")
        bridge(source, target, catalog, source_catalog="memory", policies=_all_verbs("main.t"))
        sources.append(source)
    return sources, target


SINGLE_WRITER = "single transaction can only write to a single attached database"


def test_single_writer_two_sources_rejected():
    (source_a, source_b), target = _two_source_target()
    target.execute("BEGIN")
    target.execute('UPDATE "src_a"."main"."t" SET v = \'x\' WHERE id = 1')
    with pytest.raises(duckdb.TransactionException, match=SINGLE_WRITER):
        target.execute('UPDATE "src_b"."main"."t" SET v = \'x\' WHERE id = 1')
    target.execute("ROLLBACK")

    assert source_a.sql("SELECT v FROM t WHERE id = 1").fetchone() == ("a",)
    assert source_b.sql("SELECT v FROM t WHERE id = 1").fetchone() == ("a",)


def test_single_writer_target_then_source_rejected():
    (source_a, _source_b), target = _two_source_target()
    target.execute("BEGIN")
    target.execute("INSERT INTO memory.main.nat_t VALUES (1, 'n')")
    with pytest.raises(duckdb.TransactionException, match=SINGLE_WRITER):
        target.execute('UPDATE "src_a"."main"."t" SET v = \'x\' WHERE id = 1')
    target.execute("ROLLBACK")

    assert source_a.sql("SELECT v FROM t WHERE id = 1").fetchone() == ("a",)
    assert target.sql("SELECT count(*) FROM memory.main.nat_t").fetchone() == (0,)


def test_single_writer_readers_are_unrestricted():
    (source_a, _source_b), target = _two_source_target()
    target.execute("BEGIN")
    assert target.sql('SELECT count(*) FROM "src_a"."main"."t"').fetchone() == (1,)
    assert target.sql('SELECT count(*) FROM "src_b"."main"."t"').fetchone() == (1,)
    assert target.sql("SELECT count(*) FROM memory.main.nat_t").fetchone() == (0,)
    target.execute('UPDATE "src_a"."main"."t" SET v = \'x\' WHERE id = 1')
    target.execute("COMMIT")

    assert source_a.sql("SELECT v FROM t WHERE id = 1").fetchone() == ("x",)


# ---------------------------------------------------------------------------
# Same-instance bridging: source DatabaseInstance == target DatabaseInstance.
# The source side still runs in its own transaction on that instance, so the two
# xfails below are the gap between that and a native table; strict so a fix flips them.
# ---------------------------------------------------------------------------

SEPARATE_TXN = "source connection is a separate transaction on the same instance"
SAME_INST_BRIDGED = '"workspace"."main"."src_t"'


def _same_instance_fixture():
    conn = _target()
    conn.execute("CREATE TABLE memory.main.src_t(id INTEGER PRIMARY KEY, v VARCHAR)")
    conn.execute("INSERT INTO memory.main.src_t VALUES (1, 'a'), (2, 'b')")
    conn.execute("CREATE TABLE memory.main.nat_t(id INTEGER PRIMARY KEY, v VARCHAR)")
    conn.execute("INSERT INTO memory.main.nat_t VALUES (1, 'a'), (2, 'b')")
    _bridge(conn, conn, _all_verbs("main.src_t"))
    return conn


def test_same_instance_read_and_write():
    conn = _same_instance_fixture()
    assert conn.sql(f"SELECT count(*) FROM {SAME_INST_BRIDGED}").fetchone() == (2,)
    conn.execute(f"UPDATE {SAME_INST_BRIDGED} SET v = 'z' WHERE id = 1")
    assert conn.sql("SELECT v FROM memory.main.src_t WHERE id = 1").fetchone() == ("z",)


@pytest.mark.xfail(strict=True, reason=SEPARATE_TXN)
def test_txn_source_write_visible_through_bridge():
    conn = _same_instance_fixture()
    conn.execute("BEGIN")
    conn.execute("INSERT INTO memory.main.src_t VALUES (3, 'c')")
    assert conn.sql("SELECT count(*) FROM memory.main.src_t").fetchone() == (3,)
    assert conn.sql(f"SELECT count(*) FROM {SAME_INST_BRIDGED}").fetchone() == (3,)
    conn.execute("ROLLBACK")


@pytest.mark.xfail(strict=True, reason=SEPARATE_TXN)
def test_txn_bridge_write_visible_to_source_read():
    conn = _same_instance_fixture()
    conn.execute("BEGIN")
    conn.execute(f"INSERT INTO {SAME_INST_BRIDGED} VALUES (3, 'c')")
    assert conn.sql("SELECT count(*) FROM memory.main.src_t").fetchone() == (3,)
    conn.execute("ROLLBACK")


def _tenant_source():
    return _source(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, tenant_id INTEGER, name VARCHAR)",
        "INSERT INTO t VALUES (1, 42, 'mine'), (2, 7, 'theirs')",
    )


def test_policies_select_restricts_rows():
    target = _target()
    _bridge(_tenant_source(), target, [Policy("main.t", "select", "tenant_id = 42")])

    assert target.sql(f"SELECT id FROM {T}").fetchall() == [(1,)]


def test_policies_select_only_blocks_update():
    target = _target()
    _bridge(_tenant_source(), target, [Policy("main.t", "select", "true")])

    with pytest.raises(duckdb.Error):
        target.sql(f"UPDATE {T} SET name = 'x' WHERE id = 1")


def test_policies_update_reaches_only_permitted_rows():
    source = _tenant_source()
    target = _target()
    _bridge(
        source,
        target,
        [Policy("main.t", "select", "true"), Policy("main.t", "update", "tenant_id = 42")],
    )

    target.sql(f"UPDATE {T} SET name = 'edited'")
    assert source.sql("SELECT id, name FROM t ORDER BY id").fetchall() == [(1, "edited"), (2, "theirs")]


def test_policies_insert_check_rejects_row():
    source = _tenant_source()
    target = _target()
    _bridge(
        source,
        target,
        [Policy("main.t", "select", "true"), Policy("main.t", "insert", "tenant_id = 42")],
    )

    target.sql(f"INSERT INTO {T} VALUES (3, 42, 'ok')")
    with pytest.raises(duckdb.Error):
        target.sql(f"INSERT INTO {T} VALUES (4, 7, 'nope')")
    assert source.sql("SELECT id FROM t ORDER BY id").fetchall() == [(1,), (2,), (3,)]


def test_policies_delete_ignores_select_policy():
    source = _tenant_source()
    target = _target()
    _bridge(
        source,
        target,
        [Policy("main.t", "select", "tenant_id = 42"), Policy("main.t", "delete", "true")],
    )

    target.sql(f"DELETE FROM {T}")
    assert source.sql("SELECT id FROM t ORDER BY id").fetchall() == []


def test_unqualified_policy_table_rejected():
    with pytest.raises(duckdb.Error, match="schema.table"):
        _bridge(_source(), _target(), [Policy("t", "select", "true")])


def test_policies_primary_key_override():
    source = _source(
        "CREATE TABLE t(id INTEGER, name VARCHAR)",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b')",
    )
    target = _target()
    _bridge(
        source,
        target,
        [
            Policy("main.t", "primary_key", ["id"]),
            Policy("main.t", "select", "true"),
            Policy("main.t", "update", "true"),
        ],
    )

    target.sql(f"UPDATE {T} SET name = 'z' WHERE id = 2")
    assert source.sql("SELECT id, name FROM t ORDER BY id").fetchall() == [(1, "a"), (2, "z")]


def test_context_select_restricts_rows():
    target = _target()
    _bridge(_tenant_source(), target, [Policy("main.t", "select", "tenant_id = $tenant")], context={"tenant": 42})

    assert target.sql(f"SELECT id FROM {T}").fetchall() == [(1,)]


def test_context_insert_check_rejects_row():
    source = _tenant_source()
    target = _target()
    _bridge(
        source,
        target,
        [Policy("main.t", "select", "true"), Policy("main.t", "insert", "tenant_id = $tenant")],
        context={"tenant": 42},
    )

    target.sql(f"INSERT INTO {T} VALUES (3, 42, 'ok')")
    with pytest.raises(duckdb.Error):
        target.sql(f"INSERT INTO {T} VALUES (4, 7, 'nope')")
    assert source.sql("SELECT id FROM t ORDER BY id").fetchall() == [(1,), (2,), (3,)]


def test_context_unset_name_rejected():
    with pytest.raises(duckdb.Error, match="which is not set"):
        _bridge(_tenant_source(), _target(), [Policy("main.t", "select", "tenant_id = $tenant")])


def test_context_finalized_after_bridge():
    source = _tenant_source()
    target = _target()
    bridge_id = _bridge(source, target, [Policy("main.t", "select", "tenant_id = $tenant")], context={"tenant": 42})

    with pytest.raises(duckdb.Error, match="is finalized"):
        source.sql("SELECT vcatbridge_set_context($1, 'tenant', 7)", params=[bridge_id]).fetchone()


def test_context_none_value_rejected():
    with pytest.raises(duckdb.Error, match="cannot be NULL"):
        _bridge(
            _tenant_source(), _target(), [Policy("main.t", "select", "tenant_id = $tenant")], context={"tenant": None}
        )
