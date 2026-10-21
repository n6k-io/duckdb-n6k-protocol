"""Tests for the Provider-backed dynamic virtual-table API.

Providers require a provider catalog (ATTACH ... (TYPE virtual_catalog_provider)); the
extension materialises the schemas a provider lists.

Queries run via asyncio.to_thread so DuckDB executes on a worker thread; the
provider's async methods marshal back to the test's event loop via
asyncio.run_coroutine_threadsafe inside the per-probe UDFs. Running queries
synchronously on the loop thread would deadlock.
"""

import asyncio
import gc
import weakref
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

import duckdb
import pyarrow as pa
import pytest

from n6k_server.extension import load_n6k, load_virtual_catalog_provider
from n6k_protocol.filters import Filters, filter_and_project
from n6k_server.provider import (
    PK_METADATA_KEY,
    Provider,
    TableNotFound,
    _catalogs,
    invalidate_provider_tables,
    register_provider,
    release_providers,
    unregister_provider,
)

CONN_CONFIG: dict[str, str | bool | int | float | list[str]] = {"allow_unsigned_extensions": "true"}


def _new_n6k_conn(schema_name: str = "public") -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(config=CONN_CONFIG)
    load_virtual_catalog_provider(conn)
    load_n6k(conn)
    conn.execute("ATTACH ':memory:' AS workspace (TYPE virtual_catalog_provider)")
    conn.execute(f"CREATE SCHEMA workspace.{schema_name}")
    return conn


class _FakeStore(Provider):
    """In-memory provider for tests. Tracks call counts so cache-hit
    behavior is observable."""

    def __init__(self) -> None:
        self.tables: dict[str, pa.Table] = {}
        self.pks: dict[str, list[str]] = {}
        self.schema_calls = 0
        self.list_calls = 0
        self.scan_calls = 0

    def set_table(self, name: str, table: pa.Table, pks: Optional[list[str]] = None) -> None:
        if pks:
            meta = {PK_METADATA_KEY: ",".join(pks).encode("utf-8")}
            table = table.replace_schema_metadata({**(table.schema.metadata or {}), **meta})
            self.pks[name] = pks
        self.tables[name] = table

    async def list_tables(self) -> list[str]:
        self.list_calls += 1
        return sorted(self.tables.keys())

    async def schema(self, name: str) -> pa.Schema:
        self.schema_calls += 1
        if name not in self.tables:
            raise TableNotFound(name)
        return self.tables[name].schema

    async def scan(
        self,
        name: str,
        columns: Optional[list[str]],
        filters: Filters,
    ) -> pa.Table:
        self.scan_calls += 1
        if name not in self.tables:
            raise TableNotFound(name)
        table = self.tables[name]
        if columns is not None:
            table = table.select(columns)
        return table

    async def insert(self, name: str, rows: pa.Table) -> int:
        existing = self.tables[name]
        self.tables[name] = pa.concat_tables([existing, rows.select(existing.column_names)])
        return rows.num_rows

    async def update(self, name: str, rows: pa.Table, keys: list[str]) -> int:
        return rows.num_rows

    async def delete(self, name: str, keys: pa.Table) -> int:
        return keys.num_rows


@pytest.mark.asyncio
async def test_read_from_provider() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("t", pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(lambda: conn.sql("SELECT * FROM workspace.public.t").fetchall())
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


@pytest.mark.asyncio
async def test_show_tables_lists_provider_tables() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    store.set_table("b", pa.table({"y": [2]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["a", "b"]


@pytest.mark.asyncio
async def test_invalidate_reveals_new_table() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public'"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["a"]

    store.set_table("b", pa.table({"y": [2]}))
    await invalidate_provider_tables(conn, catalog="workspace")
    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["a", "b"]


@pytest.mark.asyncio
async def test_invalidate_on_provider_instance() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    store.set_table("b", pa.table({"y": [2]}))
    await store.invalidate()

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["a", "b"]


@pytest.mark.asyncio
async def test_insert_routes_to_provider() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table(
        "t",
        pa.table({"id": pa.array([1], type=pa.int64()), "name": pa.array(["a"], type=pa.string())}),
        pks=["id"],
    )
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("INSERT INTO workspace.public.t VALUES (2, 'b')"))
    assert store.tables["t"].num_rows == 2


@pytest.mark.asyncio
async def test_unregister_removes_provider_tables() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public'"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["a"]

    await unregister_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public'"
        ).fetchall()
    )
    assert rows == []

    with pytest.raises(duckdb.Error):
        await asyncio.to_thread(lambda: conn.sql("SELECT * FROM workspace.public.a").fetchall())


@pytest.mark.asyncio
async def test_unregister_on_provider_instance() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    probe_id = _catalogs[conn]["workspace"].probe_id

    await store.unregister()

    assert store._binding is None

    # UDFs must be gone — direct invocation errors out.
    with pytest.raises(duckdb.Error):
        await asyncio.to_thread(lambda: conn.sql(f"SELECT __n6k_provider_list_{probe_id}()").fetchall())


@pytest.mark.asyncio
async def test_unregister_then_reregister_same_schema() -> None:
    conn = _new_n6k_conn()
    first = _FakeStore()
    first.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=first)
    await first.unregister()

    second = _FakeStore()
    second.set_table("b", pa.table({"y": [2]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=second)

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public'"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["b"]


@pytest.mark.asyncio
async def test_unregister_twice_raises() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)
    await store.unregister()
    with pytest.raises(duckdb.Error):
        await asyncio.to_thread(lambda: conn.sql("SELECT provider_unregister('workspace')").fetchall())


@pytest.mark.asyncio
async def test_unregister_clears_binding() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await store.unregister()

    with pytest.raises(RuntimeError):
        await store.invalidate()
    with pytest.raises(RuntimeError):
        await store.unregister()


@pytest.mark.asyncio
async def test_unregister_keeps_native_tables_intact() -> None:
    conn = _new_n6k_conn()
    conn.execute("CREATE TABLE workspace.public.native_t(x INTEGER)")
    conn.execute("INSERT INTO workspace.public.native_t VALUES (99)")

    store = _FakeStore()
    store.set_table("prov_t", pa.table({"id": [1, 2]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await store.unregister()

    rows = await asyncio.to_thread(lambda: conn.sql("SELECT x FROM workspace.public.native_t").fetchall())
    assert rows == [(99,)]


@pytest.mark.asyncio
async def test_unregistered_provider_invalidate_raises() -> None:
    store = _FakeStore()
    with pytest.raises(RuntimeError):
        await store.invalidate()


@pytest.mark.asyncio
async def test_register_without_attach_errors() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    load_virtual_catalog_provider(conn)
    store = _FakeStore()
    with pytest.raises(duckdb.Error):
        register_provider(conn, catalog="memory", schema_name="main", provider=store)
    assert store._binding is None


@pytest.mark.asyncio
async def test_register_materialises_schema_on_demand() -> None:
    conn = duckdb.connect(config=CONN_CONFIG)
    load_virtual_catalog_provider(conn)
    conn.execute("ATTACH ':memory:' AS workspace (TYPE virtual_catalog_provider)")
    store = _FakeStore()
    store.set_table("t", pa.table({"id": [1]}))
    register_provider(conn, catalog="workspace", schema_name="nosuch", provider=store)
    rows = await asyncio.to_thread(lambda: conn.sql("SELECT id FROM workspace.nosuch.t").fetchall())
    assert rows == [(1,)]


@pytest.mark.asyncio
async def test_two_schemas_share_one_catalog() -> None:
    conn = _new_n6k_conn()
    a = _FakeStore()
    a.set_table("t", pa.table({"id": [1]}))
    b = _FakeStore()
    b.set_table("t", pa.table({"id": [2]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=a)
    register_provider(conn, catalog="workspace", schema_name="other", provider=b)

    rows = await asyncio.to_thread(
        lambda: conn.sql("SELECT * FROM workspace.public.t UNION ALL SELECT * FROM workspace.other.t").fetchall()
    )
    assert sorted(rows) == [(1,), (2,)]

    await a.unregister()
    rows = await asyncio.to_thread(lambda: conn.sql("SELECT id FROM workspace.other.t").fetchall())
    assert rows == [(2,)]
    with pytest.raises(duckdb.Error):
        await asyncio.to_thread(lambda: conn.sql("SELECT * FROM workspace.public.t").fetchall())


@pytest.mark.asyncio
async def test_provider_and_native_coexist_in_same_schema() -> None:
    conn = _new_n6k_conn()
    conn.execute("CREATE TABLE workspace.public.native_t(x INTEGER)")
    conn.execute("INSERT INTO workspace.public.native_t VALUES (99)")

    store = _FakeStore()
    store.set_table("prov_t", pa.table({"id": [1, 2]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["native_t", "prov_t"]

    native_rows = await asyncio.to_thread(lambda: conn.sql("SELECT x FROM workspace.public.native_t").fetchall())
    assert native_rows == [(99,)]

    prov_rows = await asyncio.to_thread(
        lambda: conn.sql("SELECT id FROM workspace.public.prov_t ORDER BY id").fetchall()
    )
    assert prov_rows == [(1,), (2,)]


@pytest.mark.asyncio
async def test_drop_provider_table_rejected() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("t", pa.table({"id": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    with pytest.raises(duckdb.Error, match="provider-managed"):
        await asyncio.to_thread(lambda: conn.execute("DROP TABLE workspace.public.t"))


@pytest.mark.asyncio
async def test_drop_native_table_still_works_in_provider_schema() -> None:
    conn = _new_n6k_conn()
    conn.execute("CREATE TABLE workspace.public.native_t(x INTEGER)")
    store = _FakeStore()
    store.set_table("prov_t", pa.table({"id": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("DROP TABLE workspace.public.native_t"))
    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["prov_t"]


@pytest.mark.asyncio
async def test_alter_provider_table_routes_to_provider() -> None:
    class AltStore(_FakeStore):
        def __init__(self) -> None:
            super().__init__()
            self.alter_calls: list[tuple[str, str, dict[str, Any]]] = []

        async def alter(self, name: str, change: Any) -> None:
            self.alter_calls.append((name, change.kind, dict(change.details)))

    conn = _new_n6k_conn()
    store = AltStore()
    store.set_table("t", pa.table({"id": pa.array([1], type=pa.int64())}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("ALTER TABLE workspace.public.t ADD COLUMN extra VARCHAR"))
    assert store.alter_calls == [("t", "add_column", {"name": "extra", "type": "VARCHAR"})]


@pytest.mark.asyncio
async def test_alter_native_table_still_works_in_provider_schema() -> None:
    conn = _new_n6k_conn()
    conn.execute("CREATE TABLE workspace.public.n(x INTEGER)")
    store = _FakeStore()
    store.set_table("p", pa.table({"id": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("ALTER TABLE workspace.public.n ADD COLUMN y VARCHAR"))
    rows = await asyncio.to_thread(lambda: conn.sql("DESCRIBE workspace.public.n").fetchall())
    assert [r[0] for r in rows] == ["x", "y"]


@pytest.mark.asyncio
async def test_invalidate_hides_removed_table() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    store.set_table("b", pa.table({"y": [2]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["a", "b"]

    del store.tables["a"]
    await store.invalidate()

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='workspace' AND table_schema='public' ORDER BY table_name"
        ).fetchall()
    )
    assert [r[0] for r in rows] == ["b"]

    with pytest.raises(duckdb.Error):
        await asyncio.to_thread(lambda: conn.sql("SELECT * FROM workspace.public.a").fetchall())


@pytest.mark.asyncio
async def test_invalidate_reveals_schema_change() -> None:
    """Proves the per-entry schema cache flushes on version bump, not just the
    name list."""
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("t", pa.table({"id": pa.array([1, 2], type=pa.int64())}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    cols_before = await asyncio.to_thread(lambda: [r[0] for r in conn.sql("DESCRIBE workspace.public.t").fetchall()])
    assert cols_before == ["id"]

    store.set_table(
        "t",
        pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "name": pa.array(["a", "b"], type=pa.string()),
            }
        ),
    )
    await store.invalidate()

    cols_after = await asyncio.to_thread(lambda: [r[0] for r in conn.sql("DESCRIBE workspace.public.t").fetchall()])
    assert cols_after == ["id", "name"]


@pytest.mark.asyncio
async def test_scan_receives_projected_columns() -> None:
    class TrackStore(_FakeStore):
        def __init__(self) -> None:
            super().__init__()
            self.scan_calls_with_columns: list[Optional[list[str]]] = []

        async def scan(
            self,
            name: str,
            columns: Optional[list[str]],
            filters: Filters,
        ) -> pa.Table:
            self.scan_calls_with_columns.append(columns)
            return await super().scan(name, columns, filters)

    conn = _new_n6k_conn()
    store = TrackStore()
    store.set_table(
        "t",
        pa.table({"id": [1, 2], "name": ["a", "b"], "extra": ["x", "y"]}),
    )
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.sql("SELECT id, name FROM workspace.public.t").fetchall())
    assert store.scan_calls_with_columns == [["id", "name"]]


@pytest.mark.asyncio
async def test_scan_post_projection_fallback() -> None:
    """Providers that ignore `columns` and return all columns still produce
    the correct result — the library projects down before returning to
    DuckDB."""

    class NaiveStore(_FakeStore):
        async def scan(
            self,
            name: str,
            columns: Optional[list[str]],
            filters: Filters,
        ) -> pa.Table:
            # Intentionally ignore `columns` — always return the full table.
            return self.tables[name]

    conn = _new_n6k_conn()
    store = NaiveStore()
    store.set_table(
        "t",
        pa.table({"id": [1, 2], "name": ["a", "b"], "extra": ["x", "y"]}),
    )
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(lambda: conn.sql("SELECT id, name FROM workspace.public.t ORDER BY id").fetchall())
    assert rows == [(1, "a"), (2, "b")]


# ── Filter-pushdown fidelity ────────────────────────────────────────────────
#
# Exercises the full filter serialization pipeline:
#   DuckDB pushdown → C++ filter_json::SerializeFilters → JSON wire →
#   Python _do_scan json.loads → tuple → Provider.scan(filters=...)
#
# These are the only tests that catch dangling-pointer / lifetime bugs in
# the C++ yyjson serializer — assertions match the op string byte-for-byte,
# so a regression that produces ``'\x00'`` or garbage fails loudly here.


class _CapturingStore(Provider):
    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.captured: list[Filters] = []

    async def list_tables(self) -> list[str]:
        return ["t"]

    async def schema(self, name: str) -> pa.Schema:
        return self.table.schema

    async def scan(
        self,
        name: str,
        columns: Optional[list[str]],
        filters: Filters,
    ) -> pa.Table:
        self.captured.append(filters)
        return self.table

    async def insert(self, name: str, rows: pa.Table) -> int:
        return 0

    async def update(self, name: str, rows: pa.Table, keys: list[str]) -> int:
        return 0

    async def delete(self, name: str, keys: pa.Table) -> int:
        return 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "where,expected",
    [
        ("id = 1", [("id", "=", 1)]),
        ("id != 1", [("id", "!=", 1)]),
        ("id > 5", [("id", ">", 5)]),
        ("id >= 5", [("id", ">=", 5)]),
        ("id < 5", [("id", "<", 5)]),
        ("id <= 5", [("id", "<=", 5)]),
    ],
)
async def test_provider_filter_pushdown_roundtrip(where: str, expected: Filters) -> None:
    # DuckDB's Arrow scan path does not push ``IS NULL`` / ``IS NOT NULL``
    # down to the TableFilterSet in practice — those cases are evaluated
    # post-scan. Comparison ops are enough to exercise the C++ serializer's
    # op-lifetime + URL-encode + JSON-tuple wire format in full.
    conn = _new_n6k_conn()
    store = _CapturingStore(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.sql(f"SELECT id FROM workspace.public.t WHERE {where}").fetchall())

    assert store.captured, "provider.scan was never called"
    assert store.captured[-1] == expected


@pytest.mark.asyncio
async def test_provider_filter_pushdown_multiple_clauses() -> None:
    conn = _new_n6k_conn()
    store = _CapturingStore(pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.sql("SELECT id FROM workspace.public.t WHERE id > 1 AND id < 10").fetchall())
    assert store.captured[-1] is not None
    clauses = sorted(store.captured[-1])
    assert clauses == [("id", "<", 10), ("id", ">", 1)]


# ── n6k_table_permissions + provider capability optionality ─────────────────
#
# A provider declares a write/edit capability by *implementing* the method;
# leaving it as the base NotImplementedError stub leaves the UDF unregistered,
# so the table's verbs leave it out and the op is rejected at plan time.
# n6k_table_permissions exposes those verbs.


class _ReadOnlyStore(Provider):
    """Provider implementing only the mandatory read path (no write/edit ops)."""

    def __init__(self) -> None:
        self.tables: dict[str, pa.Table] = {}

    def set_table(self, name: str, table: pa.Table) -> None:
        self.tables[name] = table

    async def list_tables(self) -> list[str]:
        return sorted(self.tables.keys())

    async def schema(self, name: str) -> pa.Schema:
        if name not in self.tables:
            raise TableNotFound(name)
        return self.tables[name].schema

    async def scan(self, name: str, columns: Optional[list[str]], filters: Filters) -> pa.Table:
        table = self.tables[name]
        if columns is not None:
            table = table.select(columns)
        return table


class _EditableStore(_FakeStore):
    """Full provider that also implements alter() (writes + alter)."""

    def __init__(self) -> None:
        super().__init__()
        self.alter_calls: list[tuple[str, str]] = []

    async def alter(self, name: str, change: Any) -> None:
        self.alter_calls.append((name, change.kind))


async def _permissions(
    conn: duckdb.DuckDBPyConnection,
    *,
    catalog: str = "workspace",
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> list[tuple[Any, ...]]:
    parts = [repr(catalog)]
    if schema is not None:
        parts.append(f"schema := {schema!r}")
    if table is not None:
        parts.append(f'"table" := {table!r}')
    sql = (
        "SELECT schema, name, kind, verbs, primary_key "
        f"FROM n6k_table_permissions({', '.join(parts)}) ORDER BY name"
    )
    return _writes_and_alter(await asyncio.to_thread(lambda: conn.sql(sql).fetchall()))


def _writes_and_alter(rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """Each row's verbs as (writes insert/update/delete, alters)."""
    out = []
    for *head, verbs, pk in rows:
        writes = {"insert", "update", "delete"} <= set(verbs)
        out.append((*head, writes, "alter" in verbs, pk))
    return out


@pytest.mark.asyncio
async def test_table_permissions_full_provider_writes_without_alter() -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()  # implements insert/update/delete, not alter
    store.set_table("t", pa.table({"id": [1]}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    # Provider PK (from Arrow vcat.primary_keys metadata) surfaces in the column.
    assert await _permissions(conn, table="t") == [("public", "t", "provider", True, False, ["id"])]


@pytest.mark.asyncio
async def test_table_permissions_read_only_provider_rejects_writes() -> None:
    conn = _new_n6k_conn()
    store = _ReadOnlyStore()
    store.set_table("t", pa.table({"id": pa.array([1], type=pa.int64())}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    assert await _permissions(conn, table="t") == [("public", "t", "provider", False, False, [])]

    with pytest.raises(duckdb.Error, match="does not support INSERT"):
        await asyncio.to_thread(lambda: conn.execute("INSERT INTO workspace.public.t VALUES (2)"))
    with pytest.raises(duckdb.Error, match="does not support DELETE"):
        await asyncio.to_thread(lambda: conn.execute("DELETE FROM workspace.public.t WHERE id = 1"))


@pytest.mark.asyncio
async def test_table_permissions_editable_provider() -> None:
    conn = _new_n6k_conn()
    store = _EditableStore()
    store.set_table("t", pa.table({"id": pa.array([1], type=pa.int64())}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    assert await _permissions(conn, table="t") == [("public", "t", "provider", True, True, [])]


@pytest.mark.asyncio
async def test_table_permissions_native_table_and_view() -> None:
    conn = _new_n6k_conn()
    conn.execute("CREATE TABLE workspace.public.nt(x INTEGER PRIMARY KEY)")
    conn.execute("CREATE VIEW workspace.public.nv AS SELECT 1 AS y")

    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT schema, name, kind, verbs, primary_key "
            "FROM n6k_table_permissions('workspace', schema := 'public') ORDER BY name"
        ).fetchall()
    )
    # The provider extension's own grants, passed through.
    assert rows == [
        ("public", "nt", "native_table", ["select", "insert", "update", "delete", "alter"], ["x"]),
        ("public", "nv", "native_view", ["select"], []),
    ]


@pytest.mark.asyncio
async def test_table_permissions_plain_catalog() -> None:
    """On a plain (non-virtual_catalog) catalog the function reports native tables/views."""
    conn = duckdb.connect(config=CONN_CONFIG)
    load_n6k(conn)
    conn.execute("CREATE TABLE memory.main.nt(x INTEGER PRIMARY KEY)")
    conn.execute("CREATE VIEW memory.main.nv AS SELECT 1 AS y")
    rows = await asyncio.to_thread(
        lambda: conn.sql(
            "SELECT schema, name, kind, verbs, primary_key "
            "FROM n6k_table_permissions('memory', schema := 'main') ORDER BY name"
        ).fetchall()
    )
    assert rows == [
        ("main", "nt", "native_table", ["select", "insert", "update", "delete", "alter", "drop"], ["x"]),
        ("main", "nv", "native_view", ["select", "drop"], []),
    ]


# ── UPDATE / DELETE: what actually reaches the provider ─────────────────────
#
# These assert the CONTENTS of the rows, not just the row count. The key values
# do not come from the scan the way an INSERT's rows do: DuckDB hands the DML
# operator a row id, which the extension resolves back through its own primary
# key buffer and stitches into a row. Only checking a count would pass with the
# columns transposed, the wrong key, or a stale value.


class _RecordingStore(_FakeStore):
    """Keeps every table an UPDATE or DELETE handed over, so a test can look.

    Unlike `_FakeStore` this one honours `filters`, which a WHERE clause here
    depends on: DuckDB pushes the predicate down and does NOT re-filter what
    comes back, so a provider that ignores it deletes the whole table.
    """

    def __init__(self) -> None:
        super().__init__()
        self.updates: list[tuple[pa.Table, list[str]]] = []
        self.deletes: list[pa.Table] = []

    async def scan(
        self,
        name: str,
        columns: Optional[list[str]],
        filters: Filters,
    ) -> pa.Table:
        self.scan_calls += 1
        if name not in self.tables:
            raise TableNotFound(name)
        return filter_and_project(self.tables[name], columns, filters)

    async def update(self, name: str, rows: pa.Table, keys: list[str]) -> int:
        self.updates.append((rows, keys))
        return rows.num_rows

    async def delete(self, name: str, keys: pa.Table) -> int:
        self.deletes.append(keys)
        return keys.num_rows


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


@pytest.mark.asyncio
async def test_delete_sends_the_matching_primary_keys() -> None:
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table("t", pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("DELETE FROM workspace.public.t WHERE id = 2"))

    assert len(store.deletes) == 1
    assert store.deletes[0].column_names == ["id"]
    assert _rows(store.deletes[0]) == [{"id": 2}]


@pytest.mark.asyncio
async def test_delete_without_a_filter_sends_every_key() -> None:
    """No WHERE clause means the scan projects nothing but the row id, so the key
    columns are the only thing in the payload."""
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table("t", pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("DELETE FROM workspace.public.t"))

    assert len(store.deletes) == 1
    assert _rows(store.deletes[0]) == [{"id": 1}, {"id": 2}, {"id": 3}]


@pytest.mark.asyncio
async def test_delete_with_a_varchar_primary_key() -> None:
    """A text key exercises the other half of the key reader: offsets and a data
    buffer rather than a fixed-width slot."""
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table(
        "t",
        pa.table({"code": pa.array(["aa", "bb", "cc"], type=pa.utf8()), "n": [1, 2, 3]}),
        pks=["code"],
    )
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("DELETE FROM workspace.public.t WHERE code = 'bb'"))

    assert _rows(store.deletes[0]) == [{"code": "bb"}]


@pytest.mark.asyncio
async def test_delete_with_a_composite_primary_key() -> None:
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table(
        "t",
        pa.table({"a": [1, 1, 2], "b": pa.array(["x", "y", "x"], type=pa.utf8()), "v": [10, 20, 30]}),
        pks=["a", "b"],
    )
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("DELETE FROM workspace.public.t WHERE v = 20"))

    assert store.deletes[0].column_names == ["a", "b"]
    assert _rows(store.deletes[0]) == [{"a": 1, "b": "y"}]


@pytest.mark.asyncio
async def test_update_sends_the_key_then_the_changed_columns() -> None:
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table("t", pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("UPDATE workspace.public.t SET name = 'z' WHERE id = 2"))

    assert len(store.updates) == 1
    rows, keys = store.updates[0]
    # Primary key first, then only the columns the statement touched; `keys` names
    # that second half so the provider knows where the key columns stop.
    assert rows.column_names == ["id", "name"]
    assert keys == ["name"]
    assert _rows(rows) == [{"id": 2, "name": "z"}]


@pytest.mark.asyncio
async def test_update_carries_the_key_even_when_it_is_also_read() -> None:
    """`id` is both the primary key and a filter column, so the scan projects it
    AND the row-id machinery needs it appended. Getting that wrong reads the
    wrong column as the key."""
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table("t", pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "n": [10, 20, 30]}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("UPDATE workspace.public.t SET name = 'z' WHERE id > 1"))

    rows, keys = store.updates[0]
    assert keys == ["name"]
    assert _rows(rows) == [{"id": 2, "name": "z"}, {"id": 3, "name": "z"}]


@pytest.mark.asyncio
async def test_update_of_several_columns_at_once() -> None:
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table("t", pa.table({"id": [1, 2], "name": ["a", "b"], "n": [10, 20]}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("UPDATE workspace.public.t SET name = 'z', n = 99 WHERE id = 1"))

    rows, keys = store.updates[0]
    assert sorted(keys) == ["n", "name"]
    assert _rows(rows) == [{"id": 1, "name": "z", "n": 99}]


@pytest.mark.asyncio
async def test_update_computed_from_the_existing_value() -> None:
    """The new value is derived per row, so it has to come from the scan rather
    than from a constant folded into the plan."""
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table("t", pa.table({"id": [1, 2, 3], "n": [10, 20, 30]}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("UPDATE workspace.public.t SET n = n + 1 WHERE id >= 2"))

    rows, _keys = store.updates[0]
    assert _rows(rows) == [{"id": 2, "n": 21}, {"id": 3, "n": 31}]


@pytest.mark.asyncio
async def test_delete_with_a_large_string_primary_key() -> None:
    """`pa.large_string()` keys carry 64-bit offsets. The key reader takes the width
    from the column's Arrow layout, so this must read the same as `pa.utf8()` rather
    than reinterpreting the offset buffer."""
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table(
        "t",
        pa.table({"code": pa.array(["aa", "bb", "cc"], type=pa.large_string()), "n": [1, 2, 3]}),
        pks=["code"],
    )
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("DELETE FROM workspace.public.t WHERE code = 'bb'"))

    assert _rows(store.deletes[0]) == [{"code": "bb"}]


@pytest.mark.parametrize(
    "column, refusal",
    [
        pytest.param(
            pa.array(["aa", "bb"], type=pa.utf8()).dictionary_encode(),
            "cannot be read as a key",
            id="dictionary",
        ),
        pytest.param(
            pa.array(["aa", "bb"], type=pa.string_view()),
            "could not decode the Arrow IPC",
            id="string_view",
        ),
    ],
)
@pytest.mark.asyncio
async def test_a_key_column_layout_the_key_reader_cannot_read_is_refused(column: pa.Array, refusal: str) -> None:
    """Both of these are still a VARCHAR to DuckDB while their buffers hold something
    else — dictionary indices, or views into a buffer list. Dictionary columns
    decode and scan fine, but the DML key reader reads raw Arrow buffers by layout,
    so as a PRIMARY KEY column the dictionary is told no rather than handed a silent
    misread; string_view is still refused earlier, by the IPC decoder.
    """
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table("t", pa.table({"code": column, "n": [1, 2]}), pks=["code"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    with pytest.raises(duckdb.Error, match=refusal):
        await asyncio.to_thread(lambda: conn.execute("DELETE FROM workspace.public.t WHERE code = 'bb'"))

    assert store.deletes == []


# ── Dictionary-encoded columns (pandas Categorical) ─────────────────────────
#
# A dictionary-encoded field decodes to its value type: the catalog reports
# VARCHAR, scans hand back the decoded strings, and the provider never has to
# know DuckDB has no dictionary layout of its own. Only key columns stay
# restricted (see the refusal test above).


@pytest.mark.parametrize(
    "column",
    [
        pytest.param(pa.array(["x", "y", "x"], type=pa.utf8()).dictionary_encode(), id="int32_indices"),
        pytest.param(
            pa.DictionaryArray.from_arrays(pa.array([0, 1, 0], type=pa.int8()), pa.array(["x", "y"])),
            id="int8_indices",
        ),
    ],
)
@pytest.mark.asyncio
async def test_dictionary_encoded_columns_describe_and_scan_as_their_value_type(column: pa.Array) -> None:
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("t", pa.table({"id": [1, 2, 3], "cat": column}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    described = await asyncio.to_thread(lambda: conn.execute("DESCRIBE workspace.public.t").fetchall())
    assert [(row[0], row[1]) for row in described] == [("id", "BIGINT"), ("cat", "VARCHAR")]

    rows = await asyncio.to_thread(
        lambda: conn.execute("SELECT id, cat FROM workspace.public.t ORDER BY id").fetchall()
    )
    assert rows == [(1, "x"), (2, "y"), (3, "x")]


@pytest.mark.asyncio
async def test_a_pandas_categorical_column_round_trips() -> None:
    """The user-facing shape of the dictionary case: `pd.Categorical` serializes as
    dictionary<values=string, indices=int8>."""
    pd = pytest.importorskip("pandas")

    conn = _new_n6k_conn()
    store = _FakeStore()
    table = pa.Table.from_pandas(
        pd.DataFrame({"id": [1, 2, 3], "cat": pd.Categorical(["x", "y", "x"])}), preserve_index=False
    )
    store.set_table("t", table, pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.execute("SELECT id, cat FROM workspace.public.t ORDER BY id").fetchall()
    )
    assert rows == [(1, "x"), (2, "y"), (3, "x")]


@pytest.mark.asyncio
async def test_nested_dictionary_columns_scan_as_their_value_type() -> None:
    dict_child = pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int8()), pa.array(["a", "b"]))
    struct_col = pa.StructArray.from_arrays([dict_child], names=["tag"])
    list_col = pa.ListArray.from_arrays(pa.array([0, 1, 2]), dict_child)

    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("t", pa.table({"id": [1, 2], "s": struct_col, "l": list_col}), pks=["id"])
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    rows = await asyncio.to_thread(
        lambda: conn.execute("SELECT s.tag, l FROM workspace.public.t ORDER BY id").fetchall()
    )
    assert rows == [("a", ["a"]), ("b", ["b"])]


@pytest.mark.asyncio
async def test_update_on_a_table_with_a_dictionary_data_column() -> None:
    """Key columns must stay plain, but a dictionary-encoded NON-key column must not
    get in the way of DML: the scan appends the (plain) key, the key reader reads it,
    and the rewritten rows travel back to the provider as plain values."""
    conn = _new_n6k_conn()
    store = _RecordingStore()
    store.set_table(
        "t",
        pa.table({"id": [1, 2, 3], "cat": pa.array(["x", "y", "x"]).dictionary_encode(), "n": [10, 20, 30]}),
        pks=["id"],
    )
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.execute("UPDATE workspace.public.t SET n = n + 1 WHERE cat = 'x'"))

    rows, _keys = store.updates[0]
    assert _rows(rows) == [{"id": 1, "n": 11}, {"id": 3, "n": 31}]


# ── Typed filter literals reach the provider already rebuilt ────────────────
#
# JSON has no date, decimal or 128-bit integer, so those cross as text with a tag
# naming what they were. The provider must receive the real scalar: pyarrow will
# not compare a date32 column to a string, so a provider handed "2025-05-06" here
# would silently return nothing.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "column,where,expected",
    [
        pytest.param(
            pa.array([date(2025, 5, 6), date(2025, 5, 26)], pa.date32()),
            "d >= DATE '2025-05-26'",
            date(2025, 5, 26),
            id="date",
        ),
        pytest.param(
            pa.array([datetime(2025, 5, 6), datetime(2025, 5, 26)], pa.timestamp("us")),
            "d = TIMESTAMP '2025-05-26 00:00:00'",
            datetime(2025, 5, 26),
            id="timestamp",
        ),
        pytest.param(
            pa.array([Decimal("1.50"), Decimal("2.50")], pa.decimal128(10, 2)),
            "d > CAST('1.50' AS DECIMAL(10,2))",
            Decimal("1.50"),
            id="decimal",
        ),
    ],
)
async def test_a_typed_filter_literal_arrives_as_a_scalar(column: pa.Array, where: str, expected: Any) -> None:
    conn = _new_n6k_conn()
    store = _CapturingStore(pa.table({"d": column, "n": [1, 2]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    await asyncio.to_thread(lambda: conn.sql(f"SELECT n FROM workspace.public.t WHERE {where}").fetchall())

    assert store.captured, "provider.scan was never called"
    clause = (store.captured[-1] or [])[0]
    assert clause[2] == expected
    assert not isinstance(clause[2], str), f"literal reached the provider un-rebuilt: {clause!r}"


@pytest.mark.asyncio
async def test_a_typed_filter_costs_no_extra_schema_call() -> None:
    """The tag travels with the value, so rebuilding it needs nothing from the
    provider. Before that, every filtered scan asked for the schema again."""
    conn = _new_n6k_conn()
    store = _FakeStore()
    store.set_table("t", pa.table({"d": pa.array([date(2025, 5, 6)], pa.date32()), "n": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)

    # Warm the catalog entry first; that lookup is what legitimately calls schema().
    await asyncio.to_thread(lambda: conn.sql("SELECT n FROM workspace.public.t").fetchall())
    before = store.schema_calls

    await asyncio.to_thread(
        lambda: conn.sql("SELECT n FROM workspace.public.t WHERE d >= DATE '2025-05-06'").fetchall()
    )
    assert store.schema_calls == before, "a filtered scan asked the provider for its schema again"


def _registered_store(conn: duckdb.DuckDBPyConnection) -> _FakeStore:
    store = _FakeStore()
    store.set_table("a", pa.table({"x": [1]}))
    register_provider(conn, catalog="workspace", schema_name="public", provider=store)
    return store


def _error_of(conn: duckdb.DuckDBPyConnection, sql: str) -> str:
    try:
        conn.sql(sql).fetchall()
    except duckdb.Error as exc:
        return str(exc)
    raise AssertionError(f"expected {sql!r} to fail")


async def _register_query_release() -> tuple["weakref.ref[_FakeStore]", "weakref.ref[duckdb.DuckDBPyConnection]"]:
    conn = _new_n6k_conn()
    store = _registered_store(conn)
    await asyncio.to_thread(lambda: conn.sql("SELECT * FROM workspace.public.a").fetchall())
    probe_id = _catalogs[conn]["workspace"].probe_id

    await asyncio.to_thread(release_providers, conn)

    assert conn not in _catalogs
    assert store._binding is None
    assert await asyncio.to_thread(_error_of, conn, f"SELECT __n6k_provider_list_{probe_id}()")
    assert "no provider registered" in await asyncio.to_thread(
        _error_of, conn, "SELECT provider_invalidate_tables('workspace')"
    )
    conn.close()
    return weakref.ref(store), weakref.ref(conn)


@pytest.mark.asyncio
async def test_release_providers_frees_the_connection_and_its_providers() -> None:
    for _ in range(3):
        store_ref, conn_ref = await _register_query_release()
        gc.collect()
        assert (store_ref(), conn_ref()) == (None, None)


@pytest.mark.asyncio
async def test_release_providers_on_an_unregistered_connection_is_a_no_op() -> None:
    conn = _new_n6k_conn()
    release_providers(conn)
    assert conn not in _catalogs


@pytest.mark.asyncio
async def test_a_dropped_connection_frees_its_providers_without_release() -> None:
    conn = _new_n6k_conn()
    store = _registered_store(conn)
    alive = weakref.ref(store), weakref.ref(conn)
    del conn, store
    gc.collect()
    assert [ref() for ref in alive] == [None, None]


@pytest.mark.asyncio
async def test_a_provider_on_a_dropped_connection_refuses_invalidate() -> None:
    conn = _new_n6k_conn()
    store = _registered_store(conn)
    del conn
    gc.collect()
    with pytest.raises(RuntimeError, match="no longer exists"):
        await store.invalidate()


@pytest.mark.asyncio
async def test_same_catalog_name_on_two_connections_stays_separate() -> None:
    a_conn, b_conn = _new_n6k_conn(), _new_n6k_conn()
    a, b = _registered_store(a_conn), _registered_store(b_conn)

    def tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
        return sorted(
            r[0]
            for r in conn.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog='workspace' AND table_schema='public'"
            ).fetchall()
        )

    assert await asyncio.to_thread(tables, a_conn) == ["a"]
    a.set_table("extra", pa.table({"y": [2]}))
    await b.invalidate()
    assert await asyncio.to_thread(tables, a_conn) == ["a"]
    await a.invalidate()
    assert await asyncio.to_thread(tables, a_conn) == ["a", "extra"]

    await b.unregister()
    assert await asyncio.to_thread(lambda: a_conn.sql("SELECT * FROM workspace.public.a").fetchall()) == [(1,)]
    await a.invalidate()


@pytest.mark.asyncio
async def test_unregister_keeps_the_provider_bound_when_sync_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _new_n6k_conn()
    store = _registered_store(conn)
    other = _FakeStore()
    register_provider(conn, catalog="workspace", schema_name="other", provider=other)
    entry = _catalogs[conn]["workspace"]

    def broken_sync() -> None:
        raise RuntimeError("sync failed")

    monkeypatch.setattr(entry, "sync", broken_sync)
    with pytest.raises(RuntimeError, match="sync failed"):
        await store.unregister()
    assert entry.providers["public"] is store
    assert store._binding is not None
