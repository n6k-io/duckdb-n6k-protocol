"""Filter-clause types and Arrow helpers for the `Provider` API.

Shape-compatible with pyarrow's tuple-form filter argument — a list of
`(col, op, value)` tuples, structurally identical to what
`pyarrow.parquet.read_table(filters=...)` and `pyarrow.dataset` accept.
Providers that read parquet can pass an n6k `filters` value straight to
pyarrow for the operators pyarrow understands; `is_null` and `is_not_null`
are n6k extras with no pyarrow tuple-form equivalent — use
`split_pyarrow_filters()` to partition them out, then apply the leftover
via `filter_and_project()`.
"""

import datetime
import decimal
from typing import Any, Literal, Optional

import pyarrow as pa
import pyarrow.compute as pc

FilterOp = Literal["=", "!=", "<", "<=", ">", ">=", "in", "is_null", "is_not_null"]
FilterClause = tuple[str, FilterOp, Any]
Filters = Optional[list[FilterClause]]

# The network `filters` payload is a superset of the Provider-facing `Filters`
# shape: alongside flat `(col, op, value)` triples it may carry nested
# `(kind, [clause, ...])` groups, where `kind` is "and" or "or". Providers never
# see those — the virtual_catalog serializer (sibling duckdb-virtual-catalog repo)
# emits the flat dialect only — so `Filters` stays pyarrow-tuple-compatible.
# The flat dialect additionally carries a 4th element on clauses whose value
# crossed as text (`filters_from_wire`); the network dialect never does.
# Operators whose tuple-form (``(col, op, value)``) is accepted directly by
# ``pyarrow.parquet.read_table`` / ``pyarrow.dataset``. ``is_null`` and
# ``is_not_null`` are excluded because pyarrow's tuple filter form has no
# equivalent — those require the ``Expression`` API.
_PYARROW_TUPLE_OPS: frozenset[str] = frozenset({"=", "!=", "<", "<=", ">", ">=", "in"})


# How to rebuild a literal that crossed as text, keyed by the tag the sender
# attached. Closed vocabulary, defined once by the virtual_catalog serializer
# (sibling duckdb-virtual-catalog repo) — the only place that still knows the
# engine type the value had.
_REBUILD: dict[str, Any] = {
    "date": datetime.date.fromisoformat,
    "time": datetime.time.fromisoformat,
    "timestamp": datetime.datetime.fromisoformat,
    "decimal": decimal.Decimal,
    "int": int,
}


def _rebuild_value(value: Any, tag: str, col: str) -> Any:
    """Turn one tagged wire literal back into the scalar it started as.

    JSON has no date, decimal or 128-bit integer, so the sender emits those as
    the engine's canonical string form and says which it was. SQL consumers never
    notice the difference — DuckDB casts `'2026-05-01'` to DATE implicitly — but
    pyarrow refuses to compare a `date32` column to a string, so the value has to
    be rebuilt before a provider ever sees it.
    """
    if value is None:
        return value
    # `opaque` means the sender could not encode the value in a form anything can
    # rebuild — BLOB above all, whose text form is indistinguishable from a
    # VARCHAR value. Saying so beats comparing the wrong thing.
    if tag == "opaque":
        raise ValueError(
            f"n6k: a filter on column {col!r} has a value whose type has no wire "
            f"form that can be rebuilt; the scan is refused rather than answered "
            f"against the wrong value"
        )
    rebuild = _REBUILD.get(tag)
    if rebuild is None:
        return value
    if not isinstance(value, str):
        return value
    try:
        return rebuild(value)
    except (ValueError, ArithmeticError) as exc:
        raise ValueError(
            f"n6k: filter on column {col!r} received {value!r}, which is not a " f"valid {tag} literal"
        ) from exc


def filters_from_wire(clauses: Any) -> Filters:
    """Decoded wire clauses to the `Filters` contract providers are handed.

    A clause is `[col, op, value]`, or `[col, op, value, tag]` when the value
    crossed as text and has to be rebuilt (`_rebuild_value`). Arity is the signal,
    so an untagged clause — every numeric, boolean and string comparison — costs
    nothing to pass through.

    This is where the untyped wire becomes the typed contract, so providers get
    properly typed scalars whether they hand the clauses to `filter_and_project`, to
    `pq.read_table(filters=...)`, or to their own backend.
    """
    if not clauses:
        return None
    out: list[FilterClause] = []
    for clause in clauses:
        col, op, val = clause[0], clause[1], clause[2]
        tag = clause[3] if len(clause) > 3 else None
        if tag is None:
            out.append((col, op, val))
        elif op == "in" and isinstance(val, (list, tuple)):
            out.append((col, op, [_rebuild_value(v, tag, col) for v in val]))
        else:
            out.append((col, op, _rebuild_value(val, tag, col)))
    return out


def split_pyarrow_filters(
    filters: Filters,
) -> tuple[list[FilterClause], list[FilterClause]]:
    """The first list is directly passable to `pq.read_table(filters=...)` or
    `pyarrow.dataset` — the operators `=`, `!=`, `<`, `<=`, `>`, `>=`, `in`
    all map to pyarrow's tuple form unchanged.

    The second list contains `is_null` / `is_not_null` clauses that
    pyarrow's tuple form cannot express. Apply them after the parquet read
    via `filter_and_project()` or `pyarrow.compute` directly.
    """
    if not filters:
        return [], []
    arrow: list[FilterClause] = []
    leftover: list[FilterClause] = []
    for clause in filters:
        _, op, _ = clause
        if op in _PYARROW_TUPLE_OPS:
            arrow.append(clause)
        else:
            leftover.append(clause)
    return arrow, leftover


def filter_and_project(
    table: pa.Table,
    columns: Optional[list[str]],
    filters: Filters,
) -> pa.Table:
    """Apply filters, then project, on an in-memory Arrow table.

    Filters run first so a clause can reference a column that's absent from
    `columns` (projected away on output). Every n6k operator is handled,
    including `is_null` and `is_not_null`. Projection honours `columns`
    (`None` keeps all columns).

    Use this when your provider can't push projection or filters down at
    all — load the full table and return `filter_and_project(tbl, columns, filters)`.
    """
    if filters:
        expr: Optional[pc.Expression] = None
        for col, op, val in filters:
            field = pc.field(col)
            clause: pc.Expression
            if op == "=":
                clause = field == val
            elif op == "!=":
                clause = field != val
            elif op == "<":
                clause = field < val
            elif op == "<=":
                clause = field <= val
            elif op == ">":
                clause = field > val
            elif op == ">=":
                clause = field >= val
            elif op == "in":
                clause = field.isin(val)
            elif op == "is_null":
                clause = field.is_null()
            elif op == "is_not_null":
                clause = field.is_valid()
            else:
                raise ValueError(f"filter_and_project: unsupported operator {op!r}")
            expr = clause if expr is None else expr & clause
        assert expr is not None
        table = table.filter(expr)

    if columns is not None and list(table.column_names) != columns:
        table = table.select(columns)

    return table
