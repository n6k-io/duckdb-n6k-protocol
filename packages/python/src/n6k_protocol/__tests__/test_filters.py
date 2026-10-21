"""Unit tests for ``n6k_protocol.filters`` — no DuckDB required, just
in-memory Arrow tables and pure Python.
"""

from datetime import date, datetime, time
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from n6k_protocol.filters import (
    FilterClause,
    filters_from_wire,
    filter_and_project,
    split_pyarrow_filters,
)

# ── split_pyarrow_filters ──────────────────────────────────────────────────


def test_split_empty_and_none() -> None:
    assert split_pyarrow_filters(None) == ([], [])
    assert split_pyarrow_filters([]) == ([], [])


def test_split_all_pyarrow_compatible() -> None:
    filters: list[FilterClause] = [
        ("a", "=", 1),
        ("b", ">=", 2),
        ("c", "in", [1, 2, 3]),
    ]
    arrow, leftover = split_pyarrow_filters(filters)
    assert arrow == filters
    assert leftover == []


def test_split_all_leftover() -> None:
    filters: list[FilterClause] = [
        ("x", "is_null", None),
        ("y", "is_not_null", None),
    ]
    arrow, leftover = split_pyarrow_filters(filters)
    assert arrow == []
    assert leftover == filters


def test_split_mixed_preserves_order() -> None:
    filters: list[FilterClause] = [
        ("a", "=", 1),
        ("b", "is_null", None),
        ("c", ">", 10),
        ("d", "is_not_null", None),
    ]
    arrow, leftover = split_pyarrow_filters(filters)
    assert arrow == [("a", "=", 1), ("c", ">", 10)]
    assert leftover == [("b", "is_null", None), ("d", "is_not_null", None)]


# ── filter_and_project: projection ──────────────────────────────────────────────


def _sample() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "name": pa.array(["a", "b", "c", "d"], type=pa.string()),
            "score": pa.array([10.0, 20.0, 30.0, 40.0], type=pa.float64()),
            "active": pa.array([True, False, True, None]),
        }
    )


def test_filter_and_project_projection_none_keeps_all() -> None:
    table = _sample()
    result = filter_and_project(table, None, None)
    assert result.column_names == ["id", "name", "score", "active"]
    assert result.num_rows == 4


def test_filter_and_project_projection_subset() -> None:
    result = filter_and_project(_sample(), ["id", "score"], None)
    assert result.column_names == ["id", "score"]
    assert result.num_rows == 4


def test_filter_and_project_projection_reorder() -> None:
    result = filter_and_project(_sample(), ["name", "id"], None)
    assert result.column_names == ["name", "id"]


# ── filter_and_project: operators ───────────────────────────────────────────────


def test_filter_equal() -> None:
    result = filter_and_project(_sample(), None, [("name", "=", "b")])
    assert result.to_pydict() == {"id": [2], "name": ["b"], "score": [20.0], "active": [False]}


def test_filter_not_equal() -> None:
    result = filter_and_project(_sample(), ["id"], [("name", "!=", "b")])
    assert result.to_pydict() == {"id": [1, 3, 4]}


def test_filter_less_than() -> None:
    result = filter_and_project(_sample(), ["id"], [("score", "<", 25.0)])
    assert result.to_pydict() == {"id": [1, 2]}


def test_filter_less_equal() -> None:
    result = filter_and_project(_sample(), ["id"], [("score", "<=", 20.0)])
    assert result.to_pydict() == {"id": [1, 2]}


def test_filter_greater_than() -> None:
    result = filter_and_project(_sample(), ["id"], [("score", ">", 20.0)])
    assert result.to_pydict() == {"id": [3, 4]}


def test_filter_greater_equal() -> None:
    result = filter_and_project(_sample(), ["id"], [("score", ">=", 20.0)])
    assert result.to_pydict() == {"id": [2, 3, 4]}


def test_filter_in() -> None:
    result = filter_and_project(_sample(), ["id"], [("name", "in", ["a", "c"])])
    assert result.to_pydict() == {"id": [1, 3]}


def test_filter_is_null() -> None:
    # `active` has a null at row 4 (id=4)
    result = filter_and_project(_sample(), ["id"], [("active", "is_null", None)])
    assert result.to_pydict() == {"id": [4]}


def test_filter_is_not_null() -> None:
    result = filter_and_project(_sample(), ["id"], [("active", "is_not_null", None)])
    assert result.to_pydict() == {"id": [1, 2, 3]}


# ── filter_and_project: multi-clause AND ────────────────────────────────────────


def test_filter_multi_clause_on_same_column() -> None:
    result = filter_and_project(
        _sample(),
        ["id"],
        [("id", ">", 1), ("id", "<", 4)],
    )
    assert result.to_pydict() == {"id": [2, 3]}


def test_filter_multi_clause_mixed_operators() -> None:
    result = filter_and_project(
        _sample(),
        ["id", "name"],
        [
            ("score", ">=", 20.0),
            ("name", "!=", "b"),
            ("active", "is_not_null", None),
        ],
    )
    assert result.to_pydict() == {"id": [3], "name": ["c"]}


def test_filter_and_project_together() -> None:
    result = filter_and_project(_sample(), ["name"], [("id", ">=", 3)])
    assert result.column_names == ["name"]
    assert result.to_pydict() == {"name": ["c", "d"]}


def test_filter_empty_and_none_filters_return_input() -> None:
    table = _sample()
    assert filter_and_project(table, None, None).equals(table)
    assert filter_and_project(table, None, []).equals(table)


# ── filters_from_wire ──────────────────────────────────────────────────────
# JSON has no date/decimal/128-bit-int, so those literals travel as the
# engine's canonical text form with a tag naming what they were. SQL consumers
# cast implicitly; pyarrow does not, and refuses to compare a date32 column to
# a string. These pin the rebuilding that closes that gap — and the arity rule
# that keeps untagged clauses free.


def test_untagged_clauses_pass_straight_through() -> None:
    """Three elements means the JSON value is already the right type."""
    assert filters_from_wire([["name", "=", "a"], ["n", ">", 2], ["ok", "=", True]]) == [
        ("name", "=", "a"),
        ("n", ">", 2),
        ("ok", "=", True),
    ]


def test_no_clauses_is_none() -> None:
    assert filters_from_wire([]) is None
    assert filters_from_wire(None) is None


def test_rebuilds_temporal_and_decimal_from_the_tag() -> None:
    got = filters_from_wire(
        [
            ["day", ">=", "2026-05-01", "date"],
            ["at", "<", "17:30:00", "time"],
            ["ts", "=", "2026-05-21 00:00:00", "timestamp"],
            ["amt", ">", "1.50", "decimal"],
        ]
    )
    assert [c[2] for c in got or []] == [
        date(2026, 5, 1),
        time(17, 30),
        datetime(2026, 5, 21),
        Decimal("1.50"),
    ]


def test_rebuilds_wide_integer_from_text() -> None:
    """HUGEINT exceeds JSON's safe integer range, so it arrives as digits."""
    got = filters_from_wire([["big", ">", "170141183460469231731687303715884105", "int"]]) or []
    assert got[0][2] == 170141183460469231731687303715884105


def test_a_null_value_is_never_rebuilt() -> None:
    assert filters_from_wire([["day", "is_null", None]]) == [("day", "is_null", None)]
    assert filters_from_wire([["day", "=", None, "date"]]) == [("day", "=", None)]


def test_rebuilds_an_in_list_elementwise() -> None:
    got = filters_from_wire([["day", "in", ["2026-05-01", "2026-05-21"], "date"]])
    assert got == [("day", "in", [date(2026, 5, 1), date(2026, 5, 21)])]


def test_an_unknown_tag_leaves_the_value_alone() -> None:
    """Forward compatibility: a tag this reader does not know must not be fatal,
    because the value is still whatever JSON carried."""
    assert filters_from_wire([["x", "=", "raw", "something_new"]]) == [("x", "=", "raw")]


def test_rejects_an_opaque_value() -> None:
    """A BLOB's text form is indistinguishable from a VARCHAR value, so the
    original bytes cannot be recovered — the sender says so rather than guessing."""
    with pytest.raises(ValueError, match="no wire form that can be rebuilt"):
        filters_from_wire([["raw", "=", "ab", "opaque"]])


def test_rejects_a_malformed_literal() -> None:
    with pytest.raises(ValueError, match="not a valid date literal"):
        filters_from_wire([["day", "=", "not-a-date", "date"]])


def _typed() -> pa.Table:
    return pa.table(
        {
            "day": pa.array([date(2026, 5, 1), date(2026, 5, 21)], pa.date32()),
            "name": pa.array(["a", "b"], pa.utf8()),
        }
    )


def test_rebuilt_filters_work_in_filter_and_project() -> None:
    table = _typed()
    got = filter_and_project(table, ["name"], filters_from_wire([["day", ">", "2026-05-01", "date"]]))
    assert got.to_pydict() == {"name": ["b"]}


def test_rebuilt_filters_work_for_pyarrow_dataset(tmp_path) -> None:
    """The parquet path is the one `split_pyarrow_filters` feeds, and it does
    not rebuild either — so it needs the same treatment as filter_and_project."""
    table = _typed()
    path = tmp_path / "t.parquet"
    pq.write_table(table, path)
    arrow, leftover = split_pyarrow_filters(filters_from_wire([["day", ">=", "2026-05-21", "date"]]))
    assert leftover == []
    assert pq.read_table(path, filters=arrow).num_rows == 1
