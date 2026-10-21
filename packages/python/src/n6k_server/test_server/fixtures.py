"""Loader for `fixtures.sql`, the canonical seeded-data fixture.

Deliberately side-effect free and dependency free, so `seeding.py` and `app.py`
can both read the DDL without importing each other.
"""

import os
from functools import lru_cache

_SQL_PATH = os.path.join(os.path.dirname(__file__), "fixtures.sql")


@lru_cache(maxsize=1)
def _template_lines() -> tuple[str, ...]:
    with open(_SQL_PATH, encoding="utf-8") as f:
        raw = f.read()
    return tuple(
        line for line in (raw_line.strip() for raw_line in raw.splitlines()) if line and not line.startswith("--")
    )


def rpc_fixture_statements(catalog: str) -> list[str]:
    """The RPC targets the suite calls, as DuckDB functions in `catalog`.

    As macros they keep the property the regression tests actually check — that an
    argument arrives typed, so a DECIMAL comes back DECIMAL rather than as a quoted
    string. Each overload body needs its own parens, or the comma separating them is
    parsed as another SELECT column.

    `stream_counter` and `slow_ticker` wrap the extension's native test functions:
    neither an unbounded stream nor a paced one is expressible in SQL.
    """
    return [
        f"CREATE MACRO {catalog}.main.echo(a) AS TABLE (SELECT a AS arg0), "
        f"(a, b) AS TABLE (SELECT a AS arg0, b AS arg1)",
        # Numeric columns BEFORE a VARCHAR, and more than one row per label: that shape is
        # what reproduced the projection-pushdown segfault when a filter reordered the
        # columns, so the layout is the fixture's whole purpose.
        f"CREATE MACRO {catalog}.main.xy_labeled() AS TABLE "
        f"(SELECT * FROM (VALUES (1.0::DOUBLE, 10.0::DOUBLE, 'a'), (2.0::DOUBLE, 20.0::DOUBLE, 'b'), "
        f"(3.0::DOUBLE, 30.0::DOUBLE, 'a')) t(x, y, label))",
        # Subscript, not `s.x`: inside a table macro the dotted form binds as table `s`.
        f"CREATE MACRO {catalog}.main.struct_arg(s) AS TABLE (SELECT s['x'] AS x, s['y'] AS y)",
        f"CREATE MACRO {catalog}.main.doubler(n) AS TABLE (SELECT n * 2 AS doubled)",
        # A macro cannot declare a table parameter, so OP_RPC_TABLE hands it the staged
        # view's NAME and it opens it with query_table.
        f"CREATE MACRO {catalog}.main.sum_table(t) AS TABLE " f"(SELECT sum(id)::BIGINT AS total FROM query_table(t))",
        # Parameters are named `cnt`/`ms`, never `n`: the underlying table function's output column
        # is `n`, and a macro parameter of the same name shadows it — `SELECT n` then returns the
        # argument on every row instead of the counter's value.
        f"CREATE MACRO {catalog}.main.stream_counter(cnt) AS TABLE (SELECT * FROM n6k_testing_stream_counter(cnt))",
        # `seq`, not `n`: the incremental-delivery test reads that column by name.
        f"CREATE MACRO {catalog}.main.slow_ticker(cnt, ms) AS TABLE "
        f"(SELECT n AS seq FROM n6k_testing_stream_counter(cnt, ms))",
    ]


def fixture_statements(catalog: str) -> list[str]:
    """The fixture DDL/DML for `catalog`, one statement per element, no trailing `;`.

    The catalog must already be ATTACHed — the fixture seeds tables into it and
    never creates it, because the two callers attach it differently (`:memory:`
    versus an on-disk file).
    """
    return [line.format(catalog=catalog) for line in _template_lines()]
