"""SQL over an attach behaves as it does on the server's own DuckDB.

Each test starts its own `n6k_serve_http`, so clients share one catalog.

    uv run pytest integration_tests/test_attached_sql_parity.py -v
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Iterator

import duckdb
import pytest

from _paths import EXT_PATH, REPO_ROOT

DUCKDB_BIN = os.path.join(REPO_ROOT, "build", "release", "duckdb")

SLOW_COUNT = "SELECT count(*) c FROM range(4000000000) a WHERE hash(a.range) % 7 = 3"
ENDLESS_COUNT = "SELECT count(*) c FROM range(100000000000000) a WHERE hash(a.range) % 7 = 3"


class Server:
    def __init__(self, seed: str) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        sql = f"ATTACH ':memory:' AS db; USE db; {seed} CALL n6k_serve_http('127.0.0.1', {self.port}, 'db');"
        self.proc = subprocess.Popen(
            [DUCKDB_BIN, "-no-agent", "-unsigned", "-c", sql],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.time() + 30
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.5).close()
                return
            except OSError:
                if self.proc.poll() is not None or time.time() > deadline:
                    raise RuntimeError(f"n6k_serve_http did not start for seed: {seed}")
                time.sleep(0.05)

    def client(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        con.load_extension("httpfs")
        con.execute(f"LOAD '{EXT_PATH}'")
        con.execute(f"ATTACH 'n6k://127.0.0.1:{self.port}' AS db (TYPE n6k, anonymous true)")
        return con

    def cpu_seconds(self) -> float:
        out = subprocess.check_output(["ps", "-o", "time=", "-p", str(self.proc.pid)], text=True).strip()
        total = 0.0
        for part in out.replace("-", ":").split(":"):
            total = total * 60 + float(part)
        return total

    def wait_idle(self, timeout: float) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            before = self.cpu_seconds()
            time.sleep(1)
            if self.cpu_seconds() - before < 0.2:
                return True
        return False

    def stop(self) -> None:
        self.proc.kill()
        self.proc.wait()


@pytest.fixture
def serve() -> Iterator[Callable[[str], Server]]:
    if not os.path.exists(DUCKDB_BIN) or not os.path.exists(EXT_PATH):
        pytest.skip("not built (run `make release`)")
    servers: list[Server] = []

    def start(seed: str = "") -> Server:
        server = Server(seed)
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.stop()


def interrupted(con: duckdb.DuckDBPyConnection, sql: str, delay: float = 2) -> None:
    errors: list[Exception] = []

    def run() -> None:
        try:
            con.execute(sql).fetchall()
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(delay)
    con.interrupt()
    thread.join(60)
    assert not thread.is_alive()
    assert errors and isinstance(errors[0], duckdb.InterruptException)


@pytest.mark.parametrize(
    "column_type, literal",
    [
        ("UHUGEINT", "340282366920938463463374607431768211455"),
        ("UHUGEINT", "100000000000000000000000000000000000000"),
        ("UHUGEINT", "5"),
        ("TIMETZ", "'12:00:00+05:30'"),
        ("BIT", "'10101'"),
        ("UHUGEINT[]", "[340282366920938463463374607431768211455::UHUGEINT]"),
        ("STRUCT(t TIMETZ, b BIT)", "{'t': '12:00:00+05:30'::TIMETZ, 'b': '101'::BIT}"),
    ],
)
def test_server_values_read_back_unchanged(serve, column_type: str, literal: str) -> None:
    server = serve(f"CREATE TABLE t(v {column_type}); INSERT INTO t VALUES (({literal})::{column_type});")
    expected = duckdb.connect().execute(f"SELECT ({literal})::{column_type} AS v")

    result = server.client().execute("SELECT v FROM db.main.t")

    assert str(result.description[0][1]) == str(expected.description[0][1])
    assert result.fetchall() == expected.fetchall()


def test_create_table_primary_key_rejects_duplicates(serve) -> None:
    con = serve().client()
    con.execute("CREATE TABLE db.main.k(id INTEGER PRIMARY KEY, v INTEGER)")
    con.execute("INSERT INTO db.main.k VALUES (1, 10)")

    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO db.main.k VALUES (1, 20)")

    assert con.execute("SELECT * FROM db.main.k").fetchall() == [(1, 10)]


def test_create_table_not_null_is_enforced(serve) -> None:
    con = serve().client()
    con.execute("CREATE TABLE db.main.k(id INTEGER, v INTEGER NOT NULL)")

    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO db.main.k VALUES (1, NULL)")


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("UPDATE db.main.k SET v = 99 WHERE id = 1", [(1, 99), (2, 20)]),
        ("DELETE FROM db.main.k WHERE id = 1", [(2, 20)]),
        (
            "MERGE INTO db.main.k t USING (SELECT 1 id, 5 v) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
            [(1, 5), (2, 20)],
        ),
    ],
)
def test_create_table_primary_key_allows_keyed_writes(serve, statement: str, expected: list[tuple[int, int]]) -> None:
    con = serve().client()
    con.execute("CREATE TABLE db.main.k(id INTEGER PRIMARY KEY, v INTEGER)")
    con.execute("INSERT INTO db.main.k VALUES (1, 10), (2, 20)")

    con.execute(statement)

    assert con.execute("SELECT * FROM db.main.k ORDER BY id").fetchall() == expected


def test_create_table_unique_is_enforced(serve) -> None:
    con = serve().client()
    con.execute("CREATE TABLE db.main.k(id INTEGER, u INTEGER UNIQUE)")
    con.execute("INSERT INTO db.main.k VALUES (1, 10)")

    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO db.main.k VALUES (2, 10)")


@pytest.mark.parametrize(
    "column",
    [
        "v INTEGER DEFAULT 7",
        "v INTEGER CHECK (v > 0)",
        "v INTEGER GENERATED ALWAYS AS (1)",
    ],
)
def test_create_table_with_what_the_wire_cannot_carry_is_rejected(serve, column: str) -> None:
    server = serve()

    with pytest.raises(duckdb.BinderException, match="not supported"):
        server.client().execute(f"CREATE TABLE db.main.k(id INTEGER, {column})")

    with pytest.raises(duckdb.CatalogException):
        server.client().execute("SELECT * FROM db.main.k")


def test_alter_add_column_with_default_is_rejected(serve) -> None:
    server = serve("CREATE TABLE a(id INTEGER); INSERT INTO a VALUES (1);")
    con = server.client()

    with pytest.raises(duckdb.BinderException, match="(?i)default"):
        con.execute("ALTER TABLE db.main.a ADD COLUMN d INTEGER DEFAULT 7")

    assert server.client().execute("SELECT * FROM db.main.a").fetchall() == [(1,)]


def test_alter_add_column_without_default_still_works(serve) -> None:
    server = serve("CREATE TABLE a(id INTEGER); INSERT INTO a VALUES (1);")

    server.client().execute("ALTER TABLE db.main.a ADD COLUMN d INTEGER")

    assert server.client().execute("SELECT * FROM db.main.a").fetchall() == [(1, None)]


def test_rollback_undoes_create_table(serve) -> None:
    server = serve()
    con = server.client()
    con.execute("BEGIN")
    con.execute("CREATE TABLE db.main.r(id INTEGER)")
    con.execute("ROLLBACK")

    with pytest.raises(duckdb.CatalogException):
        con.execute("SELECT * FROM db.main.r")
    with pytest.raises(duckdb.CatalogException):
        server.client().execute("SELECT * FROM db.main.r")


def test_rollback_undoes_alter_table(serve) -> None:
    server = serve("CREATE TABLE a(id INTEGER); INSERT INTO a VALUES (1);")
    con = server.client()
    con.execute("BEGIN")
    con.execute("ALTER TABLE db.main.a ADD COLUMN d INTEGER")
    assert con.execute("SELECT * FROM db.main.a").fetchall() == [(1, None)]
    assert server.client().execute("SELECT * FROM db.main.a").fetchall() == [(1,)]
    con.execute("ROLLBACK")

    assert con.execute("SELECT * FROM db.main.a").fetchall() == [(1,)]
    assert server.client().execute("SELECT * FROM db.main.a").fetchall() == [(1,)]


def test_commit_keeps_create_table_as(serve) -> None:
    server = serve()
    con = server.client()
    con.execute("BEGIN")
    con.execute("CREATE TABLE db.main.c AS SELECT 1 AS id")
    assert con.execute("SELECT * FROM db.main.c").fetchall() == [(1,)]
    con.execute("COMMIT")

    assert server.client().execute("SELECT * FROM db.main.c").fetchall() == [(1,)]


def test_rollback_undoes_create_table_as(serve) -> None:
    server = serve()
    con = server.client()
    con.execute("BEGIN")
    con.execute("CREATE TABLE db.main.c AS SELECT 1 AS id")
    con.execute("ROLLBACK")

    with pytest.raises(duckdb.CatalogException):
        server.client().execute("SELECT * FROM db.main.c")


def test_create_table_is_hidden_from_other_clients_until_commit(serve) -> None:
    server = serve()
    con = server.client()
    connected_before = server.client()
    con.execute("BEGIN")
    con.execute("CREATE TABLE db.main.v(id INTEGER)")

    with pytest.raises(duckdb.CatalogException):
        connected_before.execute("SELECT * FROM db.main.v")
    with pytest.raises(duckdb.CatalogException):
        server.client().execute("SELECT * FROM db.main.v")

    con.execute("COMMIT")
    assert server.client().execute("SELECT * FROM db.main.v").fetchall() == []


@pytest.mark.parametrize(
    "seed",
    [
        "CREATE TABLE t(id INTEGER);",
        "CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1);",
    ],
)
def test_create_index_is_refused_and_the_client_survives(serve, seed: str) -> None:
    con = serve(seed).client()

    with pytest.raises(duckdb.BinderException, match="not supported"):
        con.execute("CREATE INDEX idx ON db.main.t(id)")

    assert con.execute("SELECT 1").fetchall() == [(1,)]


def test_describe_shows_server_constraints(serve) -> None:
    con = serve("CREATE TABLE t(id INTEGER PRIMARY KEY, n VARCHAR NOT NULL, x INTEGER);").client()

    rows = con.execute("DESCRIBE db.main.t").fetchall()

    assert [(row[0], row[2], row[3]) for row in rows] == [
        ("id", "NO", "PRI"),
        ("n", "NO", None),
        ("x", "YES", None),
    ]


@pytest.mark.parametrize(
    "statement, expected",
    [
        ("INSERT INTO db.main.t VALUES (1, 'x') ON CONFLICT DO NOTHING", [(1, "a")]),
        ("INSERT INTO db.main.t VALUES (1, 'x') ON CONFLICT (id) DO NOTHING", [(1, "a")]),
        ("INSERT INTO db.main.t VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET n = excluded.n", [(1, "x")]),
        ("INSERT OR REPLACE INTO db.main.t VALUES (1, 'x')", [(1, "x")]),
        ("INSERT OR IGNORE INTO db.main.t VALUES (1, 'x')", [(1, "a")]),
    ],
)
def test_conflict_clause_on_a_server_primary_key(serve, statement: str, expected: list[tuple[int, str]]) -> None:
    con = serve("CREATE TABLE t(id INTEGER PRIMARY KEY, n VARCHAR); INSERT INTO t VALUES (1, 'a');").client()

    con.execute(statement)

    assert con.execute("SELECT * FROM db.main.t ORDER BY id").fetchall() == expected


@pytest.mark.parametrize(
    "alters",
    [
        ["ALTER TABLE db.main.t DROP COLUMN b"],
        ["ALTER TABLE db.main.t DROP COLUMN c"],
        ["ALTER TABLE db.main.t DROP COLUMN b", "ALTER TABLE db.main.t ADD COLUMN d INTEGER"],
        ["ALTER TABLE db.main.t DROP COLUMN b", "ALTER TABLE db.main.t ADD COLUMN b INTEGER"],
    ],
)
def test_stale_client_gets_an_error_after_another_client_changes_columns(serve, alters: list[str]) -> None:
    server = serve("CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER); INSERT INTO t VALUES (1, 2, 3);")
    stale = server.client()
    assert stale.execute("SELECT * FROM db.main.t").fetchall() == [(1, 2, 3)]
    other = server.client()
    for alter in alters:
        other.execute(alter)

    with pytest.raises(duckdb.CatalogException, match="changed on the source"):
        stale.execute("SELECT * FROM db.main.t").fetchall()


@pytest.mark.parametrize(
    "seed, statement",
    [
        (f"CREATE VIEW endless AS {ENDLESS_COUNT};", "SELECT * FROM db.main.endless"),
        ("", "SELECT * FROM n6k_catalog_query('db', '" + ENDLESS_COUNT.replace("'", "''") + "')"),
    ],
)
def test_cancel_stops_the_query_on_the_server(serve, seed: str, statement: str) -> None:
    server = serve(seed)

    interrupted(server.client(), statement)

    assert server.wait_idle(timeout=15)


@pytest.mark.parametrize(
    "seed, statement",
    [
        (f"CREATE VIEW slow AS {SLOW_COUNT};", "INSERT INTO db.main.out SELECT c FROM db.main.slow"),
        ("", "SELECT * FROM n6k_catalog_exec('db', 'INSERT INTO db.main.out " + SLOW_COUNT.replace("'", "''") + "')"),
    ],
)
def test_cancelled_write_does_not_commit(serve, seed: str, statement: str) -> None:
    server = serve(f"CREATE TABLE out(c BIGINT); {seed}")

    interrupted(server.client(), statement)

    assert server.wait_idle(timeout=120)
    assert server.client().execute("SELECT * FROM db.main.out").fetchall() == []


@pytest.mark.parametrize("ending", ["ROLLBACK", "COMMIT"])
def test_cancelled_write_in_a_transaction_leaves_no_row(serve, ending: str) -> None:
    server = serve(f"CREATE TABLE out(c BIGINT); CREATE VIEW slow AS {SLOW_COUNT};")
    con = server.client()
    con.execute("BEGIN")

    interrupted(con, "INSERT INTO db.main.out SELECT c FROM db.main.slow")
    with contextlib.suppress(duckdb.Error):
        con.execute(ending)

    assert server.client().execute("SELECT * FROM db.main.out").fetchall() == []
