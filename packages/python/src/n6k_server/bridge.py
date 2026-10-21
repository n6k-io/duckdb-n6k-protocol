"""Bridge setup between DuckDB connections with policy-based access control."""

import logging
import threading
import uuid
from typing import NamedTuple, Optional, Sequence

import duckdb

from n6k_server.extension import load_virtual_catalog_bridge

log = logging.getLogger(__name__)


class Policy(NamedTuple):
    """One `bridge_set_policy(bridge_id, table, setting, value)` call."""

    table: str
    setting: str
    value: object


def bridge(
    source: duckdb.DuckDBPyConnection,
    target: duckdb.DuckDBPyConnection,
    name: str,
    *,
    source_catalog: str,
    policies: Sequence[Policy],
    context: Optional[dict[str, object]] = None,
    lock: Optional[threading.Lock] = None,
) -> str:
    """Mirror tables from `source` into a new catalog `name` on `target`.

    One bridge is one attached catalog: `name` must not exist on `target` yet, and
    `DETACH name` (see `unbridge`) tears it down. Each granted source schema lands
    in the target schema of the same name.

    ## Args

    - `source`, `target` — DuckDB connections.
    - `name` — catalog created on the target.
    - `source_catalog` — where the source tables live.
    - `policies` — `Policy` settings, one per `(table, setting)`. `setting` is a verb
      (`select`, `insert`, `update`, `delete`, `alter`, `drop`) or `primary_key`.
      A verb's value is `True` or a predicate; `update` also takes
      `{"using": ..., "check": ...}`. `primary_key` takes a column list and
      overrides PK auto-discovery; without a key, `UPDATE` and `DELETE` are rejected.
    - `context` — `{name: value}` that policies reference as `$name`. Values are
      frozen once `bridge` returns.
    - `lock` — held while `bridge` registers on `source`.

    Returns the bridge id. Raises `duckdb.Error` for an empty or invalid policy.

    ## Example

    ```python
    import duckdb
    from n6k_server.bridge import Policy, bridge

    cfg = {"allow_unsigned_extensions": "true"}
    source = duckdb.connect(config=cfg)
    source.sql("CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR, tenant_id INTEGER)")

    target = duckdb.connect(config=cfg)
    bridge(source, target, "app", source_catalog="memory", policies=[
        Policy("main.users", "select", True),
        Policy("main.users", "insert", True),
        Policy("main.users", "update", True),
        Policy("main.users", "delete", True),
    ])

    target.sql("SELECT * FROM app.main.users")
    ```

    Restricted to one tenant:

    ```python
    bridge(source, target, "app", source_catalog="memory", policies=[
        Policy("main.users", "select", "tenant_id = 42"),
        Policy("main.users", "insert", "tenant_id = 42"),
        Policy("main.users", "update", "tenant_id = 42"),
        Policy("main.users", "delete", "tenant_id = 42"),
    ])
    ```

    Per-user rows via a context value:

    ```python
    bridge(source, target, "app", source_catalog="memory", context={"user_id": "abc"}, policies=[
        Policy("main.docs", "select", "owner_id = $user_id"),
        Policy("main.docs", "insert", "owner_id = $user_id"),
    ])
    ```
    """
    if not policies:
        raise duckdb.InvalidInputException("bridge: policies must be provided")

    bridge_id = uuid.uuid4().hex

    load_virtual_catalog_bridge(source)
    load_virtual_catalog_bridge(target)

    if lock is not None:
        lock.acquire()
    try:
        token_result = source.sql(
            "SELECT bridge_register_source($1, $2)",
            params=[bridge_id, source_catalog],
        ).fetchone()
        if token_result is None:
            raise RuntimeError("bridge_register_source returned no result")
        token = token_result[0]
        for key, value in (context or {}).items():
            source.sql(
                "SELECT bridge_set_context($1, $2, $3)",
                params=[bridge_id, key, value],
            ).fetchone()
        if context:
            source.sql("SELECT bridge_finalize_context($1)", params=[bridge_id]).fetchone()
        for policy in policies:
            source.sql(
                "SELECT bridge_set_policy($1, $2, $3, $4)",
                params=[bridge_id, policy.table, policy.setting, policy.value],
            ).fetchone()
    finally:
        if lock is not None:
            lock.release()

    # ATTACH options fold to constants at bind time, so neither can be a parameter.
    quoted = name.replace('"', '""')
    target.execute(
        f"ATTACH '' AS \"{quoted}\" (TYPE virtual_catalog_bridge, ID '{bridge_id}', TOKEN '{token}')",
    )
    log.debug("bridge(%s): attached as %s", name, bridge_id)
    return bridge_id


def unbridge(target: duckdb.DuckDBPyConnection, name: str) -> None:
    quoted = name.replace('"', '""')
    target.execute(f'DETACH "{quoted}"')
