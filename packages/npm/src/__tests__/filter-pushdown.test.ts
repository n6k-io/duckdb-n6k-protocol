import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, snap } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

// End-to-end filter pushdown: client serializer -> WS -> server WHERE builder -> DuckDB.
//
// Two distinct pushdown regimes, and the difference decides what these tests can prove:
//   - A filter DuckDB can express exactly (comparison / IN / IS NULL) is pushed NON-optionally and
//     the FILTER operator is dropped from the plan. Nothing re-applies it locally, so the wire
//     filter is authoritative and a mistake shows up as wrong rows.
//   - A disjunction is pushed as an `optional:` hint AND kept as a FILTER above the scan. Results
//     stay correct even if the hint is mangled or dropped; sending it faithfully only reduces how
//     many rows cross the wire.
// So the OR cases below pin semantics rather than catch a live wrong-answer bug -- see
// EXPLAIN SELECT ... WHERE age > 30 OR age < 26, which shows both the FILTER and the optional hint.
//
// db.main.users is seeded (seeding.py) as:
//   (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)

backendDescribe()(`filter pushdown [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  }, 30_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  // The serializer used to flatten a disjunction into the AND-joined clause list, which as a
  // predicate means `age > 30 AND age < 26` -- nothing. The local FILTER masked that, so the
  // observable symptom was a full table crossing the wire, not a wrong answer.
  test("OR on one column returns the union", async () => {
    const got = await snap(
      handle.conn,
      "SELECT id FROM db.main.users WHERE age > 30 OR age < 26 ORDER BY id",
    );
    expect(got.rows).toEqual([{ id: 2 }, { id: 3 }]);
  });

  // A multi-clause branch must keep its own grouping: spliced into the OR list it would read as
  // two independent branches and match Alice too.
  test("AND nested inside OR keeps its grouping", async () => {
    const got = await snap(
      handle.conn,
      "SELECT id FROM db.main.users WHERE (age > 26 AND age < 34) OR age = 35 ORDER BY id",
    );
    expect(got.rows).toEqual([{ id: 1 }, { id: 3 }]);
  });

  test("plain AND still narrows", async () => {
    const got = await snap(
      handle.conn,
      "SELECT id FROM db.main.users WHERE age >= 30 AND name != 'Alice' ORDER BY id",
    );
    expect(got.rows).toEqual([{ id: 3 }]);
  });

  test("IN and NOT IN", async () => {
    const inRows = await snap(
      handle.conn,
      "SELECT id FROM db.main.users WHERE name IN ('Alice', 'Charlie') ORDER BY id",
    );
    expect(inRows.rows).toEqual([{ id: 1 }, { id: 3 }]);

    const notIn = await snap(
      handle.conn,
      "SELECT id FROM db.main.users WHERE age NOT IN (25, 30) ORDER BY id",
    );
    expect(notIn.rows).toEqual([{ id: 3 }]);
  });

  test("IS NULL / IS NOT NULL", async () => {
    const notNull = await snap(
      handle.conn,
      "SELECT id FROM db.main.users WHERE name IS NOT NULL ORDER BY id",
    );
    expect(notNull.rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);

    const isNull = await snap(
      handle.conn,
      "SELECT id FROM db.main.users WHERE name IS NULL",
    );
    expect(isNull.rows).toEqual([]);
  });

  // A pushed filter must never widen the result. Comparing against the same predicate evaluated
  // over a locally-materialized copy pins that: the CTE forces the scan to happen first, so the
  // filter is applied by DuckDB rather than pushed.
  test("pushed OR matches locally-evaluated OR", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT id, name, age FROM db.main.users WHERE age > 30 OR age < 26 ORDER BY id",
    );
    const local = await snap(
      handle.conn,
      "WITH all_rows AS (SELECT * FROM db.main.users) " +
        "SELECT id, name, age FROM all_rows WHERE age > 30 OR age < 26 ORDER BY id",
    );
    expect(pushed).toEqual(local);
  });
});
