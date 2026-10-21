import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, snap } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

const sumTypeSql = (from: string) =>
  `SELECT sum(id) AS s, typeof(sum(id)) AS t FROM ${from}`;

const allAggsSql = (from: string) =>
  `SELECT count(*) AS c0, count(name) AS c1, sum(id) AS s, min(name) AS mn, ` +
  `max(age) AS mx, avg(age) AS av FROM ${from}`;

const orFilterSql = (from: string) =>
  `SELECT count(*) AS c FROM ${from} WHERE age > 30 OR age < 26`;

const havingSql = (from: string) =>
  `SELECT age, count(*) AS c FROM ${from} GROUP BY age HAVING count(*) > 0 ORDER BY age`;

backendDescribe()(`aggregate pushdown [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
    await handle.conn.query(`ATTACH '${N6K_URL}' AS slowdb (TYPE n6k)`);
    await handle.conn.query(
      "CREATE OR REPLACE TEMP TABLE local_users AS SELECT * FROM db.main.users",
    );
  }, 30_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("server capabilities are advertised", async () => {
    const r = await snap(
      handle.conn,
      "SELECT * FROM n6k_capabilities('db') ORDER BY 1",
    );
    expect(r.rows).toEqual([
      { capability: "catalog_plans", level: "open" },
      { capability: "crossing", level: "connect" },
    ]);
  });

  test("grouped aggregate matches the local baseline", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT age, count(*) AS c, sum(id) AS s FROM db.main.users GROUP BY age ORDER BY age",
    );
    const local = await snap(
      handle.conn,
      "SELECT age, count(*) AS c, sum(id) AS s FROM local_users GROUP BY age ORDER BY age",
    );
    expect(pushed).toEqual(local);
  });

  test("sum(INTEGER) reconciles back to HUGEINT", async () => {
    const pushed = await snap(handle.conn, sumTypeSql("db.main.users"));
    expect((pushed.rows[0] as Record<string, unknown>).t).toBe("HUGEINT");
    expect(pushed).toEqual(await snap(handle.conn, sumTypeSql("local_users")));
  });

  test("common aggregates match the local baseline", async () => {
    const pushed = await snap(handle.conn, allAggsSql("db.main.users"));
    const local = await snap(handle.conn, allAggsSql("local_users"));
    expect(pushed).toEqual(local);
  });

  test("filtered aggregate matches the local baseline", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT age, sum(id) AS s FROM db.main.users WHERE age > 26 GROUP BY age ORDER BY age",
    );
    const local = await snap(
      handle.conn,
      "SELECT age, sum(id) AS s FROM local_users WHERE age > 26 GROUP BY age ORDER BY age",
    );
    expect(pushed).toEqual(local);
  });

  test("ungrouped aggregate over no rows is exactly one row", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT count(*) AS c, min(name) AS mn FROM db.main.users WHERE id < 0",
    );
    expect(pushed.rows).toEqual([{ c: "bigint:0", mn: null }]);
  });

  test("grouped aggregate over no rows is empty", async () => {
    const pushed = await snap(
      handle.conn,
      "SELECT age, count(*) AS c FROM db.main.users WHERE id < 0 GROUP BY age",
    );
    expect(pushed.rows).toEqual([]);
  });

  test("provider-backed table stays correct", async () => {
    const got = await snap(
      handle.conn,
      "SELECT count(*) AS c, min(value) AS mn FROM slowdb.main.slow",
    );
    expect(got.rows).toEqual([{ c: "bigint:3", mn: "a" }]);
  });

  const shapes: Array<[string, string]> = [
    [
      "expression group key",
      "SELECT lower(name) AS g, count(*) AS c FROM $T GROUP BY lower(name) ORDER BY 1",
    ],
    ["DISTINCT aggregate", "SELECT count(DISTINCT age) AS c FROM $T"],
    ["median", "SELECT median(age) AS m FROM $T"],
    [
      "string_agg",
      "SELECT string_agg(name, ',') AS s FROM (SELECT name FROM $T ORDER BY name)",
    ],
    ["expression argument", "SELECT sum(id + age) AS s FROM $T"],
    [
      "aggregate FILTER clause",
      "SELECT count(*) FILTER (WHERE age > 26) AS c FROM $T",
    ],
    ["OR filter", orFilterSql("$T")],
    ["HAVING", havingSql("$T")],
  ];

  for (const [label, template] of shapes) {
    test(`matches the local baseline: ${label}`, async () => {
      const remote = template.replace("$T", "db.main.users");
      const local = template.replace("$T", "local_users");
      expect(await snap(handle.conn, remote)).toEqual(
        await snap(handle.conn, local),
      );
    });
  }
});
