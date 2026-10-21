import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

// `queryViaSend` operates on the in-process wasm connection, which lives inside
// the browser page — so the whole body runs in-page under the coi bundle.
describeCoi("queryViaSend [browser coi]", () => {
  let h: BrowserHarness;

  beforeAll(async () => {
    h = await setupCoiPage();
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  test("returns the same columns and rows as query()", async () => {
    const res = await h.page.evaluate(async () => {
      const t = globalThis.__n6kTest!;
      const sql = "SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, 'y' ORDER BY a";
      const conn = await t.db.connect();
      type Raw = Parameters<typeof t.queryViaSend>[0];
      try {
        const viaQuery = await conn.query(sql);
        const viaSend = await t.queryViaSend(conn as unknown as Raw, sql);
        return {
          qFields: viaQuery.schema.fields.map((f) => f.name),
          sFields: viaSend.schema.fields.map((f) => f.name),
          qRows: viaQuery.toArray().map((r) => r.toJSON()),
          sRows: viaSend.toArray().map((r) => r.toJSON()),
        };
      } finally {
        await conn.close();
      }
    });
    expect(res.sFields).toEqual(res.qFields);
    expect(res.sRows).toEqual(res.qRows);
  });

  test("carries decimal scale through in the schema", async () => {
    const field = await h.page.evaluate(async () => {
      const t = globalThis.__n6kTest!;
      const conn = await t.db.connect();
      type Raw = Parameters<typeof t.queryViaSend>[0];
      try {
        const r = await t.queryViaSend(
          conn as unknown as Raw,
          "SELECT 1.25::DECIMAL(10,2) AS d",
        );
        const f = r.schema.fields[0]!;
        return { name: f.name, scale: (f.type as { scale?: number }).scale };
      } finally {
        await conn.close();
      }
    });
    expect(field.name).toBe("d");
    expect(field.scale).toBe(2);
  });

  test("handles every statement kind, including row-less DDL", async () => {
    const rows = await h.page.evaluate(async () => {
      const t = globalThis.__n6kTest!;
      const conn = await t.db.connect();
      type Raw = Parameters<typeof t.queryViaSend>[0];
      const raw = conn as unknown as Raw;
      try {
        await t.queryViaSend(raw, "SET search_path = 'main'");
        await t.queryViaSend(
          raw,
          "CREATE OR REPLACE TABLE lease_t (x INTEGER)",
        );
        await t.queryViaSend(raw, "INSERT INTO lease_t VALUES (7)");
        const r = await t.queryViaSend(raw, "SELECT x FROM lease_t");
        await t.queryViaSend(raw, "DROP TABLE lease_t");
        return r.toArray().map((row) => row.toJSON());
      } finally {
        await conn.close();
      }
    });
    expect(rows).toEqual([{ x: 7 }]);
  });

  test("a row-less statement reports an empty result, not a crash", async () => {
    const res = await h.page.evaluate(async () => {
      const t = globalThis.__n6kTest!;
      const conn = await t.db.connect();
      type Raw = Parameters<typeof t.queryViaSend>[0];
      try {
        const r = await t.queryViaSend(
          conn as unknown as Raw,
          "SELECT 1 WHERE false",
        );
        return {
          len: r.toArray().length,
          isArray: Array.isArray(r.schema.fields),
        };
      } finally {
        await conn.close();
      }
    });
    expect(res.len).toBe(0);
    expect(res.isArray).toBe(true);
  });

  test("throws, rather than returning undefined, on a detached worker", async () => {
    const msg = await h.page.evaluate(async () => {
      const t = globalThis.__n6kTest!;
      type Raw = Parameters<typeof t.queryViaSend>[0];
      const detached = {
        async send(): Promise<undefined> {
          return;
        },
        close: async () => {},
        useUnsafe: () => {
          throw new Error("unused");
        },
      } as unknown as Raw;
      try {
        await t.queryViaSend(detached, "SELECT 1");
        return null;
      } catch (error) {
        return error instanceof Error ? error.message : String(error);
      }
    });
    expect(msg).toMatch(/detached/);
  });
});
