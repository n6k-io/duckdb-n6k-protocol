import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { BROWSER_AVAILABLE } from "./conftest/browser-gate";
import { SKIP_SERVER_TESTS } from "./_server-gate";
import type { BrowserHarness } from "./conftest/browser-harness";

// Exercises the provider's `createWasmConnect` lease factory. It operates on the
// in-process wasm `db`, which lives inside the browser page — so the whole test
// body runs in-page via page.evaluate against window.__n6kTest, under the coi
// (browser-threads) bundle. Requires Chromium + a live server.
const RUNNABLE = BROWSER_AVAILABLE && !SKIP_SERVER_TESTS;

(RUNNABLE ? describe : describe.skip)(
  "createWasmConnect (the provider's connect factory) [browser coi]",
  () => {
    let h: BrowserHarness;
    let catalog: string;

    beforeAll(async () => {
      const { setupBrowserDuckdb } = await import("./conftest/browser-harness");
      h = await setupBrowserDuckdb({ bundle: "coi" });
      catalog = await h.page.evaluate(async () => {
        const t = globalThis.__n6kTest!;
        const cat = `cwc_${Date.now()}`;
        const c = await t.connect();
        try {
          await c.query(`ATTACH '${t.serverUrl}' AS ${cat} (TYPE n6k)`);
        } finally {
          await c.close();
        }
        return cat;
      });
    }, 60_000);

    afterAll(async () => {
      await h?.cleanup();
    });

    test("a lease runs a query over send() and returns the right rows", async () => {
      const rows = await h.page.evaluate(async () => {
        const t = globalThis.__n6kTest!;
        const c = await t.connect();
        try {
          const r = await c.query(
            "SELECT 1 AS a UNION ALL SELECT 2 ORDER BY a",
          );
          return r.toArray().map((row) => row.toJSON());
        } finally {
          await c.close();
        }
      });
      expect(rows).toEqual([{ a: 1 }, { a: 2 }]);
    });

    test("a lease still resolves the local SQL parser (the useQuery parse path)", async () => {
      const n = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const c = await t.connect();
        try {
          const r = await c.query(
            `SELECT * FROM n6k_parse_sql_get_tables('SELECT * FROM ${cat}.users')`,
          );
          return r.toArray().length;
        } finally {
          await c.close();
        }
      }, catalog);
      expect(n).toBeGreaterThan(0);
    });

    test("concurrent leases each get their full result — no chunk stealing", async () => {
      const counts = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const sql = `SELECT * FROM ${cat}.slow_ticker(6, 30)`;
        const conns = await Promise.all(
          Array.from({ length: 4 }, () => t.connect()),
        );
        try {
          return await Promise.all(
            conns.map(async (c) => {
              const r = await c.query(sql);
              return r.toArray().length;
            }),
          );
        } finally {
          await Promise.all(conns.map((c) => c.close().catch(() => {})));
        }
      }, catalog);
      expect(counts).toEqual([6, 6, 6, 6]);
    }, 30_000);

    test("close() releases the connection; a fresh lease still works after", async () => {
      const n = await h.page.evaluate(async () => {
        const t = globalThis.__n6kTest!;
        const a = await t.connect();
        await a.close();
        await a.close();
        const b = await t.connect();
        try {
          const r = await b.query("SELECT 42");
          return r.toArray().length;
        } finally {
          await b.close();
        }
      });
      expect(n).toBe(1);
    });
  },
);
