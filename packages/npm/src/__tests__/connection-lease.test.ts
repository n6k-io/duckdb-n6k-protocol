import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage, attachCatalog } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

// Exercises the wasm streaming `send()` path (one query slot per connection),
// which lives in-process in the page — so the body runs in-page under coi.
describeCoi(
  "connection leasing (one query slot per connection) [browser coi]",
  () => {
    let h: BrowserHarness;
    let catalog: string;

    beforeAll(async () => {
      h = await setupCoiPage();
      catalog = await attachCatalog(h, "lease");
    }, 60_000);
    afterAll(async () => {
      await h?.cleanup();
    });

    test("CONCURRENT send() ON ONE CONNECTION silently splits one query's rows", async () => {
      const { counts, total } = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        type SConn = {
          query(sql: string): Promise<unknown>;
          send(
            sql: string,
            s?: boolean,
          ): Promise<AsyncIterable<{ numRows: number }>>;
          close(): Promise<void>;
        };
        const shared = (await t.db.connect()) as unknown as SConn;
        await shared.query(`SET streaming_buffer_size = '1B'`);
        const sql = `SELECT * FROM ${cat}.slow_ticker(6, 50)`;
        const rowCount = async (): Promise<number> => {
          const r = await shared.send(sql, true);
          let n = 0;
          for await (const b of r) n += b.numRows ?? 0;
          return n;
        };
        const counts = await Promise.all([0, 1, 2].map(() => rowCount()));
        await shared.close();
        return { counts, total: counts.reduce((a, b) => a + b, 0) };
      }, catalog);
      console.log(`[lease] shared connection  -> ${JSON.stringify(counts)}`);
      expect(counts.every((n) => n < 6)).toBe(true);
      expect(total).toBeLessThan(18);
    }, 30_000);

    test("CONCURRENT send() ON ITS OWN CONNECTION delivers every row", async () => {
      const counts = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        type SConn = {
          query(sql: string): Promise<unknown>;
          send(
            sql: string,
            s?: boolean,
          ): Promise<AsyncIterable<{ numRows: number }>>;
          close(): Promise<void>;
        };
        const sql = `SELECT * FROM ${cat}.slow_ticker(6, 50)`;
        const conns = (await Promise.all(
          [0, 1, 2].map(() => t.db.connect()),
        )) as unknown as SConn[];
        await Promise.all(
          conns.map((c) => c.query(`SET streaming_buffer_size = '1B'`)),
        );
        try {
          return await Promise.all(
            conns.map(async (c) => {
              const r = await c.send(sql, true);
              let n = 0;
              for await (const b of r) n += b.numRows ?? 0;
              return n;
            }),
          );
        } finally {
          await Promise.all(conns.map((c) => c.close().catch(() => {})));
        }
      }, catalog);
      console.log(`[lease] leased connections -> ${JSON.stringify(counts)}`);
      expect(counts).toEqual([6, 6, 6]);
    }, 30_000);
  },
);
