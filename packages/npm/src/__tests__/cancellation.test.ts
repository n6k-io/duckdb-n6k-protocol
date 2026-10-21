import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage, attachCatalog } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

// Read cancellation over the wasm streaming path + the withLease abort contract.
// Both operate on the in-process wasm connection, so the body runs in-page.
describeCoi("read cancellation [browser coi]", () => {
  let h: BrowserHarness;
  let catalog: string;

  beforeAll(async () => {
    h = await setupCoiPage();
    catalog = await attachCatalog(h, "cancel");
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  test("cancelSent() does NOT stop a streaming send()", async () => {
    const drained = await h.page.evaluate(async (cat: string) => {
      const t = globalThis.__n6kTest!;
      type RawConn = {
        query(sql: string): Promise<unknown>;
        send(
          sql: string,
          s?: boolean,
        ): Promise<AsyncIterable<{ numRows: number }>>;
        cancelSent(): Promise<boolean>;
        close(): Promise<void>;
      };
      const a = (await t.db.connect()) as unknown as RawConn;
      await a.query("SET streaming_buffer_size = '1B'");
      const reader = await a.send(
        `SELECT * FROM ${cat}.slow_ticker(50, 100)`,
        true,
      );
      let n = 0;
      const drain = (async () => {
        for await (const b of reader) n += b.numRows ?? 0;
      })().catch(() => {});
      await new Promise((r) => setTimeout(r, 400));
      await a.cancelSent();
      await drain;
      await a.close().catch(() => {});
      return n;
    }, catalog);
    expect(drained).toBe(50);
  }, 20_000);

  test.skip("aborting withLease stops a slow read early and frees the worker", async () => {
    const res = await h.page.evaluate(async (cat: string) => {
      const t = globalThis.__n6kTest!;
      const slow = `SELECT * FROM ${cat}.slow_ticker(50, 100)`;
      const ac = new AbortController();
      let drained = 0;
      const t0 = performance.now();
      const read = t
        .withLease(t.connect, ac.signal, async (c) => {
          await c.query("SET streaming_buffer_size = '1B'");
          const r = await c.query(slow);
          drained = r.toArray().length;
          return drained;
        })
        .catch(() => -1);
      await new Promise((r) => setTimeout(r, 500));
      ac.abort();
      await read;
      const settleMs = performance.now() - t0;

      const t1 = performance.now();
      const after = await t.withLease(t.connect, undefined, (c) =>
        c.query("SELECT 42 AS n"),
      );
      return {
        settleMs,
        drained,
        afterLen: after.toArray().length,
        afterMs: performance.now() - t1,
      };
    }, catalog);
    expect(res.settleMs).toBeLessThan(3000);
    expect(res.drained).toBeLessThan(50);
    expect(res.afterLen).toBe(1);
    expect(res.afterMs).toBeLessThan(2000);
  }, 20_000);

  test("an already-aborted signal never opens a connection", async () => {
    const res = await h.page.evaluate(async () => {
      const t = globalThis.__n6kTest!;
      const ac = new AbortController();
      ac.abort();
      let ran = false;
      let rejected = false;
      try {
        await t.withLease(t.connect, ac.signal, async () => {
          ran = true;
          return 1;
        });
      } catch {
        rejected = true;
      }
      return { ran, rejected };
    });
    expect(res.rejected).toBe(true);
    expect(res.ran).toBe(false);
  });

  test("withLease closes the lease on the normal path too", async () => {
    const closed = await h.page.evaluate(async () => {
      const t = globalThis.__n6kTest!;
      let n = 0;
      const fakeConnect = async () => ({
        query: async () => ({ schema: { fields: [] }, toArray: () => [] }),
        close: async () => {
          n++;
        },
      });
      await t.withLease(fakeConnect, undefined, (c) => c.query("SELECT 1"));
      return n;
    });
    expect(closed).toBe(1);
  });
});
