import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage, attachCatalog } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

const COUNT = 150;
const INTERVAL_MS = 20;

// Streaming delivery timing over the wasm `send()` path — runs in-page under coi.
describeCoi("streaming delivery timing (slow_ticker) [browser coi]", () => {
  let h: BrowserHarness;
  let catalog: string;

  beforeAll(async () => {
    h = await setupCoiPage();
    catalog = await attachCatalog(h, "stm");
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  async function drain(): Promise<{ seqs: number[]; arrivals: number[] }> {
    return h.page.evaluate(
      async ({ cat, count, interval }) => {
        const t = globalThis.__n6kTest!;
        type SConn = {
          query(sql: string): Promise<unknown>;
          send(
            sql: string,
            s?: boolean,
          ): Promise<
            AsyncIterable<{
              numRows: number;
              toArray(): Array<{ toJSON(): Record<string, unknown> }>;
            }>
          >;
          close(): Promise<void>;
        };
        const conn = (await t.db.connect()) as unknown as SConn;
        await conn.query(`SET streaming_buffer_size = '1B'`);
        const start = performance.now();
        const reader = await conn.send(
          `SELECT * FROM ${cat}.slow_ticker(${count}, ${interval})`,
          true,
        );
        const seqs: number[] = [];
        const arrivals: number[] = [];
        for await (const batch of reader) {
          if ((batch.numRows ?? 0) === 0) continue;
          arrivals.push(performance.now() - start);
          for (const row of batch.toArray())
            seqs.push(Number(row.toJSON().seq));
        }
        await conn.close();
        return { seqs, arrivals };
      },
      { cat: catalog, count: COUNT, interval: INTERVAL_MS },
    );
  }

  test("streams every row, in order", async () => {
    const { seqs } = await drain();
    expect(seqs).toEqual(Array.from({ length: COUNT }, (_, i) => i));
  }, 20_000);

  test.skip("delivers batches incrementally, not buffered until the end", async () => {
    const { seqs, arrivals } = await drain();
    expect(seqs.length).toBe(COUNT);

    const first = arrivals[0]!;
    const last = arrivals.at(-1)!;
    const spread = last - first;
    const streamMs = COUNT * INTERVAL_MS;

    console.log(
      `[slow_ticker] batches=${arrivals.length} first=${first.toFixed(0)} ` +
        `last=${last.toFixed(0)} spread=${spread.toFixed(0)}`,
    );

    expect(first).toBeLessThan(INTERVAL_MS * 4);
    expect(spread).toBeGreaterThan(streamMs * 0.8);
  }, 20_000);
});
