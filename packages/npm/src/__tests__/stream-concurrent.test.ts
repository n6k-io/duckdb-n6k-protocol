import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage, attachCatalog } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

const COUNT = 30;
const INTERVAL_MS = 50;

type Timed = { arrivals: number[]; seqs: number[] };

function countInterleaves(a: number[], b: number[]): number {
  const merged = [
    ...a.map((t) => ({ t, s: "A" })),
    ...b.map((t) => ({ t, s: "B" })),
  ].toSorted((x, y) => x.t - y.t);
  let switches = 0;
  for (let i = 1; i < merged.length; i++) {
    if (merged[i]!.s !== merged[i - 1]!.s) switches++;
  }
  return switches;
}

// Two concurrent streams over one catalog/socket — runs in-page under coi.
describeCoi(
  "concurrent streaming (two streams, one worker/socket) [browser coi]",
  () => {
    let h: BrowserHarness;
    let a: Timed;
    let b: Timed;

    beforeAll(async () => {
      h = await setupCoiPage();
      const catalog = await attachCatalog(h, "stc");
      [a, b] = await h.page.evaluate(
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
          const drainTimed = async (
            conn: SConn,
            start: number,
          ): Promise<{ arrivals: number[]; seqs: number[] }> => {
            const reader = await conn.send(
              `SELECT * FROM ${cat}.slow_ticker(${count}, ${interval})`,
              true,
            );
            const arrivals: number[] = [];
            const seqs: number[] = [];
            for await (const batch of reader) {
              if ((batch.numRows ?? 0) === 0) continue;
              arrivals.push(performance.now() - start);
              for (const row of batch.toArray())
                seqs.push(Number(row.toJSON().seq));
            }
            return { arrivals, seqs };
          };
          const connA = (await t.db.connect()) as unknown as SConn;
          const connB = (await t.db.connect()) as unknown as SConn;
          await connA.query(`SET streaming_buffer_size = '1B'`);
          await connB.query(`SET streaming_buffer_size = '1B'`);
          const start = performance.now();
          const both = await Promise.all([
            drainTimed(connA, start),
            drainTimed(connB, start),
          ]);
          await connA.close().catch(() => {});
          await connB.close().catch(() => {});
          return both;
        },
        { cat: catalog, count: COUNT, interval: INTERVAL_MS },
      );
    }, 60_000);

    afterAll(async () => {
      await h?.cleanup();
    });

    test.skip("two streams overlap in time (not serialized) and both stay live", () => {
      const streamMs = COUNT * INTERVAL_MS;
      const wall = Math.max(a.arrivals.at(-1)!, b.arrivals.at(-1)!);
      expect(wall).toBeLessThan(streamMs * 1.6);
      expect(a.arrivals[0]!).toBeLessThan(streamMs * 0.5);
      expect(b.arrivals[0]!).toBeLessThan(streamMs * 0.5);
      expect(countInterleaves(a.arrivals, b.arrivals)).toBeGreaterThan(COUNT);
    });

    test("each concurrent stream on one catalog delivers ALL its rows", () => {
      const expectedSeqs = Array.from({ length: COUNT }, (_, i) => i);
      expect(a.seqs).toEqual(expectedSeqs);
      expect(b.seqs).toEqual(expectedSeqs);
    });
  },
);
