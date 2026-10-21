import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage, attachCatalog } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

// Runs the coi bundle at `SET threads=N` and fires N concurrent n6k scans on independent
// connections, to prove the emscripten Worker pool can satisfy N duckdb compute pthreads PLUS
// n6k's per-catalog service pthread without deadlock — the prerequisite for running n6k under
// threads>1. A pool-exhaustion deadlock hangs the page.evaluate and times out the beforeAll.
// Correct row counts confirm concurrent scans return right results.
const THREADS = 4;
const CONCURRENCY = 6; // concurrent in-flight n6k requests, > THREADS to stress the pool
const ROWS = 10;
const INTERVAL_MS = 10;

describeCoi(`threads>1 pool gate [coi, threads=${THREADS}]`, () => {
  let h: BrowserHarness;
  let observedThreads = 0;
  let counts: number[] = [];

  beforeAll(async () => {
    h = await setupCoiPage(THREADS);
    const cat = await attachCatalog(h, "poolgate");
    [observedThreads, counts] = await h.page.evaluate(
      async ({ c, k, rows, interval }) => {
        const t = globalThis.__n6kTest!;
        type Conn = {
          query(sql: string): Promise<{
            toArray(): Array<{ toJSON(): Record<string, unknown> }>;
          }>;
          close(): Promise<void>;
        };
        const scalar = async (conn: Conn, sql: string) => {
          const res = await conn.query(sql);
          return Number(res.toArray()[0]!.toJSON().n);
        };

        // 1. Confirm the post-LOAD `SET threads=N` actually took.
        const c0 = (await t.db.connect()) as unknown as Conn;
        const threads = await scalar(
          c0,
          "SELECT current_setting('threads') AS n",
        );
        await c0.close();

        // 2. Fire K concurrent n6k scans on K independent connections. If the pool can't
        //    spare a slot for the n6k service pthread alongside N compute pthreads, this
        //    Promise.all deadlocks and the whole evaluate hangs.
        const conns = (await Promise.all(
          Array.from({ length: k }, () => t.db.connect()),
        )) as unknown as Conn[];
        const rowCounts = await Promise.all(
          conns.map((conn) =>
            scalar(
              conn,
              `SELECT count(*) AS n FROM ${c}.slow_ticker(${rows}, ${interval})`,
            ),
          ),
        );
        await Promise.all(conns.map((conn) => conn.close().catch(() => {})));

        return [threads, rowCounts] as [number, number[]];
      },
      { c: cat, k: CONCURRENCY, rows: ROWS, interval: INTERVAL_MS },
    );
  }, 60_000);

  afterAll(async () => {
    await h?.cleanup();
  });

  test(`SET threads=${THREADS} took effect`, () => {
    expect(observedThreads).toBe(THREADS);
  });

  test(`${CONCURRENCY} concurrent n6k scans all complete with correct results (no pool deadlock)`, () => {
    expect(counts).toHaveLength(CONCURRENCY);
    for (const n of counts) expect(n).toBe(ROWS);
  });
});
