import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

// Automates the profile gate for checkpoint 1.c: N concurrent full scans of the deliberately-slow
// slowdb.main.slow (~500ms server await) must OVERLAP on the coi bundle, where the async scan yields its
// worker between frames. `parallel×N` (N connections) should collapse to ≈ one scan while `serial×N`
// (one connection) is ≈ N scans. Must run on coi, NOT native: @duckdb/node-api gives each query its own
// libuv thread, so native overlaps even a blocking scan and can't distinguish the fix. Held at threads=1
// (single-worker overlap) AND threads=4 (yield frees scheduler threads, so overlap isn't pool-bound).

const N = 8;

type Arms = { serial: number; parallel: number };

function overlapSuite(threads: number) {
  describeCoi(`async scan overlap [browser coi, threads=${threads}]`, () => {
    let h: BrowserHarness;
    let arms: Arms;

    beforeAll(async () => {
      h = await setupCoiPage(threads);
      arms = await h.page.evaluate(
        async ({ n }): Promise<Arms> => {
          const t = globalThis.__n6kTest!;
          type C = {
            query(sql: string): Promise<unknown>;
            close(): Promise<void>;
          };
          // slowdb.main.slow routes to the server's SlowProvider only when the ATTACH alias is `slowdb`.
          const setup = (await t.connect()) as unknown as C;
          try {
            await setup.query(`ATTACH '${t.serverUrl}' AS slowdb (TYPE n6k)`);
          } finally {
            await setup.close();
          }

          const SQL = "SELECT * FROM slowdb.main.slow";
          const scanOnce = async (): Promise<void> => {
            const c = (await t.connect()) as unknown as C;
            try {
              await c.query(SQL);
            } finally {
              await c.close();
            }
          };
          const time = async (fn: () => Promise<void>): Promise<number> => {
            const s = performance.now();
            await fn();
            return performance.now() - s;
          };

          await scanOnce(); // warm (first scan also settles the WsClient)
          const serial = await time(async () => {
            const c = (await t.connect()) as unknown as C;
            try {
              for (let i = 0; i < n; i++) await c.query(SQL);
            } finally {
              await c.close();
            }
          });
          const parallel = await time(async () => {
            await Promise.all(Array.from({ length: n }, () => scanOnce()));
          });
          return { serial, parallel };
        },
        { n: N },
      );
    }, 60_000);

    afterAll(async () => {
      await h?.cleanup();
    });

    test(`parallel×${N} overlaps (parallel < serial / 3)`, () => {
      // Observed ≈ 7.8× (parallel ≈ single). A 3× threshold leaves wide headroom while still failing
      // hard if the wake regresses back to the serialized blocking path (~1.0×).
      expect(arms.parallel).toBeLessThan(arms.serial / 3);
    });
  });
}

overlapSuite(1);
overlapSuite(4);
