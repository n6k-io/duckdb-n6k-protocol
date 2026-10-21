import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";

// n6k routes its control frames off duckdb-wasm's request/response channel, so
// AsyncDuckDB (in the page) never logs "unassociated response". The warning is
// emitted in the page's main thread, so we intercept console.warn in-page.
describeCoi(
  "n6k control frames stay off duckdb-wasm's channel [browser coi]",
  () => {
    let h: BrowserHarness;

    beforeAll(async () => {
      h = await setupCoiPage();
    }, 60_000);
    afterAll(async () => {
      await h?.cleanup();
    });

    test("attach → query → detach emits no `unassociated response` warning", async () => {
      const unassociated = await h.page.evaluate(async () => {
        const t = globalThis.__n6kTest!;
        const warnings: string[] = [];
        const original = console.warn;
        console.warn = (...args: unknown[]) => {
          warnings.push(args.map(String).join(" "));
        };
        try {
          const cat = `nuw_${Date.now()}`;
          const c = await t.connect();
          try {
            await c.query(`ATTACH '${t.serverUrl}' AS ${cat} (TYPE n6k)`);
            await c.query(`SELECT count(*) AS n FROM ${cat}.main.users`);
            await c.query(`DETACH ${cat}`);
          } finally {
            await c.close();
          }
        } finally {
          console.warn = original;
        }
        return warnings.filter((w) => w.includes("unassociated response"));
      });
      expect(unassociated).toEqual([]);
    });
  },
);
