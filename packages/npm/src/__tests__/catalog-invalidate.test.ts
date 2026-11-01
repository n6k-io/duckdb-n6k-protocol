import path from "node:path";
import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import { serverExec, pushInvalidate } from "./conftest/server";
import type { CatalogState } from "../attach/catalog-states";

const TIMEOUT = 60_000;

describeCoi("catalog store on server invalidate [browser coi]", () => {
  let h: BrowserHarness;

  beforeAll(async () => {
    const { setupBrowserDuckdb } = await import("./conftest/browser-harness");
    h = await setupBrowserDuckdb({
      bundle: "coi",
      entry: path.join(import.meta.dir, "conftest/catalog-page-entry.ts"),
    });
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  async function stateUntil(
    cat: string,
    done: (s: CatalogState) => boolean,
    timeoutMs = 15_000,
  ): Promise<CatalogState | undefined> {
    const deadline = Date.now() + timeoutMs;
    let state: CatalogState | undefined;
    while (Date.now() < deadline) {
      state = await h.page.evaluate(
        (c) => globalThis.__n6kCatalogState!(c),
        cat,
      );
      if (state && done(state)) return state;
      await new Promise((r) => setTimeout(r, 50));
    }
    return state;
  }

  const read = (sql: string) =>
    h.page.evaluate((s) => globalThis.__n6kRead!(s), sql);

  test(
    "an attached catalog turns ready, and its version moves when the server invalidates it",
    async () => {
      const cat = `cinv_${Date.now()}`;
      await h.page.evaluate((c) => globalThis.__n6kAttachCatalog!(c), cat);
      const ready = await stateUntil(cat, (s) => s.status === "ready");
      expect(ready?.status).toBe("ready");
      expect(
        await read(`SELECT name FROM ${cat}.main.users WHERE id = 1`),
      ).toEqual([{ name: "Alice" }]);

      await serverExec(
        `UPDATE ${cat}.main.users SET name = 'Zed' WHERE id = 1`,
        cat,
      );
      expect(await pushInvalidate(cat, "main")).toBe(1);
      const after = await stateUntil(cat, (s) => s.version !== ready?.version);
      expect(after?.version).not.toBe(ready?.version);
      expect(
        await read(`SELECT name FROM ${cat}.main.users WHERE id = 1`),
      ).toEqual([{ name: "Zed" }]);
    },
    TIMEOUT,
  );
});
