import path from "node:path";
import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import { serverExec, pushInvalidate } from "./conftest/server";
import type { ReactProbe } from "./conftest/react-e2e-types";

const TIMEOUT = 60_000;

describeCoi("React refetch on server invalidate [browser coi]", () => {
  let h: BrowserHarness;

  beforeAll(async () => {
    const { setupBrowserDuckdb } = await import("./conftest/browser-harness");
    h = await setupBrowserDuckdb({
      bundle: "coi",
      entry: path.join(import.meta.dir, "conftest/react-page-entry.tsx"),
    });
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  async function probeUntil(
    done: (p: ReactProbe) => boolean,
    timeoutMs = 15_000,
  ): Promise<ReactProbe | undefined> {
    const deadline = Date.now() + timeoutMs;
    let probe: ReactProbe | undefined;
    while (Date.now() < deadline) {
      probe = await h.page.evaluate(() => globalThis.__n6kReactProbe);
      if (probe && done(probe)) return probe;
      await new Promise((r) => setTimeout(r, 50));
    }
    return probe;
  }

  test(
    "a mounted useQuery shows server-side changes only after the server invalidates its catalog",
    async () => {
      const cat = `rinv_${Date.now()}`;
      await h.page.evaluate(
        ({ cat }) =>
          globalThis.__n6kMountReact!(
            cat,
            `SELECT name FROM ${cat}.main.users WHERE id = 1`,
          ),
        { cat },
      );
      const before = await probeUntil((p) => p.status === "ready");
      expect(before?.rows).toEqual([{ name: "Alice" }]);

      await serverExec(
        `UPDATE ${cat}.main.users SET name = 'Zed' WHERE id = 1`,
        cat,
      );
      // Nothing tells the client yet, so the cached result stands.
      await new Promise((r) => setTimeout(r, 500));
      const cached = await h.page.evaluate(() => globalThis.__n6kReactProbe);
      expect(cached?.rows).toEqual([{ name: "Alice" }]);

      expect(await pushInvalidate(cat, "main")).toBe(1);
      const after = await probeUntil(
        (p) => p.status === "ready" && p.rows[0]?.name === "Zed",
      );
      expect(after?.rows).toEqual([{ name: "Zed" }]);
    },
    TIMEOUT,
  );
});
