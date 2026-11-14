import path from "node:path";
import { expect, test } from "bun:test";
import { describeCoi } from "./conftest/coi-page";
import { backendName } from "./conftest/backend";

const ENTRY = path.join(import.meta.dir, "conftest/stock-load-entry.ts");

describeCoi("LOAD n6k_client in stock duckdb-wasm", () => {
  for (const strictCsp of [false, true]) {
    test.if(backendName === "browser-threads")(
      `ATTACH + query + DETACH${strictCsp ? " under strict CSP" : ""}`,
      async () => {
        const { setupBrowserDuckdb } =
          await import("./conftest/browser-harness");
        const h = await setupBrowserDuckdb({
          bundle: "coi",
          entry: ENTRY,
          strictCsp,
        });
        try {
          const out = await h.page.evaluate(() => globalThis.__stockLoad);
          expect(out?.detached).toBe(true);
          expect(out?.rows.length).toBe(2);
          expect(out?.rows[0]?.id).toBe(1);
        } finally {
          await h.cleanup();
        }
      },
      60_000,
    );
  }
});
