import path from "node:path";
import { expect, test } from "bun:test";
import { describeCoi } from "./conftest/coi-page";
import { backendName } from "./conftest/backend";

const ENTRY = path.join(import.meta.dir, "conftest/open-n6k-control-entry.ts");

describeCoi("openN6kControl on a caller-owned duckdb-wasm worker", () => {
  test.if(backendName === "browser-threads")(
    "registerWebsocket + ATTACH CONNECTION + query, onStatus fires",
    async () => {
      const { setupBrowserDuckdb } = await import("./conftest/browser-harness");
      const h = await setupBrowserDuckdb({ bundle: "coi", entry: ENTRY });
      try {
        const out = await h.page.evaluate(() => globalThis.__openN6kControl);
        expect(out?.rows).toEqual([
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
        ]);
        expect(out?.statuses).toContainEqual(["page_db", "connected"]);
      } finally {
        await h.cleanup();
      }
    },
    60_000,
  );
});
