import { describe } from "bun:test";
import { BROWSER_AVAILABLE } from "./browser-gate";
import { SKIP_SERVER_TESTS } from "../_server-gate";
import type { BrowserHarness } from "./browser-harness";

// Shared setup for tests that exercise the wasm driver's in-process internals
// (live `db`, streaming `send()`, app sockets, in-process helpers). Those objects
// can't cross the page.evaluate boundary, so each test body runs in-page against
// window.__n6kTest, under the coi (browser-threads) bundle. Skips without Chromium
// or a live server.
export const COI_RUNNABLE = BROWSER_AVAILABLE && !SKIP_SERVER_TESTS;
export const describeCoi = COI_RUNNABLE ? describe : describe.skip;

// `maxThreads > 1` raises duckdb's scheduler pool via `SET threads` after LOAD
// (opens single-threaded regardless, so dlopen doesn't deadlock). Default 1.
export async function setupCoiPage(
  maxThreads?: number,
): Promise<BrowserHarness> {
  const { setupBrowserDuckdb } = await import("./browser-harness");
  return setupBrowserDuckdb({ bundle: "coi", maxThreads });
}

// ATTACH a uniquely-named server catalog in-page; returns its name.
export async function attachCatalog(
  h: BrowserHarness,
  prefix: string,
): Promise<string> {
  return h.page.evaluate(async (p: string) => {
    const t = globalThis.__n6kTest!;
    const cat = `${p}_${Date.now()}`;
    const c = await t.connect();
    try {
      await c.query(`ATTACH '${t.serverUrl}' AS ${cat} (TYPE n6k)`);
    } finally {
      await c.close();
    }
    return cat;
  }, prefix);
}
