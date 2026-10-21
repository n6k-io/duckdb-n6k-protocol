import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { WASM_AVAILABLE } from "../_wasm-gate";

function findChromium(): string | null {
  // The `test` npm script overrides HOME with a throwaway dir (to isolate
  // duckdb-wasm's on-disk extension cache), which would hide the real Playwright
  // browser cache from os.homedir(). It stashes the real home in N6K_REAL_HOME
  // so we can still locate the cache; an explicit PLAYWRIGHT_BROWSERS_PATH wins.
  const base =
    process.env.PLAYWRIGHT_BROWSERS_PATH ||
    path.join(
      process.env.N6K_REAL_HOME || os.homedir(),
      process.platform === "darwin"
        ? "Library/Caches/ms-playwright"
        : ".cache/ms-playwright",
    );
  if (!fs.existsSync(base)) return null;
  for (const dir of fs.readdirSync(base)) {
    if (!dir.startsWith("chromium-") || dir.includes("headless")) continue;
    const candidates = [
      "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
      "chrome-mac/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
      "chrome-linux/chrome",
      "chrome-win/chrome.exe",
    ];
    for (const rel of candidates) {
      const exe = path.join(base, dir, rel);
      if (fs.existsSync(exe)) return exe;
    }
  }
  return null;
}

export const CHROMIUM_PATH = findChromium();

export const BROWSER_AVAILABLE = WASM_AVAILABLE && CHROMIUM_PATH !== null;

/** Why BROWSER_AVAILABLE is false — surfaced by the backend fail-fast guard. */
export function browserUnavailableReason(): string {
  if (!WASM_AVAILABLE) {
    return (
      "coi wasm extension not found under packages/npm/wasm/**/wasm_threads/" +
      "n6k_client.duckdb_extension.wasm — run `make wasm` to build and stage it " +
      "(`make release` builds the native extension only)."
    );
  }
  if (CHROMIUM_PATH === null) {
    return (
      "Playwright Chromium not found (looked under $PLAYWRIGHT_BROWSERS_PATH, " +
      "else <home>/{Library/Caches,.cache}/ms-playwright, where <home> is " +
      "$N6K_REAL_HOME || os.homedir()). Install it with " +
      "`bunx playwright install chromium`, or set PLAYWRIGHT_BROWSERS_PATH."
    );
  }
  return "browser backend unavailable for an unknown reason.";
}
