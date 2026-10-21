import { describe, test } from "bun:test";
import { SKIP_SERVER_TESTS } from "../_server-gate";
import type { Backend, BackendName, Capability } from "./types";
import { nativeBackend } from "./native";
import { browserThreadsBackend } from "./browser";

const ALL: Record<BackendName, Backend> = {
  native: nativeBackend,
  "browser-threads": browserThreadsBackend,
};

const selection = process.env.BACKEND;
if (!selection) {
  throw new Error(
    "BACKEND env var is required. Set BACKEND=native|browser-threads to run a " +
      "backend, or BACKEND=skip (CI, where no server/wasm is available).",
  );
}

export const BACKEND_SKIP = selection === "skip";

function resolveBackend(): Backend | null {
  if (BACKEND_SKIP) return null;
  const b = ALL[selection as BackendName];
  if (!b) {
    throw new Error(
      `Unknown BACKEND="${selection}". Use native|browser-threads|skip.`,
    );
  }
  // Fail fast: a backend was explicitly requested but can't actually run, so
  // every backendDescribe() block would silently skip and report a false green.
  // Error loudly with the specific cause instead. (Use BACKEND=skip to run the
  // backend-agnostic suite with no backend on purpose.)
  if (!b.available()) {
    throw new Error(
      `BACKEND="${selection}" was selected but its runtime is unavailable, so ` +
        `every backend test would silently skip. ` +
        (b.unavailableReason?.() ?? "") +
        ` — set BACKEND=skip to intentionally run without a backend.`,
    );
  }
  return b;
}

export const backend: Backend | null = resolveBackend();

export const backendName: string = backend?.name ?? "skip";

export function backendDescribe(...caps: Capability[]) {
  if (SKIP_SERVER_TESTS || BACKEND_SKIP || !backend || !backend.available())
    return describe.skip;
  const b = backend;
  return caps.every((c) => b.caps[c]) ? describe : describe.skip;
}

export function testFailing(
  expectedFailing: string,
  name: string,
  fn: () => void | Promise<void>,
  timeout?: number,
): void {
  const xfail = expectedFailing
    .split(",")
    .map((s) => s.trim())
    .includes(backendName);
  (xfail ? test.failing : test)(name, fn, timeout);
}
