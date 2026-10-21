import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import type { BackendHandle } from "./conftest/types";

// Proves the loaded extension can spawn + join a worker pthread, round-trip the wasm futex wake
// on it, and reach the main-worker-only n6k JS glue from a pthread via emscripten main-thread
// proxying. Runs `SELECT * FROM n6k_selftest_thread_park_wake()`. On the coi (browser-threads) bundle these
// are the threading primitives the off-worker data path depends on; on native it just proves
// spawn/join/CV.
type SelftestRow = {
  mode: string;
  ok: boolean;
  spawned: boolean;
  joined: boolean;
  woke_from_park: boolean;
  wait_result: number;
  main_proxy_sees_n6k: boolean;
  elapsed_ms: number;
};

backendDescribe()(`thread selftest [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    handle = await backend!.setup();
  }, 60_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("spawns and joins a worker pthread from the loaded extension", async () => {
    const r = await handle.conn.query(
      "SELECT * FROM n6k_selftest_thread_park_wake()",
    );
    const row = r.toArray()[0]!.toJSON() as SelftestRow;

    // Baseline (all backends): the extension spawned and joined a pthread with no deadlock.
    expect(row.ok).toBe(true);

    if (backendName === "browser-threads") {
      // The off-worker data path depends on these threading facts holding in the coi bundle:
      expect(row.mode).toBe("wasm_threads"); // coi bundle actually has threads
      expect(row.spawned).toBe(true); // pthread_create from the duckdb worker succeeded
      expect(row.joined).toBe(true); // join() returned (no pool/main-thread-proxy deadlock)
      // JS/C++ futex interop on a SPAWNED pthread: notify woke a genuinely-parked waiter.
      expect(row.woke_from_park).toBe(true);
      expect(row.wait_result).toBe(0); // 0 = woken (not 1 not-equal / 2 timed-out)
      // A pthread can reach the main-worker n6k glue via emscripten main-thread proxying — the
      // basis for making the control-plane glue (vsockAttach/vsockClose) safe under threads>1.
      expect(row.main_proxy_sees_n6k).toBe(true);
    }
  }, 30_000);
});
