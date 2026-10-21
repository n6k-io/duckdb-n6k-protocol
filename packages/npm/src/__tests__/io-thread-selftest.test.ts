import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import type { BackendHandle } from "./conftest/types";

// Gates the single-shared-I/O-thread design: one consumer pthread parks on a single standalone
// doorbell int32 in shared heap and drains TWO independent vsock rings when woken, while two
// producer pthreads write frames into their own ring and ring the doorbell after each write. Runs
// `SELECT * FROM n6k_selftest_io_thread_doorbell()`. On the coi (browser-threads) bundle it proves the
// doorbell futex wake + multi-ring drain loop (the core of the shared reactor) before that machinery
// is wired into WsClient; on native it proves the same loop over std::deque rings + a CV doorbell.
type IoSelftestRow = {
  mode: string;
  ok: boolean;
  spawned: boolean;
  joined: boolean;
  doorbell_woke: boolean;
  frames_expected: number;
  frames_drained: number;
  elapsed_ms: number;
};

backendDescribe()(`io thread selftest [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    handle = await backend!.setup();
  }, 60_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("one consumer pthread drains two rings woken by a single doorbell", async () => {
    const r = await handle.conn.query(
      "SELECT * FROM n6k_selftest_io_thread_doorbell()",
    );
    const row = r.toArray()[0]!.toJSON() as IoSelftestRow;

    // Baseline (all backends): the consumer drained every frame from both rings and joined cleanly.
    expect(row.ok).toBe(true);
    expect(row.frames_drained).toBe(row.frames_expected);

    if (backendName === "browser-threads") {
      expect(row.mode).toBe("wasm_threads"); // coi bundle actually has threads
      expect(row.spawned).toBe(true); // consumer + both producers spawned
      expect(row.joined).toBe(true); // all three joined (no pool/wait deadlock)
      // A single doorbell futex genuinely parked the consumer and a producer's notify woke it — the
      // wake mechanism the shared I/O thread relies on to service N rings from one park point.
      expect(row.doorbell_woke).toBe(true);
    }
  }, 30_000);
});
