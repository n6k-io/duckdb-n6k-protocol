import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import type { BackendHandle } from "./conftest/types";

// The shared-memory data path (RX/TX rings + pthread mailboxes) requires the
// threaded `coi` duckdb-wasm bundle, whose heap is a real, forwardable
// SharedArrayBuffer. This runs under the standard backend harness — select it
// with `BACKEND=browser-threads` (coi). The only other backend is native, which
// has no page at all (probeMemory undefined), so the SharedArrayBuffer assertion
// is scoped to browser-threads.
backendDescribe()(`coi shared-memory heap [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    // Resolving proves the enforced-coi boot path end to end: coi bundle
    // instantiated, opened single-threaded, LOAD n6k_client, SET threads=4, ATTACH.
    handle = await backend!.setup();
  }, 60_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  test.if(backendName === "browser-threads")(
    "duckdb coi heap is a forwardable SharedArrayBuffer",
    async () => {
      const probe = await handle.probeMemory!();
      // The whole plan rests on this: duckdb's wasm heap is a SharedArrayBuffer
      // we can hand to the ws-worker and pthreads. If any fail, the shared-heap
      // approach is not viable on this bundle.
      expect(probe.found).toBe(true);
      expect(probe.isSharedArrayBuffer).toBe(true);
      expect(probe.byteLength).toBeGreaterThan(0);
      expect(probe.source).not.toBe("none");
    },
  );

  test.if(backendName === "browser-threads")(
    "vsock attach takes the heap-backed ring path (not the SAB bootstrap)",
    async () => {
      // The catalog attached during setup drives its byte-rings through duckdb's shared
      // wasm heap: the rebuilt coi extension passed a malloc pointer, JS built ring views
      // over Module.wasmMemory at that offset. A non-null offset proves we are NOT on the
      // standalone-SharedArrayBuffer fallback — i.e. Phase 1 is actually live.
      const st = await handle.vsockSelftest!();
      expect(st.attached).toBe(true);
      expect(st.heap).toBe(true);
      expect(st.ringOffset).toBeGreaterThan(0);
      expect(st.canFree).toBe(true);
    },
  );

  test("coi bundle executes queries", async () => {
    const r = await handle.conn.query("select 42 as v");
    expect(r.toArray()[0]!.toJSON()).toEqual({ v: 42 });
  });
});
