import { BROWSER_AVAILABLE, browserUnavailableReason } from "./browser-gate";
import type { Backend, BackendHandle } from "./types";

const caps = {
  reconnect: true,
  status: true,
  forceDrop: true,
  protocolCounts: true,
};

// Boots the threaded (coi) bundle — the only supported wasm runtime. Opened
// single-threaded so LOAD doesn't deadlock, then raised afterwards.
export const browserThreadsBackend: Backend = {
  name: "browser-threads",
  caps,
  available: () => BROWSER_AVAILABLE,
  unavailableReason: browserUnavailableReason,
  async setup(opts): Promise<BackendHandle> {
    const { setupBrowserDuckdb } = await import("./browser-harness");
    const h = await setupBrowserDuckdb({
      onStatus: opts?.onStatus,
      bundle: "coi",
    });
    return {
      name: "browser-threads",
      caps,
      conn: h.conn,
      reconnect: h.reconnect,
      cleanup: h.cleanup,
      probeMemory: () => h.page.evaluate(() => globalThis.__n6kProbeMemory!()),
      vsockSelftest: (catalog) =>
        h.page.evaluate(
          (c) => globalThis.__n6kVsockSelftest!(c),
          catalog as string | undefined,
        ),
    };
  },
};
