import type { AttachConfig } from "../attachment";
import type { ClientMsg, Mirror } from "./protocol";

export type HostCoreOptions = {
  // Return Arrow IPC bytes for a SQL string; injected so this core imports no duckdb-wasm/arrow.
  runQueryIPC: (sql: string) => Promise<Uint8Array>;
  // Attach intents from the iframe; omit to keep attaches parent-only.
  onSetDesired?: (catalog: string, config: AttachConfig) => void;
  onRemoveDesired?: (catalog: string) => void;
  onReconnect?: (catalog: string) => void;
};

export type HostCore = {
  bind: (port: MessagePort) => void;
  pushState: (mirror: Mirror) => void;
  dispose: () => void;
};

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export function createHostCore(opts: HostCoreOptions): HostCore {
  let port: MessagePort | null = null;

  async function answerQuery(id: number, sql: string): Promise<void> {
    try {
      const ipc = await opts.runQueryIPC(sql);
      port?.postMessage({ __n6k: "queryOk", id, ipc }, [
        ipc.buffer as ArrayBuffer,
      ]);
    } catch (error) {
      port?.postMessage({
        __n6k: "queryErr",
        id,
        message: errorMessage(error),
      });
    }
  }

  function onMessage(ev: MessageEvent): void {
    const d = ev.data as ClientMsg | undefined;
    if (!d || typeof d !== "object" || !("__n6k" in d)) return;
    switch (d.__n6k) {
      case "query": {
        void answerQuery(d.id, d.sql);
        break;
      }
      case "setDesired": {
        opts.onSetDesired?.(d.catalog, d.config);
        break;
      }
      case "removeDesired": {
        opts.onRemoveDesired?.(d.catalog);
        break;
      }
      case "reconnect": {
        opts.onReconnect?.(d.catalog);
        break;
      }
    }
  }

  return {
    bind(p) {
      port = p;
      p.addEventListener("message", onMessage);
      p.start();
    },
    pushState(mirror) {
      port?.postMessage({ __n6k: "state", ...mirror });
    },
    dispose() {
      port?.close();
      port = null;
    },
  };
}
