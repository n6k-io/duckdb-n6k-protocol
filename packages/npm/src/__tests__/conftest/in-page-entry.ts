import { createDuckDB } from "../../create-duckdb";
import type { RegisteredWebsocket } from "../../create-n6k-worker";
import { createWasmConnect, queryViaSend } from "../../wasm-lease";
import { parseSqlTables } from "../../react/parse-sql-tables";
import { withLease, type ConnectionLike } from "../../connection-shape";
import { unpackFrame } from "../../ws-framing";
import type * as duckdb from "@duckdb/duckdb-wasm";

type WireResult = {
  fields: { name: string; typeId: number; scale?: number }[];
  rows: Record<string, unknown>[];
  error?: string;
};

// Tag values page.evaluate can't carry (BigInt/Date/Uint8Array); decodeValue rebuilds them.
function encodeValue(v: unknown): unknown {
  if (v === null || v === undefined) return v;
  if (typeof v === "bigint") return { $n6k: "bigint", v: v.toString() };
  if (v instanceof Date) return { $n6k: "date", v: v.toISOString() };
  if (v instanceof Uint8Array) {
    return {
      $n6k: "bytes",
      v: [...v].map((b) => b.toString(16).padStart(2, "0")).join(""),
    };
  }
  if (Array.isArray(v)) return v.map((x) => encodeValue(x));
  if (typeof v === "object") {
    // Nested Arrow values expose toJSON() to a plainer form; checked after Date/Uint8Array.
    const maybe = v as { toJSON?: () => unknown; [Symbol.iterator]?: unknown };
    if (typeof maybe.toJSON === "function") return encodeValue(maybe.toJSON());
    if (typeof maybe[Symbol.iterator] === "function") {
      return [...(v as Iterable<unknown>)].map((x) => encodeValue(x));
    }
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      out[k] = encodeValue(val);
    }
    return out;
  }
  return v;
}

declare global {
  interface Window {
    __n6kOnStatus?: (catalog: string, status: string) => void;
    __n6k?: {
      query: (sql: string) => Promise<WireResult>;
      reconnect: (catalog: string) => void;
      detach: () => Promise<void>;
    };
    // Open a fresh leased connection (production `withLease` path). Exposed so an
    // out-of-page driver (the profiler) can run a multi-connection concurrency
    // probe entirely in-page.
    __n6kConnect?: () => Promise<ConnectionLike>;
    __n6kReady?: boolean;
    __n6kError?: string;
    __n6kServerUrl?: string;
    // "coi" selects the threaded bundle; anything else falls back to eh, which
    // cannot LOAD n6k_client — the harness always sets "coi".
    __n6kBundle?: string;
    // DuckDB scheduler thread count, applied via `SET threads` after LOAD n6k_client.
    __n6kMaxThreads?: number;
    // Probe duckdb-wasm's heap through the driver's worker: confirms the coi build
    // hands out a forwardable SharedArrayBuffer for the shared-memory data path.
    __n6kProbeMemory?: () => Promise<{
      found: boolean;
      isSharedArrayBuffer: boolean;
      byteLength: number;
      source: string;
    }>;
    // Report how a catalog's vsock channel is backed: heap == rings live in duckdb's
    // shared WebAssembly.Memory; else the standalone-SAB bootstrap.
    // Omit `catalog` for the most recent attach.
    __n6kVsockSelftest?: (catalog?: string) => Promise<{
      attached: boolean;
      heap: boolean;
      ringOffset: number | null;
      canFree: boolean;
    }>;
    // In-page test surface: the wasm driver internals a test needs to exercise
    // that can't cross the page.evaluate boundary (the live `db`, streaming
    // `send()`, app-socket registration, in-process helpers). Tests run their
    // whole body via `page.evaluate` against this.
    __n6kTest?: {
      serverUrl: string;
      wsUrl: string;
      db: duckdb.AsyncDuckDB;
      connect: () => Promise<ConnectionLike>;
      createWasmConnect: typeof createWasmConnect;
      queryViaSend: typeof queryViaSend;
      parseSqlTables: typeof parseSqlTables;
      withLease: typeof withLease;
      unpackFrame: typeof unpackFrame;
      registerWebsocket: (
        socket: WebSocket,
        token: string,
      ) => Promise<RegisteredWebsocket>;
      replaceWebsocket: (
        wsId: string,
        socket: WebSocket,
        token: string,
      ) => Promise<void>;
      listCatalogs: (wsId: string) => Promise<string[]>;
      // Every onCatalogsChanged call, in order.
      catalogsChanged: { wsId: string; catalogs: string[] }[];
      // Every onInvalidated call, in order.
      invalidated: { catalog: string; schemas: string[] }[];
    };
  }
}

// Derive the ws:// base from the n6k:// server URL (n6k://host → ws://host/ws).
function wsUrlFromServer(serverUrl: string): string {
  try {
    const u = new URL(serverUrl.replace(/^n6k:/, "ws:"));
    return `ws://${u.host}/ws`;
  } catch {
    return "ws://localhost:8099/ws";
  }
}

async function boot(): Promise<void> {
  const coi = globalThis.__n6kBundle === "coi";
  const bundle = coi
    ? {
        mainModule: "/duckdb/duckdb-coi.wasm",
        mainWorker: "/duckdb/duckdb-browser-coi.worker.js",
        pthreadWorker: "/duckdb/duckdb-browser-coi.pthread.worker.js",
      }
    : {
        mainModule: "/duckdb/duckdb-eh.wasm",
        mainWorker: "/duckdb/duckdb-browser-eh.worker.js",
        pthreadWorker: null,
      };
  const catalogsChanged: { wsId: string; catalogs: string[] }[] = [];
  const invalidated: { catalog: string; schemas: string[] }[] = [];
  const {
    db,
    conn,
    reconnect,
    registerWebsocket,
    replaceWebsocket,
    listCatalogs,
  } = await createDuckDB({
    bundle,
    // Open the coi bundle single-threaded so LOAD/dlopen doesn't deadlock
    // against parked scheduler pthreads.
    maximumThreads: coi ? 1 : undefined,
    extensionRepository: globalThis.location.origin,
    onStatus: (catalog, status) => globalThis.__n6kOnStatus?.(catalog, status),
    onCatalogsChanged: (wsId, catalogs) =>
      catalogsChanged.push({ wsId, catalogs }),
    onInvalidated: (catalog, schemas) => invalidated.push({ catalog, schemas }),
  });
  await conn.query("LOAD n6k_client;");
  // Raise the scheduler pool AFTER LOAD — opening multi-threaded deadlocks dlopen.
  const maxThreads = globalThis.__n6kMaxThreads ?? 1;
  if (maxThreads > 1) await conn.query(`SET threads=${maxThreads};`);
  const serverUrl = globalThis.__n6kServerUrl ?? "n6k://localhost:8099";
  await conn.query(`ATTACH '${serverUrl}' AS db (TYPE n6k)`);

  // Reach duckdb-wasm's heap by messaging the driver's worker directly (the `conn`
  // proxy can't express this). Replies arrive on a dedicated MessageChannel so
  // duckdb-wasm's own worker.onmessage handler never sees the probe response.
  globalThis.__n6kProbeMemory = () =>
    new Promise((resolve, reject) => {
      const worker = (db as unknown as { _worker?: Worker })._worker;
      if (!worker) {
        reject(new Error("driver worker not reachable on db"));
        return;
      }
      const ch = new MessageChannel();
      const timer = setTimeout(() => {
        ch.port1.close();
        reject(new Error("n6k-probe-memory timed out"));
      }, 5000);
      ch.port1.addEventListener("message", (e: MessageEvent) => {
        clearTimeout(timer);
        ch.port1.close();
        resolve(e.data);
      });
      ch.port1.start();
      worker.postMessage({ type: "n6k-probe-memory", replyPort: ch.port2 }, [
        ch.port2,
      ]);
    });

  // Same worker + reply-port harness as __n6kProbeMemory, for the heap-path selftest.
  globalThis.__n6kVsockSelftest = (catalog?: string) =>
    new Promise((resolve, reject) => {
      const worker = (db as unknown as { _worker?: Worker })._worker;
      if (!worker) {
        reject(new Error("driver worker not reachable on db"));
        return;
      }
      const ch = new MessageChannel();
      const timer = setTimeout(() => {
        ch.port1.close();
        reject(new Error("n6k-vsock-selftest timed out"));
      }, 5000);
      ch.port1.addEventListener("message", (e: MessageEvent) => {
        clearTimeout(timer);
        ch.port1.close();
        resolve(e.data);
      });
      ch.port1.start();
      worker.postMessage(
        { type: "n6k-vsock-selftest", catalog, replyPort: ch.port2 },
        [ch.port2],
      );
    });

  globalThis.__n6kConnect = createWasmConnect(db);
  globalThis.__n6kTest = {
    serverUrl,
    wsUrl: wsUrlFromServer(serverUrl),
    db,
    connect: createWasmConnect(db),
    createWasmConnect,
    queryViaSend,
    parseSqlTables,
    withLease,
    unpackFrame,
    registerWebsocket,
    replaceWebsocket,
    listCatalogs,
    catalogsChanged,
    invalidated,
  };
  globalThis.__n6k = {
    async query(sql: string): Promise<WireResult> {
      try {
        const r = await conn.query(sql);
        return {
          fields: r.schema.fields.map((f) => {
            const t = f.type as { typeId: number; scale?: number };
            return {
              name: f.name,
              typeId: t.typeId,
              ...(t.scale === undefined ? {} : { scale: t.scale }),
            };
          }),
          rows: r
            .toArray()
            .map((row) => encodeValue(row.toJSON()) as Record<string, unknown>),
        };
      } catch (error) {
        return {
          fields: [],
          rows: [],
          error: error instanceof Error ? error.message : String(error),
        };
      }
    },
    reconnect,
    async detach(): Promise<void> {
      try {
        await conn.close();
      } catch {
        /* ignore */
      }
      try {
        await db.terminate();
      } catch {
        /* ignore */
      }
    },
  };
  globalThis.__n6kReady = true;
}

boot().catch((error) => {
  globalThis.__n6kError =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
});
