import * as duckdb from "@duckdb/duckdb-wasm";
import { createN6kWorker, type RegisteredWebsocket } from "./create-n6k-worker";
import { logger as log, stopwatch } from "./logger";

import type { WsStatus } from "./types";
import { N6K_VERSION } from "./version";
import { N6K_DEFAULT_WORKER_URLS } from "./workers/manifest";

export type CreateDuckDBOptions = {
  // Defaults to the jsDelivr CDN bundle; pass a local bundle for offline/hermetic use.
  bundle?: duckdb.DuckDBBundle;
  // Task-scheduler thread cap at open() time; defaults to 1. The threaded (coi) bundle
  // deadlocks on LOAD/dlopen when scheduler pthreads are parked, so `SET threads` above 1
  // only after extensions are loaded. N6kDuckDB never raises it.
  maximumThreads?: number;
  duckdbWorkerUrl?: string;
  wsWorkerUrl?: string;
  extensionRepository?: string;
  onStatus?: (catalog: string, status: WsStatus) => void;
  // The server added or removed catalogs on a registered wsId; `catalogs` is the full new list.
  onCatalogsChanged?: (wsId: string, catalogs: string[]) => void;
  // The server invalidated `schemas` of an attached catalog (e.g. it swapped the catalog); cached reads are stale.
  onInvalidated?: (catalog: string, schemas: string[]) => void;
};

// duckdb-wasm's getJsDelivrBundles() ships only mvp + eh, so its selectBundle()
// can never return the threaded coi bundle even on a cross-origin-isolated page.
// n6k is coi-only: the extension is built solely for shared memory (wasm_threads),
// so eh/mvp bundles cannot load it. We re-add the coi entry so selectBundle picks
// it when the browser supports wasm threads/SIMD + isolation; createDuckDB then
// hard-requires it (see below). The dist base is derived from the eh URL to stay
// version-agnostic.
function jsDelivrBundlesWithCoi(): duckdb.DuckDBBundles {
  const bundles = duckdb.getJsDelivrBundles();
  const ehBase = bundles.eh?.mainModule?.replace(/duckdb-eh\.wasm$/, "");
  if (!ehBase) return bundles;
  return {
    ...bundles,
    coi: {
      mainModule: `${ehBase}duckdb-coi.wasm`,
      mainWorker: `${ehBase}duckdb-browser-coi.worker.js`,
      pthreadWorker: `${ehBase}duckdb-browser-coi.pthread.worker.js`,
    },
  };
}

let nextInstanceId = 1;

export type DuckDBInstance = {
  id: number;
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
  reconnect: (catalog: string) => void;
  registerWebsocket: (
    socket: WebSocket,
    token: string,
  ) => Promise<RegisteredWebsocket>;
  // Swap the socket behind a wsId; rebinds an attached catalog without DETACH/ATTACH.
  replaceWebsocket: (
    wsId: string,
    socket: WebSocket,
    token: string,
  ) => Promise<void>;
  // Catalog names the server serves on a registered wsId.
  listCatalogs: (wsId: string) => Promise<string[]>;
  // Terminates the duckdb worker, its pthreads and ws-workers. Idempotent.
  dispose: () => Promise<void>;
};

export async function createDuckDB(
  opts: CreateDuckDBOptions = {},
): Promise<DuckDBInstance> {
  const id = nextInstanceId++;
  const sinceBoot = stopwatch();
  log.debug(
    `#${id} (v${N6K_VERSION}) createDuckDB starting, crossOriginIsolated =`,
    globalThis.crossOriginIsolated,
  );

  const bundle =
    opts.bundle ?? (await duckdb.selectBundle(jsDelivrBundlesWithCoi()));
  log.debug(`#${id} bundle selected (${sinceBoot()}):`, {
    mainModule: bundle.mainModule,
    mainWorker: bundle.mainWorker,
    pthreadWorker: bundle.pthreadWorker,
  });

  // n6k is coi-only: the extension is built for shared memory (wasm_threads) and
  // cannot load into a non-threaded engine. When auto-selecting, a null
  // pthreadWorker means selectBundle fell back to eh/mvp (no wasm threads / not
  // cross-origin isolated) — fail fast instead of loading an engine the extension
  // can't attach to. An explicit opts.bundle is the caller's responsibility.
  if (!opts.bundle && bundle.pthreadWorker == null) {
    throw new TypeError(
      "n6k requires the threaded (coi) duckdb-wasm bundle, but selectBundle " +
        `resolved to a non-threaded bundle (${bundle.mainModule}). The browser ` +
        "lacks wasm threads or the page is not cross-origin isolated. " +
        "crossOriginIsolated = " +
        globalThis.crossOriginIsolated,
    );
  }

  const {
    worker,
    reconnect,
    registerWebsocket,
    replaceWebsocket,
    listCatalogs,
    dispose: disposeWorkers,
  } = createN6kWorker({
    instanceId: id,
    mainWorkerUrl: bundle.mainWorker!,
    duckdbWorkerUrl:
      opts.duckdbWorkerUrl ?? N6K_DEFAULT_WORKER_URLS.duckdbWorkerUrl,
    wsWorkerUrl: opts.wsWorkerUrl ?? N6K_DEFAULT_WORKER_URLS.wsWorkerUrl,
    onStatus: opts.onStatus,
    onCatalogsChanged: opts.onCatalogsChanged,
    onInvalidated: opts.onInvalidated,
  });

  const duckdbLogger: duckdb.Logger = {
    log: (entry) => log.debug("duckdb", entry),
  };
  const db = new duckdb.AsyncDuckDB(duckdbLogger, worker);

  let disposed: Promise<void> | undefined;
  const dispose = () => {
    disposed ??= (async () => {
      log.debug(`#${id} disposing`);
      disposeWorkers();
      await db.terminate();
    })();
    return disposed;
  };

  try {
    return await boot();
  } catch (error) {
    await dispose();
    throw error;
  }

  async function boot(): Promise<DuckDBInstance> {
    const sinceInstantiate = stopwatch();
    log.debug(`#${id} instantiating WASM...`, {
      mainModule: bundle.mainModule,
      pthreadWorker: bundle.pthreadWorker,
    });
    try {
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    } catch (error) {
      log.error("WASM instantiation failed:", error);
      throw error;
    }
    log.debug(`#${id} WASM instantiated (${sinceInstantiate()})`);

    const maximumThreads = opts.maximumThreads ?? 1;
    const sinceOpen = stopwatch();
    log.debug(`#${id} opening database with accessMode READ_WRITE...`, {
      maximumThreads,
    });
    await db.open({
      accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
      allowUnsignedExtensions: true,
      maximumThreads,
    });
    log.debug(`#${id} database opened (${sinceOpen()})`);

    const conn = await db.connect();
    log.debug(`#${id} connection created (boot so far ${sinceBoot()})`);

    const extensionRepo =
      opts.extensionRepository ?? globalThis.location.origin;
    log.debug("setting extension repository:", extensionRepo);
    await conn.query(`SET custom_extension_repository = '${extensionRepo}';`);

    return {
      id,
      db,
      conn,
      reconnect,
      registerWebsocket,
      replaceWebsocket,
      listCatalogs,
      dispose,
    };
  }
}

export { type WsStatus } from "./types";
