import * as duckdb from "@duckdb/duckdb-wasm";
import { createN6kWorker, type RegisteredWebsocket } from "./create-n6k-worker";
import { logger as log } from "./logger";

import type { WsStatus } from "./types";
import { N6K_VERSION } from "./version";
import { N6K_DEFAULT_WORKER_URLS } from "./workers/manifest";

export type CreateDuckDBOptions = {
  // Defaults to the jsDelivr CDN bundle; pass a local bundle for offline/hermetic use.
  bundle?: duckdb.DuckDBBundle;
  // Task-scheduler thread cap at open() time. The threaded (coi) bundle deadlocks
  // on LOAD/dlopen when scheduler pthreads are parked; open single-threaded, then
  // raise threads after extensions are loaded.
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

export async function createDuckDB(opts: CreateDuckDBOptions = {}): Promise<{
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
}> {
  log.debug(
    "(v" + N6K_VERSION + ") createDuckDB starting, crossOriginIsolated =",
    globalThis.crossOriginIsolated,
  );

  const bundle =
    opts.bundle ?? (await duckdb.selectBundle(jsDelivrBundlesWithCoi()));
  log.debug("bundle selected:", {
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
  } = createN6kWorker({
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

  log.debug("instantiating WASM...", {
    mainModule: bundle.mainModule,
    pthreadWorker: bundle.pthreadWorker,
  });
  try {
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  } catch (error) {
    log.error("WASM instantiation failed:", error);
    throw error;
  }
  log.debug("WASM instantiated");

  // The threaded (coi) bundle deadlocks on LOAD/dlopen when scheduler pthreads are
  // parked, so always open single-threaded; the caller raises `SET threads` after
  // `LOAD n6k_client`. coi is the only supported bundle, so this is unconditional. An
  // explicit opts.maximumThreads always wins.
  const maximumThreads = opts.maximumThreads ?? 1;
  log.debug("opening database with accessMode READ_WRITE...", {
    maximumThreads,
  });
  await db.open({
    accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
    allowUnsignedExtensions: true,
    maximumThreads,
  });
  log.debug("database opened");

  const conn = await db.connect();
  log.debug("connection created");

  const extensionRepo = opts.extensionRepository ?? globalThis.location.origin;
  log.debug("setting extension repository:", extensionRepo);
  await conn.query(`SET custom_extension_repository = '${extensionRepo}';`);

  return {
    db,
    conn,
    reconnect,
    registerWebsocket,
    replaceWebsocket,
    listCatalogs,
  };
}

export { type WsStatus } from "./types";
