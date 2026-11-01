import { N6kDuckDB } from "../../n6k-duckdb";
import type { CatalogState } from "../../attach/catalog-states";

declare global {
  // Attaches `catalog` from the test server through setDatabases.
  var __n6kAttachCatalog: ((catalog: string) => void) | undefined;
  var __n6kCatalogState:
    | ((catalog: string) => CatalogState | undefined)
    | undefined;
  var __n6kRead:
    | ((sql: string) => Promise<Array<Record<string, unknown>>>)
    | undefined;
}

const duckdb = new N6kDuckDB({
  bundle: {
    mainModule: "/duckdb/duckdb-coi.wasm",
    mainWorker: "/duckdb/duckdb-browser-coi.worker.js",
    pthreadWorker: "/duckdb/duckdb-browser-coi.pthread.worker.js",
  },
  extensionRepository: globalThis.location.origin,
});
duckdb.metadataFns.catalogs.subscribe(() => {});

globalThis.__n6kAttachCatalog = (catalog) =>
  duckdb.setDatabases({
    [catalog]: globalThis.__n6kServerUrl ?? "n6k://localhost:8099",
  });

globalThis.__n6kCatalogState = (catalog) =>
  duckdb.metadataFns.catalogs.getSnapshot()[catalog];

globalThis.__n6kRead = async (sql) => {
  const c = await duckdb.connect();
  try {
    const r = await c.query(sql);
    return r.toArray().map((row) => row.toJSON());
  } finally {
    await c.close();
  }
};

globalThis.__n6kReady = true;
