import { N6kDuckDB } from "../../n6k-duckdb";
import type { CatalogState } from "../../attach/catalog-states";

declare global {
  // Builds an N6kDuckDB that attaches `catalog` from the test server; resolves once connect() does.
  var __n6kAttachCatalog: ((catalog: string) => Promise<void>) | undefined;
  var __n6kCatalogState:
    | ((catalog: string) => CatalogState | undefined)
    | undefined;
  var __n6kRead:
    | ((sql: string) => Promise<Array<Record<string, unknown>>>)
    | undefined;
}

let duckdb: N6kDuckDB | null = null;

function current(): N6kDuckDB {
  if (!duckdb) throw new Error("no catalog attached");
  return duckdb;
}

globalThis.__n6kAttachCatalog = async (catalog) => {
  await duckdb?.dispose();
  duckdb = new N6kDuckDB({
    bundle: {
      mainModule: "/duckdb/duckdb-coi.wasm",
      mainWorker: "/duckdb/duckdb-browser-coi.worker.js",
      pthreadWorker: "/duckdb/duckdb-browser-coi.pthread.worker.js",
    },
    extensionRepository: globalThis.location.origin,
    databases: {
      [catalog]: globalThis.__n6kServerUrl ?? "n6k://localhost:8099",
    },
  });
  duckdb.metadataFns.catalogs.subscribe(() => {});
  const c = await duckdb.connect();
  await c.close();
};

globalThis.__n6kCatalogState = (catalog) =>
  current().metadataFns.catalogs.getSnapshot()[catalog];

globalThis.__n6kRead = async (sql) => {
  const c = await current().connect();
  try {
    const r = await c.query(sql);
    return r.toArray().map((row) => row.toJSON());
  } finally {
    await c.close();
  }
};

globalThis.__n6kReady = true;
