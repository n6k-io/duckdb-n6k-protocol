import * as duckdb from "@duckdb/duckdb-wasm";

declare global {
  var __stockLoad:
    | { rows: Array<Record<string, unknown>>; detached: boolean }
    | undefined;
}

// Plain duckdb-wasm with no @n6k.io/db: the extension must bring its own glue and pump.
(async () => {
  try {
    const worker = new Worker("/duckdb/duckdb-browser-threads.worker.js");
    const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
    await db.instantiate(
      "/duckdb/duckdb-threads.wasm",
      "/duckdb/duckdb-browser-threads.pthread.worker.js",
    );
    await db.open({ allowUnsignedExtensions: true, maximumThreads: 1 });
    const conn = await db.connect();
    await conn.query(
      `SET custom_extension_repository = '${globalThis.location.origin}'`,
    );
    await conn.query("LOAD n6k_client");
    const url = globalThis.__n6kServerUrl ?? "n6k://localhost:8099";
    await conn.query(`ATTACH '${url}' AS db (TYPE n6k)`);
    const r = await conn.query(
      "SELECT id, name FROM db.main.users ORDER BY id LIMIT 2",
    );
    const rows = r.toArray().map((row) => {
      const o = row.toJSON() as Record<string, unknown>;
      return { id: Number(o.id), name: o.name };
    });
    await conn.query("DETACH db");
    globalThis.__stockLoad = { rows, detached: true };
    globalThis.__n6kReady = true;
  } catch (error) {
    globalThis.__n6kError = String((error as Error)?.stack ?? error);
  }
})();
