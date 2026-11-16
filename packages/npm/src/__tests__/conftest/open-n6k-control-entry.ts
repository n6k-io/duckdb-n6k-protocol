import * as duckdb from "@duckdb/duckdb-wasm";
import { openN6kControl } from "../../n6k-control";
import type { WsStatus } from "../../types";

declare global {
  var __openN6kControl:
    | {
        rows: Array<Record<string, unknown>>;
        statuses: Array<[string, WsStatus]>;
      }
    | undefined;
}

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

    const statuses: Array<[string, WsStatus]> = [];
    const control = await openN6kControl(worker, {
      onStatus: (catalog, status) => statuses.push([catalog, status]),
    });

    const serverUrl = globalThis.__n6kServerUrl ?? "n6k://localhost:8099";
    const host = new URL(serverUrl.replace(/^n6k:/, "ws:")).host;
    const ws = new WebSocket(`ws://${host}/ws?catalog=page_db`);
    const { wsId } = await control.registerWebsocket(ws, "");
    await conn.query(`ATTACH '' AS page_db (TYPE n6k, CONNECTION '${wsId}')`);
    const r = await conn.query(
      "SELECT id, name FROM page_db.main.users ORDER BY id LIMIT 2",
    );
    const rows = r.toArray().map((row) => {
      const o = row.toJSON() as Record<string, unknown>;
      return { id: Number(o.id), name: o.name };
    });
    await conn.query("DETACH page_db");
    ws.close();
    control.dispose();
    await db.terminate();
    globalThis.__openN6kControl = { rows, statuses };
    globalThis.__n6kReady = true;
  } catch (error) {
    globalThis.__n6kError = String((error as Error)?.stack ?? error);
  }
})();
