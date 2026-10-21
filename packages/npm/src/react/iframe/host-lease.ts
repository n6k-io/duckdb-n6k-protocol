// One leased connection per RPC: RPCs dispatch concurrently and one shared connection can't serve two.

import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { ConnectionLike } from "../../connection-shape";

export type RunOne = (conn: ConnectionLike, sql: string) => Promise<Uint8Array>;

// Arrow IPC bytes straight from duckdb-wasm; avoids a cross-major tableToIPC on its bundled Table.
export function queryIPC(
  conn: ConnectionLike,
  sql: string,
): Promise<Uint8Array> {
  const c = conn as unknown as AsyncDuckDBConnection;
  if (typeof c.useUnsafe !== "function") {
    return Promise.reject(
      new Error("useDuckDBIframeHost requires a duckdb-wasm connection"),
    );
  }
  return c.useUnsafe((bindings, connId) => bindings.runQuery(connId, sql));
}

export function makeRunQueryIPC(
  connect: () => Promise<ConnectionLike>,
  runOne: RunOne = queryIPC,
): (sql: string) => Promise<Uint8Array> {
  return async (sql: string): Promise<Uint8Array> => {
    const conn = await connect();
    try {
      return await runOne(conn, sql);
    } finally {
      await conn.close();
    }
  };
}
