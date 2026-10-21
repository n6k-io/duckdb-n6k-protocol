// Lease a wasm connection per read; concurrent send()s on one connection silently split rows — lease, use, close.
// We never build an apache-arrow Table: duckdb-wasm bundles v17, this package v21; batches carry schema+toArray().

import type { ArrowLikeResult, ConnectionLike } from "./connection-shape";

const EMPTY_SCHEMA: ArrowLikeResult["schema"] = { fields: [] };

type RawBatch = {
  schema: ArrowLikeResult["schema"];
  toArray(): Array<{ toJSON(): Record<string, unknown> }>;
};

// The surface of duckdb-wasm's AsyncDuckDBConnection that we rely on.
export type RawWasmConnection = {
  send(
    sql: string,
    allowStreamResult?: boolean,
  ): Promise<AsyncIterable<RawBatch> | undefined>;
  close(): Promise<void>;
  useUnsafe<R>(callback: (bindings: never, connId: number) => R): R;
};

export type WasmDatabase = { connect(): Promise<RawWasmConnection> };

export async function queryViaSend(
  raw: RawWasmConnection,
  sql: string,
): Promise<ArrowLikeResult> {
  // allowStreamResult=true delivers batches incrementally, which is what makes a read cancellable (close mid-drain).
  // send() answers `undefined` (not a rejection) once the worker is detached.
  const reader = await raw.send(sql, true);
  if (!reader) {
    throw new Error(`duckdb worker detached while running: ${sql}`);
  }

  const batches: RawBatch[] = [];
  for await (const batch of reader) batches.push(batch);

  // Never read reader.schema (undefined before open and after drain); batches carry it.
  const schema = batches[0]?.schema ?? EMPTY_SCHEMA;

  return {
    schema,
    toArray: () => batches.flatMap((b) => b.toArray()),
  };
}

// Build the `connect` consumers use: each call opens a fresh cheap connection; query() runs via send().
export function createWasmConnect(
  db: WasmDatabase,
): () => Promise<ConnectionLike> {
  return async (): Promise<ConnectionLike> => {
    const raw = await db.connect();
    return {
      query: (sql: string) => queryViaSend(raw, sql),
      close: () => raw.close(),
      send: (sql: string, allowStreamResult?: boolean) =>
        raw.send(sql, allowStreamResult),
      useUnsafe: raw.useUnsafe.bind(raw),
    } as unknown as ConnectionLike;
  };
}
