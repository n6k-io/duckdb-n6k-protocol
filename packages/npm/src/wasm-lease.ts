// Lease a wasm connection per read; concurrent send()s on one connection silently split rows — lease, use, close.
// We never build an apache-arrow Table: duckdb-wasm bundles v17, this package v21; batches carry schema+toArray().

import type { AsyncDuckDB } from "@duckdb/duckdb-wasm";
import type {
  ArrowLikeBatch,
  ArrowLikeResult,
  ConnectionLike,
  StreamReader,
} from "./connection-shape";

const EMPTY_SCHEMA: ArrowLikeResult["schema"] = { fields: [] };

type RawBatch = ArrowLikeBatch;

// The surface of duckdb-wasm's AsyncDuckDBConnection that we rely on.
export type RawWasmConnection = {
  send(
    sql: string,
    allowStreamResult?: boolean,
  ): Promise<StreamReader | undefined>;
  close(): Promise<void>;
  useUnsafe<R>(callback: (bindings: AsyncDuckDB, connId: number) => R): R;
};

export type WasmDatabase = { connect(): Promise<RawWasmConnection> };

// send() answers `undefined` (not a rejection) once the worker is detached.
async function sendOrThrow(
  raw: RawWasmConnection,
  sql: string,
  allowStreamResult?: boolean,
): Promise<StreamReader> {
  const reader = await raw.send(sql, allowStreamResult);
  if (!reader) {
    throw new Error(`duckdb worker detached while running: ${sql}`);
  }
  return reader;
}

export async function queryViaSend(
  raw: RawWasmConnection,
  sql: string,
): Promise<ArrowLikeResult> {
  // allowStreamResult=true delivers batches incrementally, which is what makes a read cancellable (close mid-drain).
  const reader = await sendOrThrow(raw, sql, true);

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
        sendOrThrow(raw, sql, allowStreamResult),
      useUnsafe: raw.useUnsafe.bind(raw),
    };
  };
}
