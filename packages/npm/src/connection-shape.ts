// Framework-agnostic Arrow-like result shape shared by React hooks and native adapter.
export type ArrowLikeField = {
  name: string;
  type: { typeId: number; scale?: number };
};

export type ArrowLikeResult = {
  schema: { fields: ArrowLikeField[] };
  toArray(): Array<{ toJSON(): Record<string, unknown> }>;
};

export interface ConnectionLike {
  query(sql: string): Promise<ArrowLikeResult>;
  // Release the leased connection; borrowed handles (iframe proxy, native) close() to a no-op.
  close(): Promise<void>;
}

// Wrap a connection the caller does not own; close() is inert.
export function borrow(conn: Pick<ConnectionLike, "query">): ConnectionLike {
  return {
    query: (sql) => conn.query(sql),
    close: async () => {},
  };
}

// Lease a connection for one unit of work; abort cancels by CLOSING it (the only thing that stops a send() drain).
export async function withLease<T>(
  connect: () => Promise<ConnectionLike>,
  signal: AbortSignal | undefined,
  fn: (conn: ConnectionLike) => Promise<T>,
): Promise<T> {
  if (signal?.aborted) throw signal.reason ?? new Error("aborted");
  const conn = await connect();
  // Close exactly once: duckdb-wasm reuses connection ids, so a second close can kill another lease's connection.
  let closing: Promise<void> | undefined;
  const close = () => (closing ??= conn.close());
  const onAbort = () => {
    void close().catch(() => {});
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    return await fn(conn);
  } finally {
    signal?.removeEventListener("abort", onAbort);
    await close();
  }
}
