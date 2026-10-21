import { useEffect, useRef, useState } from "react";
import { Type } from "apache-arrow";
import { useDuckDB } from "./use-duckdb";
import { useCatalogGate } from "./catalog-gate";
import { configFingerprint } from "./attachment";
import type { ArrowLikeField, ConnectionLike } from "../connection-shape";
import { logger as log } from "../logger";

type LikeSchema = { fields: ArrowLikeField[] };

type ArrowLikeBatch = {
  schema: LikeSchema;
  numRows: number;
  toArray(): Array<{ toJSON(): Record<string, unknown> }>;
};
type StreamReader = AsyncIterable<ArrowLikeBatch> & {
  cancel?: () => Promise<void>;
};
type StreamingConnection = ConnectionLike & {
  send(sql: string, allowStreamResult?: boolean): Promise<StreamReader>;
};

export type Row = Record<string, unknown>;

export type StreamStatus = "idle" | "streaming" | "done" | "error";

// A multicast batch stream whose identity is stable until the query restarts (the signal for consumers to reset).
export type RowStream = {
  subscribe(onBatch: (rows: Row[], batch: ArrowLikeBatch) => void): () => void;
  readonly rowCount: number;
};

export type StreamMeta = {
  stream: RowStream;
  columns: string[];
  schema: LikeSchema;
  status: StreamStatus;
  error: string | null;
};

export type StreamQueryOptions = {
  enabled?: boolean;
  // DuckDB streaming_buffer_size in bytes; the default of 1 forces each chunk to overflow so batches deliver per tick instead of buffering to ~1MB.
  streamingBufferSizeBytes?: number;
};

const EMPTY: LikeSchema = { fields: [] };

type MutableRowStream = RowStream & {
  _emit(rows: Row[], batch: ArrowLikeBatch): void;
};

function createStream(): MutableRowStream {
  const subscribers = new Set<(rows: Row[], batch: ArrowLikeBatch) => void>();
  let count = 0;
  return {
    get rowCount() {
      return count;
    },
    subscribe(onBatch) {
      subscribers.add(onBatch);
      return () => {
        subscribers.delete(onBatch);
      };
    },
    _emit(rows, batch) {
      count += rows.length;
      for (const cb of subscribers) cb(rows, batch);
    },
  };
}

// DECIMAL arrives as a scaled integer we divide back to a number.
function describe(schema: LikeSchema): {
  columns: string[];
  decimals: Array<{ name: string; divisor: number }>;
} {
  return {
    columns: schema.fields.map((f) => f.name),
    decimals: schema.fields
      .filter((f) => f.type.typeId === Type.Decimal)
      .map((f) => ({ name: f.name, divisor: 10 ** (f.type.scale ?? 0) })),
  };
}

function batchRows(
  batch: ArrowLikeBatch,
  decimals: Array<{ name: string; divisor: number }>,
): Row[] {
  return batch.toArray().map((row) => {
    const obj = row.toJSON();
    for (const { name, divisor } of decimals) {
      if (obj[name] != null) obj[name] = Number(obj[name]) / divisor;
    }
    return obj;
  });
}

export function useStreamSQLQuery(
  query: string,
  options?: StreamQueryOptions,
): StreamMeta {
  const { connect, status: dbStatus, attached } = useDuckDB();

  const enabled = options?.enabled ?? true;
  const gate = useCatalogGate(enabled ? query : "");
  const gateError = gate.parseError?.message ?? gate.depError;
  const gateOpen =
    gate.parseReady &&
    gate.depsReady &&
    !gate.disconnected &&
    !gate.reconnecting;
  // Restarts the stream when a managed catalog's attached config changes (attach, detach, path or options).
  const attachKey = gate.managed
    .map((c) => (attached[c] ? configFingerprint(attached[c]) : ""))
    .join("|");
  const streamingBufferSizeBytes = options?.streamingBufferSizeBytes ?? 1;

  // Stable per (query, enabled, bufferSize); changing any mints a NEW stream so consumers reset. Held in state (not useMemo) to guarantee stable identity.
  const [current, setCurrent] = useState(() => ({
    query,
    enabled,
    bufferSize: streamingBufferSizeBytes,
    stream: createStream(),
  }));
  if (
    current.query !== query ||
    current.enabled !== enabled ||
    current.bufferSize !== streamingBufferSizeBytes
  ) {
    setCurrent({
      query,
      enabled,
      bufferSize: streamingBufferSizeBytes,
      stream: createStream(),
    });
  }
  const stream = current.stream;

  const [meta, setMeta] = useState<Omit<StreamMeta, "stream">>({
    columns: [],
    schema: EMPTY,
    status: "idle",
    error: null,
  });

  useEffect(() => {
    let cancelled = false;
    let ownConn: StreamingConnection | null = null;
    let reader: StreamReader | null = null;

    (async () => {
      if (!enabled || !query || dbStatus !== "ready") {
        setMeta({ columns: [], schema: EMPTY, status: "idle", error: null });
        return;
      }
      if (gateError) {
        setMeta({
          columns: [],
          schema: EMPTY,
          status: "error",
          error: gateError,
        });
        return;
      }
      if (!gateOpen) {
        setMeta({ columns: [], schema: EMPTY, status: "idle", error: null });
        return;
      }
      setMeta({ columns: [], schema: EMPTY, status: "streaming", error: null });
      try {
        // A dedicated connection: two streams sharing one query slot would truncate each other.
        const c = (await connect()) as StreamingConnection;
        if (cancelled) {
          await c.close();
          return;
        }
        if (typeof c.send !== "function") {
          throw new TypeError(
            "this connection does not support streaming send() — " +
              "useStreamSQLQuery requires the duckdb-wasm backend",
          );
        }
        ownConn = c;
        if (
          !Number.isInteger(streamingBufferSizeBytes) ||
          streamingBufferSizeBytes < 1
        ) {
          throw new TypeError(
            `invalid streamingBufferSizeBytes ${streamingBufferSizeBytes}: ` +
              `expected a positive integer`,
          );
        }
        // DuckDB's size parser requires the unit.
        await c.query(
          `SET streaming_buffer_size = '${streamingBufferSizeBytes}B'`,
        );

        // allowStreamResult=true: a lazy result set, so a never-ending SELECT yields batches incrementally.
        reader = await c.send(query, true);

        for await (const batch of reader) {
          if (cancelled) break;
          if ((batch.numRows ?? 0) === 0) continue;
          const { columns, decimals } = describe(batch.schema);
          const rows = batchRows(batch, decimals);
          // Publish columns/schema ONCE from the first batch; rows flow to subscribers via _emit, not React state.
          setMeta((prev) =>
            prev.columns.length > 0
              ? prev
              : { ...prev, columns, schema: batch.schema },
          );
          stream._emit(rows, batch);
        }

        if (!cancelled) setMeta((prev) => ({ ...prev, status: "done" }));
      } catch (error) {
        if (!cancelled) {
          const message = (error as Error).message;
          log.error("useStreamSQLQuery error:", error);
          setMeta((prev) => ({ ...prev, status: "error", error: message }));
        }
      } finally {
        try {
          await reader?.cancel?.();
        } catch {
          /* ignore */
        }
        try {
          await ownConn?.close();
        } catch {
          /* ignore */
        }
      }
    })();

    return () => {
      cancelled = true;
      try {
        void reader?.cancel?.();
      } catch {
        /* ignore */
      }
    };
  }, [
    stream,
    connect,
    dbStatus,
    query,
    enabled,
    streamingBufferSizeBytes,
    gateError,
    gateOpen,
    attachKey,
  ]);

  return { stream, ...meta };
}

// Rolling-window consumer keeping the most recent `maxRows` (omit to accumulate all); resets when `stream` changes.
export function useWindow(stream: RowStream, maxRows?: number): Row[] {
  const [rows, setRows] = useState<Row[]>([]);
  const [seenStream, setSeenStream] = useState(stream);
  if (seenStream !== stream) {
    setSeenStream(stream);
    setRows([]);
  }

  const maxRef = useRef<number | undefined>(maxRows);
  useEffect(() => {
    maxRef.current = maxRows;
  }, [maxRows]);

  useEffect(() => {
    return stream.subscribe((batchRows) => {
      setRows((prev) => {
        const merged = prev.length > 0 ? [...prev, ...batchRows] : batchRows;
        const max = maxRef.current;
        return max !== undefined && merged.length > max
          ? merged.slice(-max)
          : merged;
      });
    });
  }, [stream]);

  return rows;
}

// Latest-row consumer: the most recent row emitted (null before the first); resets when `stream` changes.
export function useLast(stream: RowStream): Row | null {
  const [last, setLast] = useState<Row | null>(null);
  const [seenStream, setSeenStream] = useState(stream);
  if (seenStream !== stream) {
    setSeenStream(stream);
    setLast(null);
  }

  useEffect(() => {
    return stream.subscribe((batchRows) => {
      if (batchRows.length > 0) setLast(batchRows.at(-1) ?? null);
    });
  }, [stream]);

  return last;
}
