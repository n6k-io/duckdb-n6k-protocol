import { Type } from "apache-arrow";
import {
  useQuery as useTanstackQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useDuckDB } from "./use-duckdb";
import type { TableRef } from "./parse-sql-tables";
import { useCatalogGate, type CatalogGateOptions } from "./catalog-gate";
import { configFingerprint } from "./attachment";
import { logger as log } from "../logger";
import { withLease, type ArrowLikeResult } from "../connection-shape";

type LikeSchema = ArrowLikeResult["schema"];

type QueryData = {
  columns: string[];
  rows: Record<string, unknown>[];
  schema: LikeSchema;
  tables: TableRef[];
};

type StatusInfo =
  | "noQuery"
  | "awaitingDb"
  | "awaitingAttach"
  | "executing"
  | "disconnected"
  | null;

type TabularResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  status: "idle" | "loading" | "ready" | "error";
  statusInfo: StatusInfo;
  error: string | null;
  schema: LikeSchema;
};

type QueryOptions = CatalogGateOptions;

const EMPTY: LikeSchema = { fields: [] };

function empty(
  status: TabularResult["status"],
  statusInfo: StatusInfo,
  error: string | null = null,
): TabularResult {
  return { columns: [], rows: [], status, statusInfo, error, schema: EMPTY };
}

export function useSQLQuery(
  query: string,
  options?: QueryOptions,
): TabularResult {
  const {
    conn,
    connect,
    status: dbStatus,
    attached,
    connStatus,
    invalidations,
  } = useDuckDB();
  const {
    tables,
    managed,
    parseReady,
    parseError,
    depsReady,
    depError,
    anyPending,
    disconnected,
    reconnecting,
  } = useCatalogGate(query, options);

  const enabled =
    dbStatus === "ready" &&
    !!conn &&
    !!query &&
    parseReady &&
    depsReady &&
    !depError &&
    !disconnected &&
    !reconnecting;

  const result = useTanstackQuery<QueryData>({
    // Catalog socket status, server invalidations and attach fingerprint are in the key so a reconnect, a server-side swap or a re-attach refetches the cached read.
    queryKey: [
      "n6k-query",
      query,
      ...managed.map((c) => connStatus[c] ?? null),
      ...managed.map((c) => invalidations[c] ?? 0),
      ...managed.map((c) =>
        attached[c] ? configFingerprint(attached[c]) : null,
      ),
    ],
    queryFn: async ({ signal }) => {
      const r = await withLease(connect, signal, (c) => c.query(query));
      const schema = r.schema;
      const cols = schema.fields.map((f: { name: string }) => f.name);

      const decimalFields = schema.fields
        .filter((f) => f.type.typeId === Type.Decimal)
        .map((f) => ({
          name: f.name,
          divisor: 10 ** (f.type.scale ?? 0),
        }));

      const rows = r
        .toArray()
        .map((row: { toJSON: () => Record<string, unknown> }) => {
          const obj = row.toJSON();
          for (const { name, divisor } of decimalFields) {
            if (obj[name] != null) obj[name] = Number(obj[name]) / divisor;
          }
          return obj;
        });

      return { columns: cols, rows, schema, tables };
    },
    enabled,
  });

  if (!query) return empty("idle", "noQuery");
  if (dbStatus !== "ready" || !conn) return empty("loading", "awaitingDb");

  if (disconnected)
    return empty(
      "error",
      "disconnected",
      "Connection to the database was lost",
    );

  if (parseError) {
    log.error("parse error:", parseError);
    return empty("error", null, parseError.message);
  }

  if (depError) return empty("error", null, depError);

  if (anyPending || !parseReady) return empty("loading", "awaitingAttach");
  if (result.status === "pending") return empty("loading", "executing");

  if (result.status === "error") {
    log.error("query error:", result.error);
    return empty("error", null, (result.error as Error).message);
  }

  return {
    columns: result.data.columns,
    rows: result.data.rows,
    status: "ready",
    statusInfo: null,
    error: null,
    schema: result.data.schema,
  };
}

export const useQuery = useSQLQuery;

export function useInvalidateQueries() {
  const queryClient = useQueryClient();
  return (pattern?: string) => {
    if (pattern) {
      queryClient.invalidateQueries({
        predicate: (query) => {
          const key = query.queryKey;
          return (
            key[0] === "n6k-query" &&
            typeof key[1] === "string" &&
            key[1].includes(pattern)
          );
        },
      });
    } else {
      queryClient.invalidateQueries({ queryKey: ["n6k-query"] });
    }
  };
}
