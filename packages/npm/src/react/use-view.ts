import { useEffect, useRef } from "react";
import { Type } from "apache-arrow";
import {
  useQuery as useTanstackQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useDuckDB } from "./use-duckdb";
import { logger as log } from "../logger";
import { withLease, type ArrowLikeResult } from "../connection-shape";

type LikeSchema = ArrowLikeResult["schema"];

type QueryData = {
  columns: string[];
  rows: Record<string, unknown>[];
  schema: LikeSchema;
};

type TabularResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  status: "idle" | "loading" | "ready" | "error";
  error: string | null;
  schema: LikeSchema | null;
};

export function useView(db: string, name: string, sql: string): TabularResult {
  const {
    conn,
    connect,
    status: dbStatus,
    connStatus,
    invalidations,
  } = useDuckDB();
  const queryClient = useQueryClient();
  const disconnected = connStatus[db] === "disconnected";
  const enabled =
    dbStatus === "ready" && !!conn && !!db && !!name && !!sql && !disconnected;
  const prevRef = useRef<{ db: string; name: string } | null>(null);

  // Drop old view on param change/unmount; runs detached since cleanup can't be async, so it releases its own lease and swallows rejection.
  useEffect(() => {
    prevRef.current = enabled ? { db, name } : null;
    return () => {
      if (prevRef.current && conn) {
        const { db: prevDb, name: prevName } = prevRef.current;
        void (async () => {
          const c = await connect();
          try {
            await c.query(`DROP VIEW IF EXISTS "${prevDb}".main."${prevName}"`);
          } catch (error_) {
            log.error("view cleanup failed:", error_);
          } finally {
            await c.close();
          }
        })();
        queryClient.removeQueries({ queryKey: ["n6k-view", prevDb, prevName] });
      }
    };
  }, [conn, connect, db, name, sql, enabled, queryClient]);

  const result = useTanstackQuery<QueryData>({
    queryKey: [
      "n6k-view",
      db,
      name,
      sql,
      connStatus[db] ?? null,
      invalidations[db] ?? 0,
    ],
    queryFn: async ({ signal }) => {
      // One lease for CREATE + SELECT: the read must not race another hook's CREATE OR REPLACE of the same view.
      const result: ArrowLikeResult = await withLease(
        connect,
        signal,
        async (c) => {
          await c.query(
            `CREATE OR REPLACE VIEW "${db}".main."${name}" AS ${sql}`,
          );
          return c.query(`SELECT * FROM "${db}".main."${name}"`);
        },
      );
      const schema = result.schema;
      const cols = schema.fields.map((f: { name: string }) => f.name);

      const decimalFields = schema.fields
        .filter((f) => f.type.typeId === Type.Decimal)
        .map((f) => ({
          name: f.name,
          divisor: 10 ** (f.type.scale ?? 0),
        }));

      const rows = result
        .toArray()
        .map((row: { toJSON: () => Record<string, unknown> }) => {
          const obj = row.toJSON();
          for (const { name, divisor } of decimalFields) {
            if (obj[name] != null) obj[name] = Number(obj[name]) / divisor;
          }
          return obj;
        });

      return { columns: cols, rows, schema };
    },
    enabled,
  });

  if (disconnected) {
    return {
      columns: [],
      rows: [],
      status: "error",
      error: "Connection to the database was lost",
      schema: null,
    };
  }

  if (!enabled) {
    return { columns: [], rows: [], status: "idle", error: null, schema: null };
  }

  if (result.status === "pending") {
    return {
      columns: [],
      rows: [],
      status: "loading",
      error: null,
      schema: null,
    };
  }

  if (result.status === "error") {
    log.error("view error:", result.error);
    return {
      columns: [],
      rows: [],
      status: "error",
      error: (result.error as Error).message,
      schema: null,
    };
  }

  return {
    columns: result.data.columns,
    rows: result.data.rows,
    status: "ready",
    error: null,
    schema: result.data.schema,
  };
}
