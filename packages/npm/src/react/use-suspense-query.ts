import { Type } from "apache-arrow";
import { useSuspenseQuery as useTanstackSuspenseQuery } from "@tanstack/react-query";
import { useDuckDB } from "./use-duckdb";
import {
  catalogOf,
  parseSqlTables,
  stringToTableRef,
  type TableRef,
} from "./parse-sql-tables";
import { statusOf, configFingerprint } from "./attachment";
import { withLease, type ArrowLikeResult } from "../connection-shape";

type LikeSchema = ArrowLikeResult["schema"];

export type SuspenseQueryResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  schema: LikeSchema;
};

type CatalogRef = string | { catalog: string };

export type SuspenseQueryOptions = {
  tables?: Array<string | TableRef>;
  catalogs?: CatalogRef[];
};

// Suspense variant of useQuery: throws the in-flight promise (no `enabled` flag) so caller must ensure conn ready and catalogs attached before mount.
export function useSuspenseQuery(
  query: string,
  options?: SuspenseQueryOptions,
): SuspenseQueryResult {
  const {
    conn,
    connect,
    status: dbStatus,
    desired,
    attached,
    errors,
  } = useDuckDB();
  const hasExplicitTables = !!options?.tables;

  const parse = useTanstackSuspenseQuery<TableRef[]>({
    queryKey: ["n6k-parse", query],
    queryFn: async ({ signal }) => {
      if (!query) {
        throw new Error("useSuspenseQuery requires a non-empty SQL query");
      }
      if (dbStatus !== "ready" || !conn) {
        throw new Error(
          "useSuspenseQuery requires the DuckDB conn to be ready before mounting; " +
            "gate the calling subtree on status === 'ready' or use useQuery instead.",
        );
      }
      if (hasExplicitTables) return [];
      return withLease(connect, signal, (c) => parseSqlTables(c, query));
    },
    staleTime: Infinity,
  });

  const tables: TableRef[] = hasExplicitTables
    ? options!.tables!.map((t) =>
        typeof t === "string" ? stringToTableRef(t) : t,
      )
    : parse.data;

  const autoCatalogs: string[] = tables
    .map((t) => catalogOf(t))
    .filter((c): c is string => c !== null);
  const explicitCatalogs: string[] = (options?.catalogs ?? []).map((r) =>
    typeof r === "string" ? r : r.catalog,
  );
  const allCatalogs = [...new Set([...autoCatalogs, ...explicitCatalogs])];
  const managed = allCatalogs.filter(
    (c) => explicitCatalogs.includes(c) || desired[c] !== undefined,
  );

  return useTanstackSuspenseQuery<SuspenseQueryResult>({
    queryKey: [
      "n6k-suspense-query",
      query,
      ...managed.map((c) =>
        attached[c] ? configFingerprint(attached[c]) : null,
      ),
    ],
    queryFn: async ({ signal }) => {
      if (dbStatus !== "ready" || !conn) {
        throw new Error(
          "useSuspenseQuery: DuckDB conn became unready between renders",
        );
      }
      const attachState = { desired, attached, errors };
      for (const c of managed) {
        const s = statusOf(attachState, c);
        const ready =
          s === "ready" || (s === undefined && !explicitCatalogs.includes(c));
        if (!ready) {
          throw new Error(
            `useSuspenseQuery: catalog '${c}' referenced by the query is not attached. ` +
              `Pass it via the ServerDuckDBProvider 'databases' prop (or run ATTACH ` +
              `before mounting) so the catalog is ready before render.`,
          );
        }
      }

      const r: ArrowLikeResult = await withLease(connect, signal, (c) =>
        c.query(query),
      );
      const schema = r.schema;
      const cols = schema.fields.map((f) => f.name);

      const decimalFields = schema.fields
        .filter((f) => f.type.typeId === Type.Decimal)
        .map((f) => ({ name: f.name, divisor: 10 ** (f.type.scale ?? 0) }));

      const rows = r.toArray().map((row) => {
        const obj = row.toJSON();
        for (const { name, divisor } of decimalFields) {
          if (obj[name] != null) obj[name] = Number(obj[name]) / divisor;
        }
        return obj;
      });

      return { columns: cols, rows, schema };
    },
  }).data;
}
