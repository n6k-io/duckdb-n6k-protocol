import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { useDuckDB } from "./use-duckdb";
import {
  parseSqlTables,
  stringToTableRef,
  catalogOf,
  type TableRef,
} from "./parse-sql-tables";
import { statusOf, errorOf } from "./attachment";
import { withLease } from "../connection-shape";

export type CatalogRef = string | { catalog: string };

export type CatalogGateOptions = {
  tables?: Array<string | TableRef>;
  catalogs?: CatalogRef[];
};

export type CatalogGate = {
  tables: TableRef[];
  managed: string[];
  parseReady: boolean;
  parseError: Error | null;
  depsReady: boolean;
  depError: string | null;
  anyPending: boolean;
  disconnected: boolean;
  reconnecting: boolean;
};

export function useCatalogGate(
  query: string,
  options?: CatalogGateOptions,
): CatalogGate {
  const {
    conn,
    connect,
    status: dbStatus,
    desired,
    attached,
    errors,
    connStatus,
  } = useDuckDB();
  const hasExplicitTables = !!options?.tables;

  const parse = useTanstackQuery<TableRef[]>({
    queryKey: ["n6k-parse", query],
    queryFn: ({ signal }) =>
      withLease(connect, signal, (c) => parseSqlTables(c, query)),
    enabled: dbStatus === "ready" && !!conn && !!query && !hasExplicitTables,
    staleTime: Infinity,
  });

  const tables: TableRef[] = hasExplicitTables
    ? options!.tables!.map((t) =>
        typeof t === "string" ? stringToTableRef(t) : t,
      )
    : (parse.data ?? []);

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

  const disconnected = managed.some((c) => connStatus[c] === "disconnected");
  const reconnecting = managed.some((c) => connStatus[c] === "reconnecting");
  const attachState = { desired, attached, errors };
  const catalogStates = managed.map((c) => {
    const s = statusOf(attachState, c);
    if (s !== undefined) return s;
    return explicitCatalogs.includes(c) ? "pending" : "ready";
  });

  const erroredCatalog = managed.find(
    (c) => statusOf(attachState, c) === "error",
  );
  let depError: string | null = null;
  if (catalogStates.includes("error")) {
    depError = erroredCatalog
      ? `Attach ${erroredCatalog} failed: ${errorOf(attachState, erroredCatalog) ?? "unknown error"}`
      : "Attach failed";
  }

  return {
    tables,
    managed,
    parseReady: hasExplicitTables || parse.isSuccess,
    parseError:
      !hasExplicitTables && parse.status === "error"
        ? (parse.error as Error)
        : null,
    depsReady: catalogStates.every((s) => s === "ready"),
    depError,
    anyPending: catalogStates.includes("pending"),
    disconnected,
    reconnecting,
  };
}
