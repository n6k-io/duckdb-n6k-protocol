import { useQuery as useTanstackQuery } from "@tanstack/react-query";
import { useDuckDB } from "./use-duckdb";
import { statusOf, configFingerprint } from "./attachment";
import { withLease } from "../connection-shape";

export interface ColumnMeta {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
}

export interface TableMeta {
  columns: ColumnMeta[];
  primaryKey: string[];
  canInsert: boolean;
  // Row-level UPDATE/DELETE need a primary key, so an append-only table can insert but not edit.
  canEditRows: boolean;
  canModifyColumns: boolean;
  enumValues: Record<string, string[]>;
}

type TableMetaResult = {
  meta: TableMeta | null;
  status: "idle" | "loading" | "ready" | "error";
  error: string | null;
};

// `catalog`/`schema` may be omitted, resolving against the connection's current database/schema.
export interface TableMetaArgs {
  catalog?: string | null;
  schema?: string | null;
  table: string;
}

const EMPTY_META: TableMeta = {
  columns: [],
  primaryKey: [],
  canInsert: false,
  canEditRows: false,
  canModifyColumns: false,
  enumValues: {},
};

function sqlQuoteEscape(s: string): string {
  return s.replaceAll("'", "''");
}

// Normalize the wasm/native Arrow shapes (row proxies, Vectors, MapRows) into plain JS.
function toPlainObject(v: unknown): Record<string, unknown> {
  if (v && typeof (v as { toJSON?: unknown }).toJSON === "function") {
    return (v as { toJSON: () => Record<string, unknown> }).toJSON();
  }
  return (v ?? {}) as Record<string, unknown>;
}

function toIterableArray(v: unknown): unknown[] {
  if (v == null) return [];
  if (Array.isArray(v)) return v;
  if (
    typeof (v as { [Symbol.iterator]?: unknown })[Symbol.iterator] ===
    "function"
  ) {
    return [...(v as Iterable<unknown>)];
  }
  return [];
}

function toStringArray(v: unknown): string[] {
  if (typeof v === "string") return [v];
  return toIterableArray(v).map(String);
}

function toMapEntries(v: unknown): Array<[string, unknown]> {
  if (v == null) return [];
  if (v instanceof Map) {
    return [...v.entries()].map(([k, val]) => [String(k), val]);
  }
  // Arrow MapRow: iterable of {key, value} (or [key, value]) pairs.
  if (
    typeof (v as { [Symbol.iterator]?: unknown })[Symbol.iterator] ===
    "function"
  ) {
    return [...(v as Iterable<unknown>)].map((entry) => {
      if (Array.isArray(entry)) return [String(entry[0]), entry[1]];
      const o = entry as { key?: unknown; value?: unknown };
      return [String(o.key), o.value];
    });
  }
  if (typeof v === "object") {
    return Object.entries(v as Record<string, unknown>);
  }
  return [];
}

export function tableMetaFromDescribeRow(rawRow: unknown): TableMeta {
  const row = toPlainObject(rawRow);

  const columns: ColumnMeta[] = toIterableArray(row.columns).map((c) => {
    const o = toPlainObject(c);
    return {
      name: String(o.name),
      type: String(o.type),
      nullable: Boolean(o.nullable),
      default: o.default == null ? null : String(o.default),
    };
  });

  const primaryKey = toStringArray(row.primary_key);
  const verbs = toStringArray(row.verbs);
  const canInsert = verbs.includes("insert");
  const canModifyColumns = verbs.includes("alter");

  const enumValues: Record<string, string[]> = {};
  for (const [key, vals] of toMapEntries(row.enums)) {
    enumValues[key] = toStringArray(vals);
  }

  return {
    columns,
    primaryKey,
    canInsert,
    canEditRows: canInsert && primaryKey.length > 0,
    canModifyColumns,
    enumValues,
  };
}

export function useTableMeta(args: TableMetaArgs): TableMetaResult {
  const {
    conn,
    connect,
    status: dbStatus,
    desired,
    attached,
    errors,
    connStatus,
    invalidations,
  } = useDuckDB();
  const { catalog = null, schema = null, table } = args;

  // A managed catalog must finish attaching before describe, else n6k_table_describe fails with "Catalog does not exist".
  const attachState = { desired, attached, errors };
  const managed = catalog != null && desired[catalog] !== undefined;
  const attachBlocked = managed && statusOf(attachState, catalog) !== "ready";

  const enabled = dbStatus === "ready" && !!conn && !!table && !attachBlocked;

  const result = useTanstackQuery<TableMeta>({
    // Socket status, server invalidations and attach fingerprint are in the key so a reconnect, a server-side swap or a re-attach refetches the cached describe.
    queryKey: [
      "n6k-table-meta",
      catalog,
      schema,
      table,
      managed ? (connStatus[catalog] ?? null) : null,
      catalog == null ? 0 : (invalidations[catalog] ?? 0),
      managed && attached[catalog]
        ? configFingerprint(attached[catalog])
        : null,
    ],
    queryFn: ({ signal }) =>
      // ONE lease for both statements: current_database()/current_schema() are per-connection state.
      withLease(connect, signal, async (c) => {
        let catalogName = catalog ?? "";
        let schemaName = schema ?? "";
        if (!catalogName || !schemaName) {
          const ctxResult = await c.query(
            `SELECT current_database() AS db, current_schema() AS sch`,
          );
          const ctx = toPlainObject(ctxResult.toArray().at(0));
          if (!catalogName) {
            catalogName = ctx.db ? String(ctx.db) : "";
          }
          if (!schemaName) {
            schemaName = ctx.sch ? String(ctx.sch) : "";
          }
        }

        const descResult = await c.query(
          `SELECT columns, primary_key, verbs, enums FROM n6k_table_describe('${sqlQuoteEscape(catalogName)}', schema := '${sqlQuoteEscape(schemaName)}', "table" := '${sqlQuoteEscape(table)}')`,
        );
        const row = descResult.toArray().at(0);
        return row ? tableMetaFromDescribeRow(row) : EMPTY_META;
      }),
    enabled,
    staleTime: Infinity,
    gcTime: 5 * 60 * 1000,
  });

  if (!enabled) {
    return { meta: null, status: "idle", error: null };
  }

  if (result.status === "pending") {
    return { meta: null, status: "loading", error: null };
  }

  if (result.status === "error") {
    return {
      meta: null,
      status: "error",
      error: (result.error as Error).message,
    };
  }

  return { meta: result.data, status: "ready", error: null };
}
