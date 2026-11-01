import type { ConnectionLike } from "../connection-shape";

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
  // `canInsert` plus a primary key (row-level UPDATE/DELETE need one); the `update`/`delete` verbs are not consulted.
  canEditRows: boolean;
  canModifyColumns: boolean;
  enumValues: Record<string, string[]>;
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

// This and the to* helpers below normalize wasm/native Arrow values (row proxies, Vectors, MapRows) into plain JS.
export function toPlainObject(v: unknown): Record<string, unknown> {
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

export type TableIdent = { catalog: string; schema: string; table: string };

export async function describeN6kTable(
  conn: ConnectionLike,
  { catalog, schema, table }: TableIdent,
): Promise<TableMeta> {
  const result = await conn.query(
    `SELECT columns, primary_key, verbs, enums FROM n6k_table_describe('${sqlQuoteEscape(catalog)}', schema := '${sqlQuoteEscape(schema)}', "table" := '${sqlQuoteEscape(table)}')`,
  );
  const row = result.toArray().at(0);
  return row ? tableMetaFromDescribeRow(row) : EMPTY_META;
}
