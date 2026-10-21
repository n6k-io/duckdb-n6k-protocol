import type { ConnectionLike } from "../connection-shape";

export type TableRefType = "base_table" | "table_function";

export type PrivilegeType =
  | "select"
  | "insert"
  | "update"
  | "delete"
  | "create"
  | "drop"
  | "alter";

export type TableRef = {
  catalog: string | null;
  schema: string | null;
  table: string;
  refType: TableRefType;
  privilege: PrivilegeType;
};

export async function parseSqlTables(
  conn: ConnectionLike,
  sql: string,
): Promise<TableRef[]> {
  const escaped = sql.replaceAll("'", "''");
  const result = await conn.query(
    `SELECT catalog, schema, table_name, ref_type, privilege_type FROM n6k_parse_sql_get_tables('${escaped}')`,
  );
  return result
    .toArray()
    .map((r: { toJSON: () => Record<string, unknown> }) => {
      const row = r.toJSON();
      return {
        catalog: (row.catalog as string | null) ?? null,
        schema: (row.schema as string | null) ?? null,
        table: row.table_name as string,
        refType: row.ref_type as TableRefType,
        privilege: row.privilege_type as PrivilegeType,
      };
    });
}

// Ambiguous prefix (catalog=NULL, schema=X): C++ couldn't tell if X is a catalog or schema, so X matches either slot.
function isAmbiguousPrefix(r: TableRef): boolean {
  return r.catalog === null && r.schema !== null;
}

function slotMatch(a: string | null, b: string | null): boolean {
  return a === null || b === null || a === b;
}

function refsMatch(a: TableRef, b: TableRef): boolean {
  if (a.table !== b.table) return false;

  const aAmb = isAmbiguousPrefix(a);
  const bAmb = isAmbiguousPrefix(b);

  if (aAmb && bAmb) {
    return a.schema === b.schema;
  }
  if (aAmb) {
    return slotMatch(a.schema, b.catalog) || slotMatch(a.schema, b.schema);
  }
  if (bAmb) {
    return slotMatch(b.schema, a.catalog) || slotMatch(b.schema, a.schema);
  }
  return slotMatch(a.catalog, b.catalog) && slotMatch(a.schema, b.schema);
}

export function tableRefsOverlap(a: TableRef[], b: TableRef[]): boolean {
  return a.some((x) => b.some((y) => refsMatch(x, y)));
}

export function stringToTableRef(name: string): TableRef {
  return {
    catalog: null,
    schema: null,
    table: name,
    refType: "base_table",
    privilege: "select",
  };
}

// An ambiguous 2-part prefix treats schema X as the catalog.
export function catalogOf(t: TableRef): string | null {
  if (t.catalog) return t.catalog;
  if (t.schema) return t.schema;
  return null;
}

export const WRITE_PRIVILEGES: ReadonlySet<PrivilegeType> = new Set([
  "insert",
  "update",
  "delete",
  "create",
  "drop",
  "alter",
]);

export const DDL_PRIVILEGES: ReadonlySet<PrivilegeType> = new Set([
  "create",
  "drop",
  "alter",
]);
