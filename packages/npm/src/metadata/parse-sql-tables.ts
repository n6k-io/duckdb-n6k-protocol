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
