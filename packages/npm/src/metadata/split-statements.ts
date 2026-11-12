import type { ConnectionLike } from "../connection-shape";

export type SplitStatement = {
  sql: string;
  statementType: string;
};

export async function splitStatements(
  conn: Pick<ConnectionLike, "query">,
  sql: string,
): Promise<SplitStatement[]> {
  const escaped = sql.replaceAll("'", "''");
  const result = await conn.query(
    `SELECT statement, statement_type FROM n6k_split_statements('${escaped}') ORDER BY ordinality`,
  );
  return result
    .toArray()
    .map((r: { toJSON: () => Record<string, unknown> }) => {
      const row = r.toJSON();
      return {
        sql: row.statement as string,
        statementType: row.statement_type as string,
      };
    });
}
