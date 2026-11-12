import { borrow, type ConnectionLike } from "./connection-shape";
import { parseSqlTables } from "./metadata/parse-sql-tables";
import { splitStatements } from "./metadata/split-statements";
import { describeN6kTable } from "./metadata/table-describe";

// SSR/native: one connection the caller owns, its catalogs already attached, so there is no catalog store.
export function serverN6k(conn: ConnectionLike) {
  return {
    connect: async () => borrow(conn),
    metadataFns: {
      tablesOf: parseSqlTables,
      tableMeta: describeN6kTable,
      splitStatements,
    },
  };
}
