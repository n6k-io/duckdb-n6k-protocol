import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useDuckDB } from "./use-duckdb";
import {
  parseSqlTables,
  stringToTableRef,
  tableRefsOverlap,
  catalogOf,
  WRITE_PRIVILEGES,
  DDL_PRIVILEGES,
  type TableRef,
} from "./parse-sql-tables";

type ExecOptions = {
  invalidate?: Array<string | TableRef> | false;
};

type ExecResult = {
  tables: TableRef[];
  // True when the caller supplied an explicit `invalidate` list; false for auto-parsed SQL.
  explicit: boolean;
};

export function useExec() {
  const { conn, connect, connStatus } = useDuckDB();
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: async ({
      sql,
      options,
    }: {
      sql: string;
      options?: ExecOptions;
    }): Promise<ExecResult> => {
      if (!conn) throw new Error("DuckDB not connected");

      // ONE lease for the whole mutation: parse and the write it authorizes must run on the same connection.
      const c = await connect();
      try {
        // Parsing runs in the local wasm parser, safe even while a remote catalog is down; skip the pre-flight check if it can't parse.
        let parsed: TableRef[] | null;
        try {
          parsed = await parseSqlTables(c, sql);
        } catch {
          parsed = null;
        }

        // Fail fast on a dropped socket for the catalogs this statement targets; writes are never auto-replayed.
        if (parsed) {
          const targets = parsed
            .map((t) => catalogOf(t))
            .filter((c2): c2 is string => c2 !== null);
          if (targets.some((c2) => connStatus[c2] === "disconnected"))
            throw new Error(
              "DuckDB is disconnected — reconnect before running this statement",
            );
        }

        await c.query(sql);

        if (options?.invalidate === false)
          return { tables: [], explicit: true };
        if (options?.invalidate) {
          return {
            tables: options.invalidate.map((t) =>
              typeof t === "string" ? stringToTableRef(t) : t,
            ),
            explicit: true,
          };
        }
        return {
          tables: parsed ?? (await parseSqlTables(c, sql)),
          explicit: false,
        };
      } finally {
        await c.close();
      }
    },
    onSuccess: ({ tables, explicit }) => {
      if (tables.length === 0) return;

      // For auto-parsed SQL only writes invalidate cached reads; an explicit list is an intentional override.
      const invalidators = explicit
        ? tables
        : tables.filter((t) => WRITE_PRIVILEGES.has(t.privilege));

      if (invalidators.length > 0) {
        queryClient.invalidateQueries({
          predicate: (query) => {
            if (query.queryKey[0] !== "n6k-query") return false;
            const data = query.state.data as
              | { tables?: TableRef[] }
              | undefined;
            if (!data?.tables) return true;
            return tableRefsOverlap(invalidators, data.tables);
          },
        });
      }

      // DDL changes a table's shape/permissions, so its cached metadata must be refreshed.
      const metaTargets = explicit
        ? tables
        : tables.filter((t) => DDL_PRIVILEGES.has(t.privilege));

      if (metaTargets.length > 0) {
        queryClient.invalidateQueries({
          predicate: (query) => {
            const key = query.queryKey;
            if (key[0] !== "n6k-table-meta") return false;
            // Key shape: ["n6k-table-meta", catalog, schema, table].
            const ref: TableRef = {
              catalog: (key[1] as string | null) ?? null,
              schema: (key[2] as string | null) ?? null,
              table: key[3] as string,
              refType: "base_table",
              privilege: "select",
            };
            return tableRefsOverlap(metaTargets, [ref]);
          },
        });
      }
    },
  });

  return {
    exec: (sql: string, options?: ExecOptions) =>
      mutation.mutateAsync({ sql, options }),
    status: mutation.status,
    error: mutation.error?.message ?? null,
    // Coarse: true when any attached catalog's socket is down; per-attach precision is in useAttach(catalog).status.
    disconnected: Object.values(connStatus).includes("disconnected"),
  };
}
