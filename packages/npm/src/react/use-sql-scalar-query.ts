import { useSQLQuery } from "./use-sql-query";

export type ScalarQueryResult = {
  raw: unknown;
  status: "idle" | "loading" | "ready" | "error";
  failureMessage: string | null;
};

export function useSQLScalarQuery(sql: string): ScalarQueryResult {
  const { rows, status, error } = useSQLQuery(sql);
  const first = status === "ready" ? rows[0] : undefined;
  const raw = first ? (Object.values(first)[0] ?? null) : null;
  return { raw, status, failureMessage: error };
}
