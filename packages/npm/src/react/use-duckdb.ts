import { useContext } from "react";
import { DuckDBContext } from "./duckdb-context";

export function useDuckDB() {
  const ctx = useContext(DuckDBContext);
  if (ctx === null) {
    throw new Error("useDuckDB must be used within a <DuckDBProvider>");
  }
  return ctx;
}
