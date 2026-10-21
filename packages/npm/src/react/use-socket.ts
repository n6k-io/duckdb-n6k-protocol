import type { RegisteredWebsocket } from "../create-n6k-worker";
import { useDuckDB } from "./use-duckdb";

export function useSocket(name: string): RegisteredWebsocket | null {
  return useDuckDB().registrations[name] ?? null;
}
