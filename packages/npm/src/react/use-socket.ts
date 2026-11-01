import type { RegisteredWebsocket } from "../create-n6k-worker";
import { useN6kSnapshot, type Subscribable } from "./use-n6k-snapshot";

export function useSocket(
  duckdb: Subscribable<{ registrations: Record<string, RegisteredWebsocket> }>,
  name: string,
): RegisteredWebsocket | null {
  return useN6kSnapshot(duckdb).registrations[name] ?? null;
}
