import type { WsStatus } from "../types";
import {
  configFingerprint,
  errorOf,
  statusOf,
  type AttachState,
} from "./attachment";

export type CatalogState =
  | {
      status: "ready" | "pending" | "disconnected" | "reconnecting";
      version: string;
    }
  | { status: "error"; message: string; version: string };

export type CatalogStore = {
  subscribe(onChange: () => void): () => void;
  getSnapshot(): Readonly<Record<string, CatalogState>>;
};

export type CatalogInputs = AttachState & {
  connStatus: Record<string, WsStatus>;
  invalidations: Record<string, number>;
};

// `version` changes on a server invalidation, a re-attach with a different config, or a socket status change.
export function catalogStatesOf(
  s: CatalogInputs,
): Record<string, CatalogState> {
  const out: Record<string, CatalogState> = {};
  for (const c of Object.keys(s.desired)) {
    const have = s.attached[c];
    const conn = s.connStatus[c];
    const version = `${s.invalidations[c] ?? 0}|${have ? configFingerprint(have) : ""}|${conn ?? ""}`;
    const attach = statusOf(s, c);
    if (attach === "error") {
      out[c] = {
        status: "error",
        message: errorOf(s, c) ?? "unknown error",
        version,
      };
    } else if (attach === "pending") {
      out[c] = { status: "pending", version };
    } else if (conn === "disconnected" || conn === "reconnecting") {
      out[c] = { status: conn, version };
    } else {
      out[c] = { status: "ready", version };
    }
  }
  return out;
}

// Memoized on input identity so getSnapshot is stable between changes, as useSyncExternalStore requires.
export function createCatalogStore(
  subscribe: (fn: () => void) => () => void,
  inputs: () => CatalogInputs,
): CatalogStore {
  let last: CatalogInputs | null = null;
  let states: Record<string, CatalogState> = {};
  return {
    subscribe,
    getSnapshot() {
      const now = inputs();
      if (now !== last) {
        last = now;
        states = catalogStatesOf(now);
      }
      return states;
    },
  };
}
