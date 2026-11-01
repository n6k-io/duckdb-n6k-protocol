// Wire protocol for the iframe <-> parent DuckDB bridge; every payload is structured-clone-safe.

import type { AttachConfig } from "../attach/attachment";
import type { N6kDuckDBSnapshot } from "../n6k-duckdb";

// The slice of provider state the parent mirrors to the iframe.
export type Mirror = Pick<
  N6kDuckDBSnapshot,
  | "status"
  | "error"
  | "connStatus"
  | "invalidations"
  | "desired"
  | "attached"
  | "errors"
>;

// Seed before the parent's first push: not "ready", so hooks wait rather than query.
export const INITIAL_MIRROR: Mirror = {
  status: "initializing",
  error: null,
  connStatus: {},
  invalidations: {},
  desired: {},
  attached: {},
  errors: {},
};

export type ClientMsg =
  | { __n6k: "query"; id: number; sql: string }
  | { __n6k: "setDesired"; catalog: string; config: AttachConfig }
  | { __n6k: "removeDesired"; catalog: string }
  | { __n6k: "reconnect"; catalog: string };

export type HostMsg =
  | ({ __n6k: "state" } & Mirror)
  | { __n6k: "queryOk"; id: number; ipc: Uint8Array }
  | { __n6k: "queryErr"; id: number; message: string };

export type N6kMsg = ClientMsg | HostMsg;

// Window handshake; clientId is a per-mount epoch — the only reliable reload signal.
export type N6kHandshake =
  | { __n6k: "hello"; clientId: string }
  | { __n6k: "port" };
