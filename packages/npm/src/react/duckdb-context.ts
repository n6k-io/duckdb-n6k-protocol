import { createContext } from "react";
import type { ConnectionLike } from "../connection-shape";
import type { RegisteredWebsocket } from "../create-n6k-worker";
import type { WsStatus } from "../types";
import type { AttachConfig, AttachError } from "./attachment";

export type DuckDBContextValue = {
  conn: ConnectionLike | null;
  status: string;
  error: string | null;
  connStatus: Record<string, WsStatus>;
  // Per catalog, how many times the server invalidated it; in query keys so cached reads refetch.
  invalidations: Record<string, number>;
  reconnect: (catalog: string) => void;
  registerWebsocket: (
    socket: WebSocket,
    token: string,
  ) => Promise<RegisteredWebsocket>;
  replaceWebsocket: (
    wsId: string,
    socket: WebSocket,
    token: string,
  ) => Promise<void>;
  listCatalogs: (wsId: string) => Promise<string[]>;
  desired: Record<string, AttachConfig>;
  attached: Record<string, AttachConfig>;
  errors: Record<string, AttachError>;
  // Registered `sockets` by name; a name is absent until it registers.
  registrations: Record<string, RegisteredWebsocket>;
  // `setup` runs before this catalog's ATTACH in the same reconcile step; a setup-only change does not re-attach.
  setDesired: (catalog: string, config: AttachConfig, setup?: string[]) => void;
  removeDesired: (catalog: string) => void;
  // Lease a connection per read: a DuckDB connection holds one query slot, so shared reads collide (loser returns zero rows). Throws until ready.
  connect: () => Promise<ConnectionLike>;
};

export const DuckDBContext = createContext<DuckDBContextValue | null>(null);
