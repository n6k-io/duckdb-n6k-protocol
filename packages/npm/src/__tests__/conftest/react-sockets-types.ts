import type { RegisteredWebsocket } from "../../create-n6k-worker";

export type SocketsMount = {
  mode: "connect" | "socket";
  path: string;
  token: string;
  catalogs: string[];
  // Alias -> the server catalog it opens; an alias absent here opens its own name.
  serverCatalogs?: Record<string, string>;
  sql: string;
};

export type SocketsProbeState = {
  status: string;
  statusInfo: string | null;
  error: string | null;
};

export type SocketsProbe = SocketsProbeState & {
  history: SocketsProbeState[];
  rows: Array<Record<string, unknown>>;
};

declare global {
  var __n6kMountSockets: ((m: SocketsMount) => void) | undefined;
  var __n6kUnmountSockets: (() => void) | undefined;
  var __n6kSocketsProbe: SocketsProbe | undefined;
  var __n6kSetSocket: (() => void) | undefined;
  var __n6kOpenedSockets: WebSocket[] | undefined;
  var __n6kSocketReg: RegisteredWebsocket | undefined;
}
