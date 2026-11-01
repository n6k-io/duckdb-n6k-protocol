import type { CatalogState } from "../../attach/catalog-states";

export type SocketsMount = {
  mode: "connect" | "socket";
  path: string;
  token: string;
  catalogs: string[];
  // Alias -> the server catalog it opens; an alias absent here opens its own name.
  serverCatalogs?: Record<string, string>;
};

declare global {
  var __n6kConfigure: ((m: SocketsMount) => void) | undefined;
  var __n6kReset: (() => void) | undefined;
  // Hands a fresh app-owned socket to a "socket"-mode mount.
  var __n6kSetSocket: (() => void) | undefined;
  var __n6kOpenedSockets: WebSocket[] | undefined;
  var __n6kStates: (() => Record<string, CatalogState>) | undefined;
  // Each catalog's statuses since the last configure, consecutive repeats collapsed.
  var __n6kHistory: Record<string, string[]> | undefined;
  var __n6kRead:
    | ((sql: string) => Promise<Array<Record<string, unknown>>>)
    | undefined;
  var __n6kEcho: ((x: number) => Promise<unknown>) | undefined;
}
