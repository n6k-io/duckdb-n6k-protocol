import type { CatalogState } from "../../attach/catalog-states";

export type SocketsMount = {
  path: string;
  token: string;
  catalogs: string[];
  // Alias -> the server catalog it opens; an alias absent here opens its own name.
  serverCatalogs?: Record<string, string>;
};

declare global {
  // Builds a fresh N6kDuckDB for the mount and resolves once connect() does.
  var __n6kConfigure: ((m: SocketsMount) => Promise<void>) | undefined;
  var __n6kReset: (() => Promise<void>) | undefined;
  var __n6kOpenedSockets: WebSocket[] | undefined;
  var __n6kStates: (() => Record<string, CatalogState>) | undefined;
  var __n6kRead:
    | ((sql: string) => Promise<Array<Record<string, unknown>>>)
    | undefined;
  var __n6kEcho: ((x: number) => Promise<unknown>) | undefined;
}
