// Iframe side. Must NOT import ../create-duckdb — that keeps this bundle wasm-free.

import { tableFromIPC } from "apache-arrow";
import type { AttachConfig } from "../attach/attachment";
import {
  createCatalogStore,
  type CatalogStore,
} from "../attach/catalog-states";
import type { ArrowLikeResult, ConnectionLike } from "../connection-shape";
import { parseSqlTables, type TableRef } from "../metadata/parse-sql-tables";
import {
  describeN6kTable,
  type TableIdent,
  type TableMeta,
} from "../metadata/table-describe";
import { createIframeClient } from "./iframe-client";
import type { Mirror, N6kHandshake } from "./protocol";

const HELLO_RETRY_MS = 200;

export type IframeN6kOptions = {
  // Exact parent origin. "*" never matches an incoming port, so the iframe never connects, and
  // hello is still posted to any origin.
  parentOrigin: string;
  queryTimeoutMs?: number;
};

export type IframeN6k = {
  connect: () => Promise<ConnectionLike>;
  metadataFns: {
    tablesOf(conn: ConnectionLike, sql: string): Promise<TableRef[]>;
    tableMeta(conn: ConnectionLike, ref: TableIdent): Promise<TableMeta>;
    catalogs: CatalogStore;
  };
  subscribe: (onChange: () => void) => () => void;
  getSnapshot: () => Mirror;
  setDesired: (catalog: string, config: AttachConfig) => void;
  removeDesired: (catalog: string) => void;
  reconnect: (catalog: string) => void;
  dispose: () => void;
};

export function createIframeN6k({
  parentOrigin,
  queryTimeoutMs,
}: IframeN6kOptions): IframeN6k {
  const client = createIframeClient({
    decodeIPC: (bytes) => tableFromIPC(bytes) as unknown as ArrowLikeResult,
    queryTimeoutMs,
  });

  let connected = false;
  function onWindowMessage(e: MessageEvent): void {
    if (e.origin !== parentOrigin) return;
    const data = e.data as N6kHandshake | undefined;
    if (!data || data.__n6k !== "port") return;
    const port = e.ports[0];
    if (!port) return;
    // Accept a fresh port at any time so a re-handshaking parent re-links.
    connected = true;
    client.attachPort(port);
  }

  // Fresh per-instance epoch: the only signal the parent has that we reloaded.
  const hello: N6kHandshake = {
    __n6k: "hello",
    clientId: globalThis.crypto.randomUUID(),
  };
  let timer: ReturnType<typeof setInterval> | undefined;
  if (globalThis.window !== undefined) {
    globalThis.window.addEventListener("message", onWindowMessage);
    globalThis.window.parent.postMessage(hello, parentOrigin);
    timer = setInterval(() => {
      if (connected) {
        clearInterval(timer);
        return;
      }
      globalThis.window.parent.postMessage(hello, parentOrigin);
    }, HELLO_RETRY_MS);
  }

  return {
    connect: async () => client.conn,
    metadataFns: {
      tablesOf: parseSqlTables,
      tableMeta: describeN6kTable,
      catalogs: createCatalogStore(client.subscribe, client.getMirror),
    },
    subscribe: client.subscribe,
    getSnapshot: client.getMirror,
    setDesired: client.setDesired,
    removeDesired: client.removeDesired,
    reconnect: client.reconnect,
    dispose() {
      clearInterval(timer);
      if (globalThis.window !== undefined) {
        globalThis.window.removeEventListener("message", onWindowMessage);
      }
      client.teardown("iframe n6k disposed");
    },
  };
}
