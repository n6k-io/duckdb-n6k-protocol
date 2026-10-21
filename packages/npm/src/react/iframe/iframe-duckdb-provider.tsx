// Iframe-side React shell. Must NOT import ../create-duckdb — that keeps this bundle wasm-free.

import {
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { tableFromIPC } from "apache-arrow";
import { DuckDBContext, type DuckDBContextValue } from "../duckdb-context";
import type { ArrowLikeResult } from "../../connection-shape";
import { createIframeClient } from "./iframe-client";
import { INITIAL_MIRROR, type N6kHandshake } from "./protocol";

const HELLO_RETRY_MS = 200;

export type IFrameDuckDBProviderProps = {
  children: ReactNode;
  // Exact parent origin, never "*".
  parentOrigin: string;
  queryTimeoutMs?: number;
};

export function IFrameDuckDBProvider({
  children,
  parentOrigin,
  queryTimeoutMs,
}: IFrameDuckDBProviderProps) {
  const [client] = useState(() =>
    createIframeClient({
      decodeIPC: (bytes) => tableFromIPC(bytes) as unknown as ArrowLikeResult,
      queryTimeoutMs,
    }),
  );

  const mirror = useSyncExternalStore(
    client.subscribe,
    client.getMirror,
    () => INITIAL_MIRROR,
  );

  useEffect(() => {
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

    window.addEventListener("message", onWindowMessage);

    // Fresh per-mount epoch: the only signal the parent has that we reloaded.
    const clientId = globalThis.crypto.randomUUID();

    // Announce and retry until the parent answers; keep the listener for later re-handshakes.
    const hello: N6kHandshake = { __n6k: "hello", clientId };
    window.parent.postMessage(hello, parentOrigin);
    const timer = setInterval(() => {
      if (connected) {
        clearInterval(timer);
        return;
      }
      window.parent.postMessage(hello, parentOrigin);
    }, HELLO_RETRY_MS);

    return () => {
      clearInterval(timer);
      window.removeEventListener("message", onWindowMessage);
    };
  }, [parentOrigin, client]);

  // Teardown on unmount, separate effect so a parentOrigin change doesn't tear it down.
  useEffect(
    () => () => client.teardown("IFrameDuckDBProvider unmounted"),
    [client],
  );

  const value = useMemo<DuckDBContextValue>(
    () => ({
      conn: client.conn,
      ...mirror,
      registrations: {},
      setDesired: client.setDesired,
      removeDesired: client.removeDesired,
      reconnect: client.reconnect,
      registerWebsocket: async () => {
        throw new Error("registerWebsocket: the parent window owns sockets");
      },
      replaceWebsocket: async () => {
        throw new Error("replaceWebsocket: the parent window owns sockets");
      },
      listCatalogs: async () => {
        throw new Error("listCatalogs: the parent window owns sockets");
      },
      connect: async () => client.conn,
    }),
    [client, mirror],
  );

  return (
    <DuckDBContext.Provider value={value}>{children}</DuckDBContext.Provider>
  );
}
