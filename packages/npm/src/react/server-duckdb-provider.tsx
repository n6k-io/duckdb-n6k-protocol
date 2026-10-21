import type { ReactNode } from "react";
import { DuckDBContext } from "./duckdb-context";
import type { RegisteredWebsocket } from "../create-n6k-worker";
import { borrow, type ConnectionLike } from "../connection-shape";
import {
  makeConfig,
  type AttachConfig,
  type AttachOptions,
} from "./attachment";

export type ServerDatabaseSpec =
  | string
  | { path: string; options?: AttachOptions };

export type ServerDuckDBProviderProps = {
  children: ReactNode;
  conn: ConnectionLike;
  databases?: Record<string, ServerDatabaseSpec>;
};

function specToConfig(spec: ServerDatabaseSpec): AttachConfig {
  if (typeof spec === "string") return makeConfig(spec);
  return makeConfig(spec.path, spec.options);
}

function specsToConfigs(
  specs: Record<string, ServerDatabaseSpec> | undefined,
): Record<string, AttachConfig> {
  if (!specs) return {};
  return Object.fromEntries(
    Object.entries(specs).map(([k, v]) => [k, specToConfig(v)]),
  );
}

function noopSetDesired(): void {}

function noopRemoveDesired(): void {}

function noopReconnect(): void {}

async function noopRegisterWebsocket(): Promise<RegisteredWebsocket> {
  return {
    wsId: "",
    capabilities: [],
    get rpc(): never {
      throw new Error("registerWebsocket: no JSON-RPC on the server");
    },
    close: () => {},
  };
}

async function noopReplaceWebsocket(): Promise<void> {}

async function noopListCatalogs(): Promise<string[]> {
  return [];
}

export function ServerDuckDBProvider({
  children,
  conn,
  databases,
}: ServerDuckDBProviderProps) {
  const configs = specsToConfigs(databases);
  return (
    <DuckDBContext.Provider
      value={{
        conn,
        status: "ready",
        error: null,
        connStatus: {},
        invalidations: {},
        reconnect: noopReconnect,
        registerWebsocket: noopRegisterWebsocket,
        replaceWebsocket: noopReplaceWebsocket,
        listCatalogs: noopListCatalogs,
        desired: configs,
        attached: configs,
        errors: {},
        registrations: {},
        setDesired: noopSetDesired,
        removeDesired: noopRemoveDesired,
        connect: async () => borrow(conn),
      }}
    >
      {children}
    </DuckDBContext.Provider>
  );
}
