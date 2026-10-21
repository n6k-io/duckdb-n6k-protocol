import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { createDuckDB } from "../create-duckdb";
import type { CreateDuckDBOptions, WsStatus } from "../create-duckdb";
import type { RegisteredWebsocket } from "../create-n6k-worker";
import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { ConnectionLike } from "../connection-shape";
import {
  configEqual,
  makeConfig,
  type AttachConfig,
  type AttachError,
  type AttachOptions,
} from "./attachment";
import {
  createAttachReconciler,
  type AttachReconciler,
} from "./attach-reconciler";
import { DuckDBContext } from "./duckdb-context";
import { logger as log } from "../logger";
import { createWasmConnect, type WasmDatabase } from "../wasm-lease";
import {
  omitKey,
  useProviderSockets,
  validateSockets,
  type SocketSpec,
} from "./use-provider-sockets";

export type DatabaseSpec =
  | string
  | { path: string; options?: AttachOptions }
  | { socket: string; catalog?: string; options?: AttachOptions };

type DuckDBProviderProps = {
  children: React.ReactNode;
  sockets?: Record<string, SocketSpec>;
  databases?: Record<string, DatabaseSpec>;
  duckdbOptions?: CreateDuckDBOptions;
};

// Socket-backed entries carry no wsId here; the reconciler adds it at ATTACH time.
function specToConfig(spec: DatabaseSpec): AttachConfig {
  if (typeof spec === "string") return makeConfig(spec);
  if ("socket" in spec) {
    return makeConfig("", {
      ...spec.options,
      ...(spec.catalog === undefined ? {} : { catalog: spec.catalog }),
    });
  }
  return makeConfig(spec.path, spec.options);
}

function socketsOf(
  databases: Record<string, DatabaseSpec> | undefined,
  sockets: Record<string, SocketSpec> | undefined,
): Record<string, string> {
  validateSockets(sockets);
  const out: Record<string, string> = {};
  for (const [alias, spec] of Object.entries(databases ?? {})) {
    if (typeof spec === "string" || !("socket" in spec)) continue;
    if (!sockets || !(spec.socket in sockets)) {
      throw new Error(
        `n6k: databases.${alias} references unknown socket '${spec.socket}'`,
      );
    }
    const keys = new Set(
      Object.keys(spec.options ?? {}).map((k) => k.toLowerCase()),
    );
    if (keys.has("token") || keys.has("wsid")) {
      throw new Error(
        `n6k: databases.${alias} rides socket '${spec.socket}'; set the token on the socket, not 'token'/'wsId' options`,
      );
    }
    out[alias] = spec.socket;
  }
  return out;
}

function specsToConfigs(
  specs: Record<string, DatabaseSpec> | undefined,
): Record<string, AttachConfig> {
  if (!specs) return {};
  return Object.fromEntries(
    Object.entries(specs).map(([k, v]) => [k, specToConfig(v)]),
  );
}

export function DuckDBProvider({
  children,
  sockets,
  databases,
  duckdbOptions,
}: DuckDBProviderProps) {
  const [socketOf] = useState(() => socketsOf(databases, sockets));
  const [conn, setConn] = useState<AsyncDuckDBConnection | null>(null);
  const [status, setStatus] = useState("initializing");
  const [error, setError] = useState<string | null>(null);
  const [connStatus, setConnStatus] = useState<Record<string, WsStatus>>({});
  const [invalidations, setInvalidations] = useState<Record<string, number>>(
    {},
  );
  const reconnectRef = useRef<(catalog: string) => void>(() => {});
  const dbRef = useRef<AsyncDuckDB | null>(null);
  const connectRef = useRef<(() => Promise<ConnectionLike>) | null>(null);
  const registerWebsocketRef = useRef<
    (socket: WebSocket, token: string) => Promise<RegisteredWebsocket>
  >(async () => {
    throw new Error(
      "n6k: registerWebsocket() was called before the DuckDB provider was ready. " +
        "Gate it on useDuckDB().status === 'ready' (the WebSocket API is wired up " +
        "only once the worker has started and the n6k extension has loaded).",
    );
  });
  const replaceWebsocketRef = useRef<
    (wsId: string, socket: WebSocket, token: string) => Promise<void>
  >(async () => {
    throw new Error(
      "n6k: replaceWebsocket() was called before the DuckDB provider was ready. " +
        "Gate it on useDuckDB().status === 'ready'.",
    );
  });
  const listCatalogsRef = useRef<(wsId: string) => Promise<string[]>>(
    async () => {
      throw new Error(
        "n6k: listCatalogs() was called before the DuckDB provider was ready. " +
          "Gate it on useDuckDB().status === 'ready'.",
      );
    },
  );
  const [desired, setDesiredState] = useState<Record<string, AttachConfig>>(
    () => specsToConfigs(databases),
  );
  const [attached, setAttached] = useState<Record<string, AttachConfig>>({});
  const [errors, setErrors] = useState<Record<string, AttachError>>({});
  const initRef = useRef(false);

  // Pre-attach SQL kept out of React state: it can carry a secret token, so it must never land in context-exposed `desired`/`attached`.
  const setupRef = useRef<Record<string, string[]>>({});

  const setDesired = useCallback(
    (catalog: string, config: AttachConfig, setup?: string[]) => {
      if (setup && setup.length > 0) {
        setupRef.current[catalog] = setup;
      } else {
        delete setupRef.current[catalog];
      }
      setDesiredState((prev) => {
        const existing = prev[catalog];
        if (existing && configEqual(existing, config)) return prev;
        return { ...prev, [catalog]: config };
      });
    },
    [],
  );

  const removeDesired = useCallback((catalog: string) => {
    delete setupRef.current[catalog];
    setDesiredState((prev) => omitKey(prev, catalog));
  }, []);

  const reconnect = useCallback(
    (catalog: string) => reconnectRef.current(catalog),
    [],
  );

  const connect = useCallback(async (): Promise<ConnectionLike> => {
    const lease = connectRef.current;
    if (!lease)
      throw new Error("duckdb not ready: connect() before database open");
    return lease();
  }, []);

  const registerWebsocket = useCallback(
    (socket: WebSocket, token: string) =>
      registerWebsocketRef.current(socket, token),
    [],
  );

  const replaceWebsocket = useCallback(
    (wsId: string, socket: WebSocket, token: string) =>
      replaceWebsocketRef.current(wsId, socket, token),
    [],
  );

  const listCatalogs = useCallback(
    (wsId: string) => listCatalogsRef.current(wsId),
    [],
  );

  const { registrations, socketErrors } = useProviderSockets(
    sockets,
    status === "ready",
    registerWebsocket,
    replaceWebsocket,
  );

  const visibleErrors = useMemo(() => {
    let out = errors;
    for (const [alias, name] of Object.entries(socketOf)) {
      const message = socketErrors[name];
      const config = desired[alias];
      if (message === undefined || config === undefined) continue;
      out = { ...out, [alias]: { config, message } };
    }
    return out;
  }, [errors, socketErrors, socketOf, desired]);

  const dropAttached = (c: string) => setAttached((prev) => omitKey(prev, c));
  const dropError = (c: string) => setErrors((prev) => omitKey(prev, c));

  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;

    createDuckDB({
      ...duckdbOptions,
      onStatus: (catalog, ws) =>
        setConnStatus((prev) => ({ ...prev, [catalog]: ws })),
      onInvalidated: (catalog, schemas) => {
        setInvalidations((prev) => ({
          ...prev,
          [catalog]: (prev[catalog] ?? 0) + 1,
        }));
        duckdbOptions?.onInvalidated?.(catalog, schemas);
      },
    })
      .then(
        async ({
          db,
          conn,
          reconnect,
          registerWebsocket,
          replaceWebsocket,
          listCatalogs,
        }) => {
          dbRef.current = db;
          connectRef.current = createWasmConnect(db as unknown as WasmDatabase);
          reconnectRef.current = reconnect;
          registerWebsocketRef.current = registerWebsocket;
          replaceWebsocketRef.current = replaceWebsocket;
          listCatalogsRef.current = listCatalogs;
          setConn(conn);
          setStatus("loading-extensions");
          log.debug("loading n6k extension...");
          await conn.query("LOAD n6k_client;");
          log.debug("n6k extension loaded");
          setStatus("ready");
          log.debug("ready");
        },
      )
      .catch((error_: unknown) => {
        log.error(error_);
        setError((error_ as Error).message);
        setStatus("error");
      });
  }, [duckdbOptions]);

  const reconcilerRef = useRef<AttachReconciler | null>(null);
  useEffect(() => {
    if (status !== "ready" || !conn) return;
    const reconciler = createAttachReconciler(conn, {
      setup: (c) => setupRef.current[c] ?? [],
      onAttached: (c, config) => {
        setAttached((prev) => ({ ...prev, [c]: config }));
        dropError(c);
      },
      onDetached: (c) => {
        dropAttached(c);
        setConnStatus((prev) => omitKey(prev, c));
      },
      onError: (c, config, message) =>
        setErrors((prev) => ({ ...prev, [c]: { config, message } })),
    });
    reconcilerRef.current = reconciler;
    return () => {
      reconciler.stop();
      reconcilerRef.current = null;
    };
  }, [conn, status]);

  useEffect(() => {
    void reconcilerRef.current?.run({ desired, socketOf, registrations });
  }, [desired, conn, status, socketOf, registrations]);

  return (
    <DuckDBContext.Provider
      value={{
        conn,
        status,
        error,
        connStatus,
        invalidations,
        reconnect,
        registerWebsocket,
        replaceWebsocket,
        listCatalogs,
        desired,
        attached,
        errors: visibleErrors,
        registrations,
        setDesired,
        removeDesired,
        connect,
      }}
    >
      {children}
    </DuckDBContext.Provider>
  );
}
