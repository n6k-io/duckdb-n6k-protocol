import { useEffect, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { QueryProvider } from "../../react/query-provider";
import { DuckDBProvider } from "../../react/duckdb-provider";
import { useQuery } from "../../react/use-sql-query";
import { useSocket } from "../../react/use-socket";
import type { SocketSpec } from "../../react/use-provider-sockets";
import type { SocketsMount, SocketsProbe } from "./react-sockets-types";

const duckdbOptions = {
  bundle: {
    mainModule: "/duckdb/duckdb-coi.wasm",
    mainWorker: "/duckdb/duckdb-browser-coi.worker.js",
    pthreadWorker: "/duckdb/duckdb-browser-coi.pthread.worker.js",
  },
  maximumThreads: 1,
  extensionRepository: globalThis.location.origin,
};

function wsBase(): string {
  const serverUrl = globalThis.__n6kServerUrl ?? "n6k://localhost:8099";
  try {
    return `ws://${new URL(serverUrl.replace(/^n6k:/, "ws:")).host}`;
  } catch {
    return "ws://localhost:8099";
  }
}

function probe(): SocketsProbe {
  return (globalThis.__n6kSocketsProbe ??= {
    history: [],
    status: "",
    statusInfo: null,
    rows: [],
    error: null,
  });
}

function Probe({ sql }: { sql: string }) {
  const r = useQuery(sql);
  useEffect(() => {
    const p = probe();
    const last = p.history.at(-1);
    if (
      last?.status !== r.status ||
      last.statusInfo !== r.statusInfo ||
      last.error !== r.error
    ) {
      p.history.push({
        status: r.status,
        statusInfo: r.statusInfo,
        error: r.error,
      });
    }
    p.status = r.status;
    p.statusInfo = r.statusInfo;
    p.rows = r.rows;
    p.error = r.error;
  });
  return null;
}

function RpcProbe() {
  const reg = useSocket("main");
  useEffect(() => {
    globalThis.__n6kSocketReg = reg ?? undefined;
  }, [reg]);
  return null;
}

function App({ m }: { m: SocketsMount }) {
  const url = `${wsBase()}${m.path}`;
  const [ws, setWs] = useState<WebSocket | null>(null);
  useEffect(() => {
    globalThis.__n6kSetSocket = () => setWs(new WebSocket(url));
  }, [url]);
  const main: SocketSpec =
    m.mode === "connect"
      ? {
          connect: () => {
            const s = new WebSocket(url);
            (globalThis.__n6kOpenedSockets ??= []).push(s);
            return s;
          },
          token: m.token,
        }
      : { socket: ws, token: m.token };
  return (
    <DuckDBProvider
      sockets={{ main }}
      databases={Object.fromEntries(
        m.catalogs.map((c) => [
          c,
          { socket: "main", catalog: m.serverCatalogs?.[c] },
        ]),
      )}
      duckdbOptions={duckdbOptions}
    >
      <Probe sql={m.sql} />
      <RpcProbe />
    </DuckDBProvider>
  );
}

let root: Root | undefined;

globalThis.__n6kMountSockets = (m: SocketsMount) => {
  root?.unmount();
  globalThis.__n6kSocketsProbe = undefined;
  globalThis.__n6kOpenedSockets = [];
  globalThis.__n6kSocketReg = undefined;
  const el = document.createElement("div");
  document.body.append(el);
  root = createRoot(el);
  root.render(
    <QueryProvider>
      <App m={m} />
    </QueryProvider>,
  );
};

globalThis.__n6kUnmountSockets = () => {
  root?.unmount();
  root = undefined;
};

globalThis.__n6kReady = true;
