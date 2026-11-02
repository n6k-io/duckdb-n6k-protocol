import { N6kDuckDB } from "../../n6k-duckdb";
import type { SocketsMount } from "./sockets-types";

const bundle = {
  mainModule: "/duckdb/duckdb-coi.wasm",
  mainWorker: "/duckdb/duckdb-browser-coi.worker.js",
  pthreadWorker: "/duckdb/duckdb-browser-coi.pthread.worker.js",
};

function wsBase(): string {
  const serverUrl = globalThis.__n6kServerUrl ?? "n6k://localhost:8099";
  try {
    return `ws://${new URL(serverUrl.replace(/^n6k:/, "ws:")).host}`;
  } catch {
    return "ws://localhost:8099";
  }
}

let duckdb: N6kDuckDB | null = null;

function current(): N6kDuckDB {
  if (!duckdb) throw new Error("not configured");
  return duckdb;
}

globalThis.__n6kConfigure = async (m: SocketsMount) => {
  globalThis.__n6kOpenedSockets = [];
  const url = `${wsBase()}${m.path}`;
  duckdb = new N6kDuckDB({
    bundle,
    maximumThreads: 1,
    extensionRepository: globalThis.location.origin,
    sockets: {
      main: {
        connect: () => {
          const s = new WebSocket(url);
          globalThis.__n6kOpenedSockets!.push(s);
          return s;
        },
        token: m.token,
      },
    },
    databases: Object.fromEntries(
      m.catalogs.map((c) => [
        c,
        { socket: "main", catalog: m.serverCatalogs?.[c] },
      ]),
    ),
  });
  const c = await duckdb.connect();
  await c.close();
};

globalThis.__n6kReset = async () => {
  await duckdb?.dispose();
  duckdb = null;
};

globalThis.__n6kStates = () => ({
  ...current().metadataFns.catalogs.getSnapshot(),
});

globalThis.__n6kRead = async (sql) => {
  const c = await current().connect();
  try {
    const r = await c.query(sql);
    return r.toArray().map((row) => row.toJSON());
  } finally {
    await c.close();
  }
};

globalThis.__n6kEcho = (x) => {
  const reg = current().getSnapshot().registrations.main;
  if (!reg) throw new Error("socket 'main' is not registered");
  return reg.rpc.call("echo", { x });
};

globalThis.__n6kReady = true;
