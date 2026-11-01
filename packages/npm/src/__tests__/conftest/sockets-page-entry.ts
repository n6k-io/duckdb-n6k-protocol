import { N6kDuckDB } from "../../n6k-duckdb";
import type { SocketSpec } from "../../sockets/sockets";
import type { SocketsMount } from "./sockets-types";

const duckdb = new N6kDuckDB({
  bundle: {
    mainModule: "/duckdb/duckdb-coi.wasm",
    mainWorker: "/duckdb/duckdb-browser-coi.worker.js",
    pthreadWorker: "/duckdb/duckdb-browser-coi.pthread.worker.js",
  },
  maximumThreads: 1,
  extensionRepository: globalThis.location.origin,
});

function wsBase(): string {
  const serverUrl = globalThis.__n6kServerUrl ?? "n6k://localhost:8099";
  try {
    return `ws://${new URL(serverUrl.replace(/^n6k:/, "ws:")).host}`;
  } catch {
    return "ws://localhost:8099";
  }
}

const catalogs = duckdb.metadataFns.catalogs;
catalogs.subscribe(() => {
  const history = (globalThis.__n6kHistory ??= {});
  for (const [c, s] of Object.entries(catalogs.getSnapshot())) {
    const seen = (history[c] ??= []);
    if (seen.at(-1) !== s.status) seen.push(s.status);
  }
});

globalThis.__n6kConfigure = (m: SocketsMount) => {
  globalThis.__n6kOpenedSockets = [];
  globalThis.__n6kHistory = {};
  const url = `${wsBase()}${m.path}`;
  const main: SocketSpec =
    m.mode === "connect"
      ? {
          connect: () => {
            const s = new WebSocket(url);
            globalThis.__n6kOpenedSockets!.push(s);
            return s;
          },
          token: m.token,
        }
      : { socket: null, token: m.token };
  globalThis.__n6kSetSocket = () =>
    duckdb.setSockets({ main: { socket: new WebSocket(url), token: m.token } });
  duckdb.setSockets({ main });
  duckdb.setDatabases(
    Object.fromEntries(
      m.catalogs.map((c) => [
        c,
        { socket: "main", catalog: m.serverCatalogs?.[c] },
      ]),
    ),
  );
};

globalThis.__n6kReset = () => {
  duckdb.setDatabases({});
  duckdb.setSockets({});
};

globalThis.__n6kStates = () => ({ ...catalogs.getSnapshot() });

globalThis.__n6kRead = async (sql) => {
  const c = await duckdb.connect();
  try {
    const r = await c.query(sql);
    return r.toArray().map((row) => row.toJSON());
  } finally {
    await c.close();
  }
};

globalThis.__n6kEcho = (x) => {
  const reg = duckdb.getSnapshot().registrations.main;
  if (!reg) throw new Error("socket 'main' is not registered");
  return reg.rpc.call("echo", { x });
};

globalThis.__n6kReady = true;
