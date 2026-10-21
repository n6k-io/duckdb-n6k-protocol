import { useEffect } from "react";
import { createRoot } from "react-dom/client";
import { QueryProvider } from "../../react/query-provider";
import { DuckDBProvider } from "../../react/duckdb-provider";
import { useQuery } from "../../react/use-sql-query";
import "./react-e2e-types";

function Probe({ sql }: { sql: string }) {
  const r = useQuery(sql);
  useEffect(() => {
    globalThis.__n6kReactProbe = {
      status: r.status,
      rows: r.rows,
      error: r.error,
    };
  });
  return null;
}

globalThis.__n6kMountReact = (catalog: string, sql: string) => {
  const root = document.createElement("div");
  document.body.append(root);
  const serverUrl = globalThis.__n6kServerUrl ?? "n6k://localhost:8099";
  createRoot(root).render(
    <QueryProvider>
      <DuckDBProvider
        databases={{ [catalog]: serverUrl }}
        duckdbOptions={{
          bundle: {
            mainModule: "/duckdb/duckdb-coi.wasm",
            mainWorker: "/duckdb/duckdb-browser-coi.worker.js",
            pthreadWorker: "/duckdb/duckdb-browser-coi.pthread.worker.js",
          },
          // Single-threaded so LOAD/dlopen doesn't deadlock against parked scheduler pthreads.
          maximumThreads: 1,
          extensionRepository: globalThis.location.origin,
        }}
      >
        <Probe sql={sql} />
      </DuckDBProvider>
    </QueryProvider>,
  );
};

globalThis.__n6kReady = true;
