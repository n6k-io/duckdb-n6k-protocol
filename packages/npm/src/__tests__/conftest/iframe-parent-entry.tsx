import { useEffect, useMemo, useRef, useState, type RefObject } from "react";
import { createRoot } from "react-dom/client";
import { createDuckDB } from "../../create-duckdb";
import type { ConnectionLike } from "../../connection-shape";
import { createWasmConnect, type WasmDatabase } from "../../wasm-lease";
import {
  DuckDBContext,
  type DuckDBContextValue,
} from "../../react/duckdb-context";
import { useDuckDBIframeHost } from "../../react/iframe/use-duckdb-iframe-host";
import "./iframe-e2e-types";

function Host({
  iframeRef,
  allowedOrigins,
}: {
  iframeRef: RefObject<HTMLIFrameElement | null>;
  allowedOrigins: string[];
}) {
  useDuckDBIframeHost(iframeRef, { allowedOrigins });
  return null;
}

function opaqueChildSrcdoc(parentOrigin: string): string {
  return (
    `<!doctype html><html><head><meta charset="utf-8"></head>` +
    `<body><div id="root"></div>` +
    `<script>window.__PARENT_ORIGIN=${JSON.stringify(parentOrigin)};</script>` +
    `<script type="module" src="${parentOrigin}/_n6k/child-opaque.js"></script>` +
    `</body></html>`
  );
}

function ParentApp({
  conn,
  connect,
}: {
  conn: ConnectionLike;
  connect: () => Promise<ConnectionLike>;
}) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const opaqueRef = useRef<HTMLIFrameElement>(null);
  const [, setBump] = useState(0);
  const origin = globalThis.location.origin;

  const value = useMemo<DuckDBContextValue>(
    () => ({
      conn,
      status: "ready",
      error: null,
      connStatus: {},
      invalidations: {},
      desired: {},
      attached: {},
      errors: {},
      setDesired: () => {},
      removeDesired: () => {},
      reconnect: () => {},
      registerWebsocket: async () => "",
      replaceWebsocket: async () => {},
      listCatalogs: async () => [],
      connect,
    }),
    [conn, connect],
  );

  useEffect(() => {
    globalThis.__bumpParent = () => setBump((b) => b + 1);
  }, []);

  return (
    <DuckDBContext.Provider value={value}>
      <Host iframeRef={iframeRef} allowedOrigins={[origin]} />
      <iframe ref={iframeRef} src="/child" title="child" />
      <Host iframeRef={opaqueRef} allowedOrigins={["null"]} />
      <iframe
        ref={opaqueRef}
        name="opaque"
        title="child-opaque"
        sandbox="allow-scripts"
        srcDoc={opaqueChildSrcdoc(origin)}
      />
    </DuckDBContext.Provider>
  );
}

async function boot(): Promise<void> {
  const { db, conn } = await createDuckDB({
    bundle: {
      mainModule: "/duckdb/duckdb-eh.wasm",
      mainWorker: "/duckdb/duckdb-browser-eh.worker.js",
      pthreadWorker: null,
    },
    extensionRepository: globalThis.location.origin,
  });
  await conn.query(
    `CREATE TABLE t AS SELECT * FROM (VALUES
       (1, 'a', CAST(1.50 AS DECIMAL(10,2))),
       (2, 'b', CAST(2.25 AS DECIMAL(10,2)))
     ) AS v(id, name, amt)`,
  );

  const el = document.querySelector("#root");
  if (!el) throw new Error("missing #root");
  const connect = createWasmConnect(db as unknown as WasmDatabase);
  createRoot(el).render(<ParentApp conn={conn} connect={connect} />);
  globalThis.__parentReady = true;
}

boot().catch((error) => {
  globalThis.__parentError =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
});
