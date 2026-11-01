import { useEffect, useRef, useState, type RefObject } from "react";
import { createRoot } from "react-dom/client";
import { createDuckDB } from "../../create-duckdb";
import type { ConnectionLike } from "../../connection-shape";
import type { IframeHostTarget } from "../../iframe/host";
import type { N6kDuckDBSnapshot } from "../../n6k-duckdb";
import { useDuckDBIframeHost } from "../../react/use-duckdb-iframe-host";
import { createWasmConnect, type WasmDatabase } from "../../wasm-lease";
import "./iframe-e2e-types";

const SNAPSHOT: N6kDuckDBSnapshot = {
  status: "ready",
  error: null,
  connStatus: {},
  invalidations: {},
  desired: {},
  attached: {},
  errors: {},
  registrations: {},
};
const unsubscribe = () => {};
const ignore = () => {};

function Host({
  iframeRef,
  target,
  allowedOrigins,
}: {
  iframeRef: RefObject<HTMLIFrameElement | null>;
  target: IframeHostTarget;
  allowedOrigins: string[];
}) {
  useDuckDBIframeHost(iframeRef, target, { allowedOrigins });
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

function ParentApp({ target }: { target: IframeHostTarget }) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const opaqueRef = useRef<HTMLIFrameElement>(null);
  const [, setBump] = useState(0);
  const origin = globalThis.location.origin;

  useEffect(() => {
    globalThis.__bumpParent = () => setBump((b) => b + 1);
  }, []);

  return (
    <>
      <Host iframeRef={iframeRef} target={target} allowedOrigins={[origin]} />
      <iframe ref={iframeRef} src="/child" title="child" />
      <Host iframeRef={opaqueRef} target={target} allowedOrigins={["null"]} />
      <iframe
        ref={opaqueRef}
        name="opaque"
        title="child-opaque"
        sandbox="allow-scripts"
        srcDoc={opaqueChildSrcdoc(origin)}
      />
    </>
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
  const connect: () => Promise<ConnectionLike> = createWasmConnect(
    db as unknown as WasmDatabase,
  );
  const target: IframeHostTarget = {
    subscribe: () => unsubscribe,
    getSnapshot: () => SNAPSHOT,
    connect,
    setDesired: ignore,
    removeDesired: ignore,
    reconnect: ignore,
  };
  createRoot(el).render(<ParentApp target={target} />);
  globalThis.__parentReady = true;
}

boot().catch((error) => {
  globalThis.__parentError =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
});
