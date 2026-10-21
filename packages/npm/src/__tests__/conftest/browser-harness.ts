import path from "node:path";
import { chromium, type Browser, type Page } from "playwright-core";
import { CHROMIUM_PATH } from "./browser-gate";
import { N6K_URL } from "../_server-gate";
import type { ConnectionLike } from "../../connection-shape";
import type { WsStatus } from "../../types";
import { N6K_WORKER_ENTRIES, n6kWorkerUrl } from "../../workers/manifest";

const SRC = path.resolve(import.meta.dir, "../..");
const DIST = path.resolve(
  import.meta.dir,
  "../../../node_modules/@duckdb/duckdb-wasm/dist",
);
const WASM_DIR = path.resolve(import.meta.dir, "../../../wasm");

const PAGE_HTML =
  `<!doctype html><html><head><meta charset="utf-8"></head>` +
  `<body><script type="module" src="/_n6k/app.js"></script></body></html>`;

const COI_HEADERS = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Resource-Policy": "cross-origin",
};

// Reuse ONE Chromium across the whole suite. bun runs test files sequentially, so
// launching a fresh browser per file churns ~N launch/close cycles and hits
// transient launch failures under load. A shared browser + a fresh context per
// test keeps isolation while launching once; the browser dies with the process.
let sharedBrowserPromise: Promise<Browser> | undefined;
export function getSharedBrowser(): Promise<Browser> {
  if (!sharedBrowserPromise) {
    sharedBrowserPromise = chromium.launch({
      executablePath: CHROMIUM_PATH ?? undefined,
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
  }
  return sharedBrowserPromise;
}

export type BrowserHarness = {
  conn: ConnectionLike;
  // The Playwright page, so a driver can run in-page probes (e.g. the profiler's
  // multi-connection concurrency test) that the single `conn` proxy can't express.
  page: Page;
  reconnect: (catalog: string) => void;
  cleanup: () => Promise<void>;
};

export type BrowserSetupOptions = {
  onStatus?: (catalog: string, status: WsStatus) => void;
  // Every caller passes "coi"; the eh path cannot LOAD n6k_client and exists only as an
  // escape hatch for testing the non-threaded engine itself.
  bundle?: "eh" | "coi";
  // DuckDB scheduler thread count (SET threads after LOAD); default 1.
  maxThreads?: number;
  // Page script to bundle instead of in-page-entry.ts; it must set __n6kReady (or __n6kError).
  entry?: string;
};

type WireResult = {
  fields: { name: string; typeId: number; scale?: number }[];
  rows: Record<string, unknown>[];
  error?: string;
};

function decodeValue(v: unknown): unknown {
  if (v === null || typeof v !== "object") return v;
  const tag = (v as { $n6k?: string }).$n6k;
  if (tag !== undefined) {
    const s = (v as { v: string }).v;
    if (tag === "bigint") return BigInt(s);
    if (tag === "date") return new Date(s);
    if (tag === "bytes") {
      const pairs = s.match(/.{2}/g) ?? [];
      return Uint8Array.from(pairs.map((h) => Number.parseInt(h, 16)));
    }
  }
  if (Array.isArray(v)) return v.map((x) => decodeValue(x));
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    out[k] = decodeValue(val);
  }
  return out;
}

async function bundleScript(
  entry: string,
  format: "iife" | "esm",
): Promise<string> {
  const out = await Bun.build({
    entrypoints: [entry],
    target: "browser",
    format,
  });
  if (!out.success) {
    throw new Error(
      `Bun.build failed for ${entry}:\n${out.logs.map(String).join("\n")}`,
    );
  }
  const text = await out.outputs[0].text();
  if (!text.trim())
    throw new Error(`Bun.build produced empty output for ${entry}`);
  return text;
}

export async function setupBrowserDuckdb(
  opts: BrowserSetupOptions = {},
): Promise<BrowserHarness> {
  if (!CHROMIUM_PATH) {
    throw new Error("browser harness: no Chromium found (install playwright)");
  }

  const pkgRoot = path.resolve(SRC, "..");
  const [appJs, workerBundles] = await Promise.all([
    bundleScript(
      opts.entry ?? path.join(import.meta.dir, "in-page-entry.ts"),
      "esm",
    ),
    Promise.all(
      N6K_WORKER_ENTRIES.map((e) =>
        bundleScript(path.join(pkgRoot, e.src), "iife"),
      ),
    ),
  ]);

  const inlineAssets: Record<string, string> = {
    "/_n6k/app.js": appJs,
    ...Object.fromEntries(
      N6K_WORKER_ENTRIES.map((e, i) => [
        n6kWorkerUrl(e.file),
        workerBundles[i],
      ]),
    ),
  };

  const server = Bun.serve({
    port: 0,
    async fetch(req) {
      const { pathname } = new URL(req.url);
      if (pathname === "/") {
        return new Response(PAGE_HTML, {
          headers: { "content-type": "text/html", ...COI_HEADERS },
        });
      }
      const inline = inlineAssets[pathname];
      if (inline !== undefined) {
        return new Response(inline, {
          headers: { "content-type": "text/javascript", ...COI_HEADERS },
        });
      }
      if (pathname.startsWith("/duckdb/")) {
        const f = Bun.file(path.join(DIST, pathname.slice("/duckdb/".length)));
        if (await f.exists())
          return new Response(f, { headers: { ...COI_HEADERS } });
      }
      const extFile = Bun.file(path.join(WASM_DIR, pathname));
      if (await extFile.exists()) {
        return new Response(extFile, { headers: { ...COI_HEADERS } });
      }
      return new Response("not found", {
        status: 404,
        headers: { ...COI_HEADERS },
      });
    },
  });

  const browser = await getSharedBrowser();
  const context = await browser.newContext();
  const page: Page = await context.newPage();
  await page.exposeBinding(
    "__n6kOnStatus",
    (_src, catalog: string, status: string) => {
      opts.onStatus?.(catalog, status as WsStatus);
    },
  );
  page.on("pageerror", (e) => {
    console.error("[browser pageerror]", e.message);
  });

  await page.addInitScript(
    ({ url, bundle, maxThreads }) => {
      globalThis.__n6kServerUrl = url;
      globalThis.__n6kBundle = bundle;
      globalThis.__n6kMaxThreads = maxThreads;
    },
    {
      url: N6K_URL,
      bundle: opts.bundle ?? "eh",
      maxThreads: opts.maxThreads ?? 1,
    },
  );

  await page.goto(`http://localhost:${server.port}/`);
  await page.waitForFunction(
    () => globalThis.__n6kReady === true || globalThis.__n6kError !== undefined,
    undefined,
    { timeout: 30_000 },
  );
  const bootError = await page.evaluate(() => globalThis.__n6kError);
  if (bootError) throw new Error(`browser boot failed: ${bootError}`);

  const conn: ConnectionLike = {
    async query(sql: string) {
      const wire = (await page.evaluate(
        (s) => globalThis.__n6k!.query(s),
        sql,
      )) as WireResult;
      if (wire.error) throw new Error(wire.error);
      return {
        schema: {
          fields: wire.fields.map((f) => ({
            name: f.name,
            type: {
              typeId: f.typeId,
              ...(f.scale === undefined ? {} : { scale: f.scale }),
            },
          })),
        },
        toArray: () =>
          wire.rows.map((row) => {
            const decoded = decodeValue(row) as Record<string, unknown>;
            return { toJSON: () => decoded };
          }),
      };
    },
    close: async () => {},
  };

  const cleanup = async () => {
    try {
      await page.evaluate(() => globalThis.__n6k?.detach());
    } catch {
      /* ignore */
    }
    try {
      // Close the context (page), not the shared browser — it lives for the
      // whole suite and is torn down when the process exits.
      await context.close();
    } catch {
      /* ignore */
    }
    server.stop();
  };

  return {
    conn,
    page,
    reconnect: (catalog: string) => {
      void page.evaluate((c) => globalThis.__n6k!.reconnect(c), catalog);
    },
    cleanup,
  };
}
