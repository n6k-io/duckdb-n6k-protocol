import path from "node:path";
import { type Page } from "playwright-core";
import { CHROMIUM_PATH } from "./browser-gate";
import { getSharedBrowser } from "./browser-harness";
import { N6K_WORKER_ENTRIES, n6kWorkerUrl } from "../../workers/manifest";

const SRC = path.resolve(import.meta.dir, "../..");
const DIST = path.resolve(
  import.meta.dir,
  "../../../node_modules/@duckdb/duckdb-wasm/dist",
);
const WASM_DIR = path.resolve(import.meta.dir, "../../../wasm");

const COI_HEADERS = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
  "Cross-Origin-Resource-Policy": "cross-origin",
  // Opaque-origin child fetches its module cross-origin under COEP, so it needs CORS (ACAO).
  "Access-Control-Allow-Origin": "*",
};

function pageHtml(scriptPath: string): string {
  return (
    `<!doctype html><html><head><meta charset="utf-8"></head>` +
    `<body><div id="root"></div>` +
    `<script type="module" src="${scriptPath}"></script></body></html>`
  );
}

async function bundleScript(
  entry: string,
  format: "iife" | "esm",
): Promise<string> {
  // Child process bundling: in-process Bun.build breaks on the symlinked out-of-repo React.
  const proc = Bun.spawn(
    ["bun", "build", entry, "--target=browser", `--format=${format}`],
    { stdout: "pipe", stderr: "pipe" },
  );
  const [text, err, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (code !== 0) {
    throw new Error(`bun build failed for ${entry}:\n${err}`);
  }
  if (!text.trim())
    throw new Error(`bun build produced empty output for ${entry}`);
  return text;
}

export type IframeBrowserHarness = {
  page: Page;
  origin: string;
  cleanup: () => Promise<void>;
};

export async function setupIframeBrowser(): Promise<IframeBrowserHarness> {
  if (!CHROMIUM_PATH) {
    throw new Error(
      "iframe browser harness: no Chromium found (install playwright)",
    );
  }

  const pkgRoot = path.resolve(SRC, "..");
  const [parentJs, childJs, childOpaqueJs, workerBundles] = await Promise.all([
    bundleScript(path.join(import.meta.dir, "iframe-parent-entry.tsx"), "esm"),
    bundleScript(path.join(import.meta.dir, "iframe-child-entry.tsx"), "esm"),
    bundleScript(
      path.join(import.meta.dir, "iframe-child-opaque-entry.tsx"),
      "esm",
    ),
    Promise.all(
      N6K_WORKER_ENTRIES.map((e) =>
        bundleScript(path.join(pkgRoot, e.src), "iife"),
      ),
    ),
  ]);

  const inlineAssets: Record<string, string> = {
    "/_n6k/parent.js": parentJs,
    "/_n6k/child.js": childJs,
    "/_n6k/child-opaque.js": childOpaqueJs,
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
        return new Response(pageHtml("/_n6k/parent.js"), {
          headers: { "content-type": "text/html", ...COI_HEADERS },
        });
      }
      if (pathname === "/child") {
        return new Response(pageHtml("/_n6k/child.js"), {
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
  page.on("pageerror", (e) => {
    console.error("[iframe-e2e pageerror]", e.message);
  });

  const origin = `http://localhost:${server.port}`;
  await page.goto(`${origin}/`);

  const cleanup = async () => {
    try {
      await context.close();
    } catch {
      /* ignore */
    }
    server.stop();
  };

  return { page, origin, cleanup };
}
