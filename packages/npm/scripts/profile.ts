/**
 * n6k concurrency profiler — proves that concurrent queries against the server
 * overlap over the single multiplexed WebSocket instead of serializing.
 *
 *   cd packages/npm
 *   BACKEND=<native|browser-threads> TEST_SERVER=http://localhost:8099 \
 *     bun run profile [--threads N]      # scheduler thread count (default 1)
 *
 * It runs two workloads, each with three arms (single / serial×N / parallel×N):
 *
 *   network — scans `slowdb.main.slow`, a virtual table whose server-side scan
 *     sleeps ~500ms (SlowProvider in the test server). The delay is an `await`
 *     on the server event loop, so N concurrent scans over the multiplexed
 *     WebSocket SHOULD overlap to ≈ one sleep — that's the property under test.
 *   local — a purely local, CPU-bound DuckDB query (no ATTACH, no n6k://, no
 *     WebSocket). This is the control: it isolates client/engine parallelism
 *     from the network path. On a single-worker engine `local parallel×N ≈
 *     local serial×N`, because one worker thread executes one query at a time;
 *     a real thread pool is what makes local parallel overlap. The coi bundle opens
 *     single-threaded; pass --threads N to raise it after LOAD.
 *
 *   single      — one query.                     ≈ one unit   (measures overhead)
 *   serial×N    — N queries on ONE connection.   ≈ N units    (a connection is one lane)
 *   parallel×N  — N queries on N connections.    ≈ one unit if the work overlaps
 *
 * The parallel win uses N connections: a single connection serializes, so
 * `parallel×N` opens a fresh connection per query — exactly what
 * `useSQLQuery`/`withLease` does in production.
 *
 * The browser-threads backend runs the probe inside the page (connection handles
 * can't cross the page.evaluate boundary), booting the coi bundle — the only
 * loadable wasm runtime now that the n6k extension is coi-only.
 */
import { parseArgs } from "util";
import type { ConnectionLike } from "../src/connection-shape";

const BACKENDS = ["native", "browser-threads"] as const;
type BackendName = (typeof BACKENDS)[number];

const BACKEND = process.env.BACKEND ?? "";
const TEST_SERVER = process.env.TEST_SERVER ?? "";
const N = Number(process.env.CONC ?? "8");

// DuckDB scheduler thread count. `--threads N` (or THREADS env); defaults to 1.
// Native: pool size at instance creation. browser-threads: SET threads after LOAD.
const { values: cli } = parseArgs({
  args: Bun.argv,
  options: { threads: { type: "string" } },
  strict: false,
  allowPositionals: true,
});
const THREADS = Math.max(
  1,
  Math.floor(Number(cli.threads ?? process.env.THREADS ?? "1")),
);

if (!BACKENDS.includes(BACKEND as BackendName)) {
  console.error(`BACKEND must be one of ${BACKENDS.join("|")} (got ${BACKEND || "<unset>"}).`);
  process.exit(2);
}
if (!TEST_SERVER || TEST_SERVER === "skip") {
  console.error("TEST_SERVER must be a base URL (this profiler needs the server).");
  process.exit(2);
}

const N6K_URL = `n6k://${new URL(TEST_SERVER).host}`;
const ATTACH_SLOW = `ATTACH '${N6K_URL}' AS slowdb (TYPE n6k)`;
const SLOW_SQL = "SELECT * FROM slowdb.main.slow";

// A purely local, CPU-bound query — no ATTACH, no n6k://, no WebSocket. Scans a
// synthetic range and hashes every row so the engine must actually burn CPU
// (DuckDB parallelizes this across its threads). LOCAL_ROWS tunes the cost so
// it lands in the same ballpark as the ~500ms network scan on your machine.
const LOCAL_ROWS = Number(process.env.LOCAL_ROWS ?? "80000000");
const LOCAL_SQL = `SELECT count(*) FROM range(${LOCAL_ROWS}) t(i) WHERE (hash(i) & 7) = 0`;

type Arm = { single: number; serial: number; parallel: number };
type Result = { network: Arm; local: Arm };

/** A backend reduced to the one thing that matters: open a fresh connection. */
type Bench = {
  connect: () => Promise<ConnectionLike>;
  cleanup: () => Promise<void>;
};

async function timeMs(fn: () => Promise<void>): Promise<number> {
  const t0 = performance.now();
  await fn();
  return performance.now() - t0;
}

/** The three arms for one workload (`sql`), over a `connect()` factory. */
async function runArms(connect: Bench["connect"], sql: string): Promise<Arm> {
  const scanOnce = async (): Promise<void> => {
    const c = await connect();
    try {
      await c.query(sql);
    } finally {
      await c.close();
    }
  };

  await scanOnce(); // warm

  const single = await timeMs(scanOnce);
  const serial = await timeMs(async () => {
    const c = await connect();
    try {
      for (let i = 0; i < N; i++) await c.query(sql);
    } finally {
      await c.close();
    }
  });
  const parallel = await timeMs(async () => {
    await Promise.all(Array.from({ length: N }, () => scanOnce()));
  });

  return { single, serial, parallel };
}

/** Both workloads, driven from Node over a `connect()` factory (native/wasm). */
async function measure(b: Bench): Promise<Result> {
  const network = await runArms(b.connect, SLOW_SQL);
  const local = await runArms(b.connect, LOCAL_SQL);
  return { network, local };
}

async function setupNative(): Promise<Bench> {
  const fs = await import("node:fs");
  const os = await import("node:os");
  const path = await import("node:path");
  const { DuckDBInstance } = await import("@duckdb/node-api");
  const { toWasmShape } = await import("../src/native-wasm-adapter/adapter");

  const n6kExt = path.resolve(
    import.meta.dir,
    "../../../build/release/extension/n6k_client/n6k_client.duckdb_extension",
  );
  const secretDir = fs.mkdtempSync(path.join(os.tmpdir(), "n6k-prof-"));
  const instance = await DuckDBInstance.create(undefined, {
    allow_unsigned_extensions: "true",
    secret_directory: secretDir,
    // Size the pool at creation: a runtime `SET threads` doesn't resize an
    // already-initialized pool, so it wouldn't actually constrain parallelism.
    threads: String(THREADS),
  });

  // Load + ATTACH once on a setup connection: extensions and attached catalogs
  // are database-global, so every later instance.connect() shares them — and,
  // crucially, shares the single WsClient bound to the slowdb ATTACH.
  const setup = await instance.connect();
  await setup.run("INSTALL httpfs");
  await setup.run("LOAD httpfs");
  await setup.run(`LOAD '${n6kExt.replaceAll("'", "''")}'`);
  await setup.run(ATTACH_SLOW);

  return {
    connect: async () => {
      const c = await instance.connect();
      const shaped = toWasmShape(c);
      return { query: shaped.query, close: async () => c.disconnectSync() };
    },
    cleanup: async () => {
      setup.disconnectSync();
      instance.closeSync();
      try {
        fs.rmSync(secretDir, { recursive: true, force: true });
      } catch {
        /* best effort */
      }
    },
  };
}

/** The browser arm runs entirely in-page — a live connection can't cross the
 * page.evaluate boundary, so we fan out inside the page and return only times.
 * Always the coi bundle (the only loadable wasm runtime). */
async function runBrowser(): Promise<Result> {
  const { setupBrowserDuckdb } = await import("../src/__tests__/conftest/browser-harness");
  const h = await setupBrowserDuckdb({ bundle: "coi", maxThreads: THREADS });
  h.page.on("pageerror", (e) => console.error("[page error]", e.stack || e.message));
  h.page.on("console", (m) => {
    if (m.type() === "error") console.error("[page console]", m.text());
  });
  try {
    return await h.page.evaluate(
      async ({ n, networkSql, localSql, attach }): Promise<Result> => {
        const connect = (
          globalThis as unknown as {
            __n6kConnect: () => Promise<{
              query: (s: string) => Promise<unknown>;
              close: () => Promise<void>;
            }>;
          }
        ).__n6kConnect;
        const time = async (fn: () => Promise<void>): Promise<number> => {
          const t = performance.now();
          await fn();
          return performance.now() - t;
        };
        const runArms = async (sql: string) => {
          const scanOnce = async (): Promise<void> => {
            const c = await connect();
            try {
              await c.query(sql);
            } finally {
              await c.close();
            }
          };
          await scanOnce(); // warm
          const single = await time(scanOnce);
          const serial = await time(async () => {
            const c = await connect();
            try {
              for (let i = 0; i < n; i++) await c.query(sql);
            } finally {
              await c.close();
            }
          });
          const parallel = await time(async () => {
            await Promise.all(Array.from({ length: n }, () => scanOnce()));
          });
          return { single, serial, parallel };
        };
        // ATTACH the slow catalog once (database-global) before the network arm.
        const a = await connect();
        try {
          await a.query(attach);
        } finally {
          await a.close();
        }
        const network = await runArms(networkSql);
        const local = await runArms(localSql);
        return { network, local };
      },
      { n: N, networkSql: SLOW_SQL, localSql: LOCAL_SQL, attach: ATTACH_SLOW },
    );
  } finally {
    await h.cleanup();
  }
}

function reportArm(title: string, note: string, arm: Arm) {
  const { single, serial, parallel } = arm;
  const f = (n: number) => `${n.toFixed(0).padStart(6)} ms`;
  console.log(`  ${title}  ${note}`);
  console.log(`    single          ${f(single)}`);
  console.log(`    serial   × ${String(N).padStart(2)}   ${f(serial)}   (≈ N × single)`);
  console.log(`    parallel × ${String(N).padStart(2)}   ${f(parallel)}   (≈ single if overlapped)`);
  console.log(
    `    speedup (serial / parallel): ${(serial / parallel).toFixed(1)}×  ` +
      `— ideal ≈ ${N}× if fully overlapped\n`,
  );
}

function report(result: Result) {
  reportArm("network", "(slowdb scan over the multiplexed WebSocket)", result.network);
  reportArm("local  ", `(CPU-bound local scan, ${LOCAL_ROWS.toLocaleString()} rows — no network)`, result.local);
}

async function main() {
  console.log(
    `\nn6k concurrency profiler — BACKEND=${BACKEND}, N=${N}, threads=${THREADS}\n`,
  );
  let result: Result;
  if (BACKEND === "native") {
    const b = await setupNative();
    try {
      result = await measure(b);
    } finally {
      await b.cleanup();
    }
  } else {
    result = await runBrowser();
  }
  report(result);
}

// Exit explicitly: the browser-threads arm holds the shared headless Chromium
// open (that harness expects the process to die and reap it, as under the test
// runner), so without this the CLI hangs after printing results.
main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
