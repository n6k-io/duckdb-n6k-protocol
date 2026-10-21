import path from "node:path";
import { test, expect, beforeAll, afterAll, afterEach } from "bun:test";
import { describeCoi } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import type {
  SocketsMount,
  SocketsProbe,
} from "./conftest/react-sockets-types";

const TIMEOUT = 60_000;

const usersSql = (cat: string) =>
  `SELECT id, name FROM ${cat}.main.users ORDER BY id`;
const USERS = [
  { id: 1, name: "Alice" },
  { id: 2, name: "Bob" },
  { id: 3, name: "Charlie" },
];

describeCoi("DuckDBProvider sockets prop [browser coi]", () => {
  let h: BrowserHarness;
  let seq = 0;
  const name = (prefix: string) => `${prefix}_${Date.now()}_${seq++}`;

  beforeAll(async () => {
    const { setupBrowserDuckdb } = await import("./conftest/browser-harness");
    h = await setupBrowserDuckdb({
      bundle: "coi",
      entry: path.join(
        import.meta.dir,
        "conftest/react-sockets-page-entry.tsx",
      ),
    });
  }, 60_000);
  afterEach(async () => {
    await h?.page.evaluate(() => globalThis.__n6kUnmountSockets!());
  });
  afterAll(async () => {
    await h?.cleanup();
  });

  const mount = (m: SocketsMount) =>
    h.page.evaluate((m) => globalThis.__n6kMountSockets!(m), m);

  async function probeUntil(
    done: (p: SocketsProbe) => boolean,
    timeoutMs = 20_000,
  ): Promise<SocketsProbe | undefined> {
    const deadline = Date.now() + timeoutMs;
    let p: SocketsProbe | undefined;
    while (Date.now() < deadline) {
      p = await h.page.evaluate(() => globalThis.__n6kSocketsProbe);
      if (p && done(p)) return p;
      await new Promise((r) => setTimeout(r, 50));
    }
    return p;
  }

  test(
    "a query on a socket-backed catalog waits for the attach instead of failing",
    async () => {
      const cat = name("sockrace");
      await mount({
        mode: "connect",
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
        sql: usersSql(cat),
      });
      const p = await probeUntil((p) => p.status === "ready");
      expect(p?.rows).toEqual(USERS);
      expect(p?.history.some((s) => s.statusInfo === "awaitingAttach")).toBe(
        true,
      );
      expect(p?.history.filter((s) => s.status === "error")).toEqual([]);
    },
    TIMEOUT,
  );

  test(
    "two catalogs ride one provider socket",
    async () => {
      const a = name("sockmuxa");
      const b = name("sockmuxb");
      await mount({
        mode: "connect",
        path: `/tuned/ws?catalogs=${a},${b}`,
        token: "",
        catalogs: [a, b],
        sql: `SELECT x.catalog AS a, y.catalog AS b FROM ${a}.main.marker x, ${b}.main.marker y`,
      });
      const p = await probeUntil((p) => p.status === "ready");
      expect(p?.rows).toEqual([{ a, b }]);
      expect(
        await h.page.evaluate(() => globalThis.__n6kOpenedSockets!.length),
      ).toBe(1);
    },
    TIMEOUT,
  );

  test(
    "a join across catalogs on one socket reads each from its own catalog",
    async () => {
      const a = name("sockjoina");
      const b = name("sockjoinb");
      const x = name("sockjoinx");
      const y = name("sockjoiny");
      await mount({
        mode: "connect",
        path: `/tuned/ws?catalogs=${a},${b}`,
        token: "",
        catalogs: [x, y],
        serverCatalogs: { [x]: a, [y]: b },
        sql: `SELECT x.catalog AS a, y.catalog AS b FROM ${x}.main.marker x JOIN ${y}.main.marker y ON x.catalog <> y.catalog`,
      });
      const p = await probeUntil((p) => p.status === "ready");
      expect(p?.error).toBeNull();
      expect(p?.rows).toEqual([{ a, b }]);
    },
    TIMEOUT,
  );

  test(
    "connect: a dropped socket is reopened and the query recovers",
    async () => {
      const cat = name("sockdrop");
      await mount({
        mode: "connect",
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
        sql: usersSql(cat),
      });
      await probeUntil((p) => p.status === "ready");
      await h.page.evaluate(() => globalThis.__n6kOpenedSockets![0]!.close());
      const p = await probeUntil(
        (p) =>
          p.status === "ready" &&
          p.history.some((s) => s.statusInfo === "disconnected"),
      );
      expect(p?.status).toBe("ready");
      expect(p?.rows).toEqual(USERS);
      expect(
        await h.page.evaluate(() => globalThis.__n6kOpenedSockets!.length),
      ).toBe(2);
    },
    TIMEOUT,
  );

  test(
    "socket: queries wait until the app hands over a socket, and survive a swap",
    async () => {
      const cat = name("sockapp");
      await mount({
        mode: "socket",
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
        sql: usersSql(cat),
      });
      const waiting = await probeUntil(
        (p) => p.statusInfo === "awaitingAttach",
      );
      await new Promise((r) => setTimeout(r, 1000));
      const still = await h.page.evaluate(() => globalThis.__n6kSocketsProbe);
      expect(waiting?.statusInfo).toBe("awaitingAttach");
      expect(still?.statusInfo).toBe("awaitingAttach");

      await h.page.evaluate(() => globalThis.__n6kSetSocket!());
      const ready = await probeUntil((p) => p.status === "ready");
      expect(ready?.rows).toEqual(USERS);

      await h.page.evaluate(() => globalThis.__n6kSetSocket!());
      await new Promise((r) => setTimeout(r, 1000));
      const after = await probeUntil((p) => p.status === "ready");
      expect(after?.rows).toEqual(USERS);
      expect(after?.history.filter((s) => s.status === "error")).toEqual([]);
    },
    TIMEOUT,
  );

  test(
    "a socket that fails its handshake fails the catalog's queries",
    async () => {
      const cat = name("sockbad");
      await mount({
        mode: "connect",
        path: `/auth/ws?catalog=${cat}`,
        token: "wrong-token",
        catalogs: [cat],
        sql: usersSql(cat),
      });
      const p = await probeUntil((p) => p.status === "error");
      expect(p?.error).toContain(`Attach ${cat} failed`);
      expect(p?.error).toContain("unauthorized");
    },
    TIMEOUT,
  );

  test(
    "useSocket exposes the registration's rpc",
    async () => {
      const cat = name("sockrpc");
      await mount({
        mode: "connect",
        path: `/rpc/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
        sql: usersSql(cat),
      });
      await probeUntil((p) => p.status === "ready");
      const echoed = await h.page.evaluate(() =>
        globalThis.__n6kSocketReg!.rpc.call("echo", { x: 1 }),
      );
      expect(echoed).toBe(1);
    },
    TIMEOUT,
  );
});
