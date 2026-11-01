import path from "node:path";
import { test, expect, beforeAll, afterAll, afterEach } from "bun:test";
import { describeCoi } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import type { SocketsMount } from "./conftest/sockets-types";
import type { CatalogState } from "../attach/catalog-states";

const TIMEOUT = 60_000;

const usersSql = (cat: string) =>
  `SELECT id, name FROM ${cat}.main.users ORDER BY id`;
const USERS = [
  { id: 1, name: "Alice" },
  { id: 2, name: "Bob" },
  { id: 3, name: "Charlie" },
];

describeCoi("N6kDuckDB sockets [browser coi]", () => {
  let h: BrowserHarness;
  let seq = 0;
  const name = (prefix: string) => `${prefix}_${Date.now()}_${seq++}`;

  beforeAll(async () => {
    const { setupBrowserDuckdb } = await import("./conftest/browser-harness");
    h = await setupBrowserDuckdb({
      bundle: "coi",
      entry: path.join(import.meta.dir, "conftest/sockets-page-entry.ts"),
    });
  }, 60_000);
  afterEach(async () => {
    await h?.page.evaluate(() => globalThis.__n6kReset!());
  });
  afterAll(async () => {
    await h?.cleanup();
  });

  const configure = (m: SocketsMount) =>
    h.page.evaluate((m) => globalThis.__n6kConfigure!(m), m);
  const read = (sql: string) =>
    h.page.evaluate((s) => globalThis.__n6kRead!(s), sql);
  const history = (cat: string) =>
    h.page.evaluate((c) => globalThis.__n6kHistory?.[c] ?? [], cat);
  const opened = () =>
    h.page.evaluate(() => globalThis.__n6kOpenedSockets!.length);

  async function stateUntil(
    cat: string,
    done: (s: CatalogState) => boolean,
    timeoutMs = 20_000,
  ): Promise<CatalogState | undefined> {
    const deadline = Date.now() + timeoutMs;
    let s: CatalogState | undefined;
    while (Date.now() < deadline) {
      const states = await h.page.evaluate(() => globalThis.__n6kStates!());
      s = states[cat];
      if (s && done(s)) return s;
      await new Promise((r) => setTimeout(r, 50));
    }
    return s;
  }
  const ready = (cat: string) => stateUntil(cat, (s) => s.status === "ready");
  const statusAfter = async (
    cat: string,
    done: (s: CatalogState) => boolean,
  ) => {
    const s = await stateUntil(cat, done);
    return s?.status;
  };

  test(
    "a socket-backed catalog is pending until its attach lands",
    async () => {
      const cat = name("sockrace");
      await configure({
        mode: "connect",
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
      });
      expect(await statusAfter(cat, (s) => s.status === "ready")).toBe("ready");
      expect(await history(cat)).toEqual(["pending", "ready"]);
      expect(await read(usersSql(cat))).toEqual(USERS);
    },
    TIMEOUT,
  );

  test(
    "two catalogs ride one socket",
    async () => {
      const a = name("sockmuxa");
      const b = name("sockmuxb");
      await configure({
        mode: "connect",
        path: `/tuned/ws?catalogs=${a},${b}`,
        token: "",
        catalogs: [a, b],
      });
      await ready(a);
      await ready(b);
      expect(
        await read(
          `SELECT x.catalog AS a, y.catalog AS b FROM ${a}.main.marker x, ${b}.main.marker y`,
        ),
      ).toEqual([{ a, b }]);
      expect(await opened()).toBe(1);
    },
    TIMEOUT,
  );

  test(
    "a join across aliased catalogs on one socket reads each from its own catalog",
    async () => {
      const a = name("sockjoina");
      const b = name("sockjoinb");
      const x = name("sockjoinx");
      const y = name("sockjoiny");
      await configure({
        mode: "connect",
        path: `/tuned/ws?catalogs=${a},${b}`,
        token: "",
        catalogs: [x, y],
        serverCatalogs: { [x]: a, [y]: b },
      });
      await ready(x);
      await ready(y);
      expect(
        await read(
          `SELECT x.catalog AS a, y.catalog AS b FROM ${x}.main.marker x JOIN ${y}.main.marker y ON x.catalog <> y.catalog`,
        ),
      ).toEqual([{ a, b }]);
    },
    TIMEOUT,
  );

  test(
    "connect: a dropped socket is reopened and the catalog recovers",
    async () => {
      const cat = name("sockdrop");
      await configure({
        mode: "connect",
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
      });
      await ready(cat);
      await h.page.evaluate(() => globalThis.__n6kOpenedSockets![0]!.close());
      await stateUntil(cat, (s) => s.status !== "ready");
      expect(await statusAfter(cat, (s) => s.status === "ready")).toBe("ready");
      expect(await history(cat)).toContain("disconnected");
      expect(await read(usersSql(cat))).toEqual(USERS);
      expect(await opened()).toBe(2);
    },
    TIMEOUT,
  );

  test(
    "socket: a catalog waits for the app's socket, and survives a swap",
    async () => {
      const cat = name("sockapp");
      await configure({
        mode: "socket",
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
      });
      await new Promise((r) => setTimeout(r, 1000));
      expect(await statusAfter(cat, () => true)).toBe("pending");

      await h.page.evaluate(() => globalThis.__n6kSetSocket!());
      await ready(cat);
      expect(await read(usersSql(cat))).toEqual(USERS);

      await h.page.evaluate(() => globalThis.__n6kSetSocket!());
      await new Promise((r) => setTimeout(r, 1000));
      expect(await statusAfter(cat, (s) => s.status === "ready")).toBe("ready");
      expect(await history(cat)).not.toContain("error");
      expect(await read(usersSql(cat))).toEqual(USERS);
    },
    TIMEOUT,
  );

  test(
    "a socket that fails its handshake errors its catalog",
    async () => {
      const cat = name("sockbad");
      await configure({
        mode: "connect",
        path: `/auth/ws?catalog=${cat}`,
        token: "wrong-token",
        catalogs: [cat],
      });
      const s = await stateUntil(cat, (s) => s.status === "error");
      expect(s?.status === "error" && s.message).toContain("unauthorized");
    },
    TIMEOUT,
  );

  test(
    "the registration exposes the socket's rpc",
    async () => {
      const cat = name("sockrpc");
      await configure({
        mode: "connect",
        path: `/rpc/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
      });
      await ready(cat);
      expect(await h.page.evaluate(() => globalThis.__n6kEcho!(1))).toBe(1);
    },
    TIMEOUT,
  );
});
