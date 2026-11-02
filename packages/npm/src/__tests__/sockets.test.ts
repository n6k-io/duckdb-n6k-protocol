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
  const states = () => h.page.evaluate(() => globalThis.__n6kStates!());
  const stateOf = async (cat: string) => {
    const all = await states();
    return all[cat];
  };
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
      s = await stateOf(cat);
      if (s && done(s)) return s;
      await new Promise((r) => setTimeout(r, 50));
    }
    return s;
  }

  test(
    "a socket-backed catalog is attached once connect resolves",
    async () => {
      const cat = name("sockrace");
      await configure({
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
      });
      expect(await stateOf(cat)).toEqual({ version: "0" });
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
        path: `/tuned/ws?catalogs=${a},${b}`,
        token: "",
        catalogs: [a, b],
      });
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
        path: `/tuned/ws?catalogs=${a},${b}`,
        token: "",
        catalogs: [x, y],
        serverCatalogs: { [x]: a, [y]: b },
      });
      expect(
        await read(
          `SELECT x.catalog AS a, y.catalog AS b FROM ${x}.main.marker x JOIN ${y}.main.marker y ON x.catalog <> y.catalog`,
        ),
      ).toEqual([{ a, b }]);
    },
    TIMEOUT,
  );

  test(
    "a dropped socket is reopened and the catalog's version moves",
    async () => {
      const cat = name("sockdrop");
      await configure({
        path: `/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
      });
      await h.page.evaluate(() => globalThis.__n6kOpenedSockets![0]!.close());
      const after = await stateUntil(cat, (s) => s.version !== "0");
      expect(after?.version).not.toBe("0");
      expect(after?.error).toBeUndefined();
      expect(await read(usersSql(cat))).toEqual(USERS);
      expect(await opened()).toBe(2);
    },
    TIMEOUT,
  );

  test(
    "a socket that fails its handshake errors its catalog",
    async () => {
      const cat = name("sockbad");
      await configure({
        path: `/auth/ws?catalog=${cat}`,
        token: "wrong-token",
        catalogs: [cat],
      });
      const s = await stateOf(cat);
      expect(s?.error).toContain("unauthorized");
    },
    TIMEOUT,
  );

  test(
    "the registration exposes the socket's rpc",
    async () => {
      const cat = name("sockrpc");
      await configure({
        path: `/rpc/ws?catalog=${cat}`,
        token: "",
        catalogs: [cat],
      });
      expect(await h.page.evaluate(() => globalThis.__n6kEcho!(1))).toBe(1);
    },
    TIMEOUT,
  );
});
