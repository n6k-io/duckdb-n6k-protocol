import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import {
  serverExec,
  pushInvalidate,
  addCatalog,
  removeCatalog,
} from "./conftest/server";
import { FT } from "../protocol-generated";

const TIMEOUT = 30_000;

// App-supplied WebSockets are registered with the in-page driver, so sockets are
// created and asserted on in-page. PUSH interleaves node-side server calls.
describeCoi("registerWebsocket [browser coi]", () => {
  let h: BrowserHarness;
  let seq = 0;

  beforeAll(async () => {
    h = await setupCoiPage();
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  const name = (prefix: string): string => `${prefix}_${Date.now()}_${seq++}`;

  test(
    "attaches over a registered socket and resolves SELECT",
    async () => {
      const rows = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const { wsId: id } = await t.registerWebsocket(ws, "");
        const c = await t.connect();
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          const r = await c.query(
            `SELECT id, name FROM ${cat}.main.users ORDER BY id`,
          );
          return r
            .toArray()
            .map((x) => [Number(x.toJSON().id), x.toJSON().name]);
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws.close();
        }
      }, name("wsreg"));
      expect(rows).toEqual([
        [1, "Alice"],
        [2, "Bob"],
        [3, "Charlie"],
      ]);
    },
    TIMEOUT,
  );

  test(
    "registerWebsocket waits out FT_HELLO_PENDING from a slow authenticate",
    async () => {
      const rows = await h.page.evaluate(async () => {
        const t = globalThis.__n6kTest!;
        const cat = "slowauth_6000";
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const { wsId: id } = await t.registerWebsocket(ws, "");
        const c = await t.connect();
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          const r = await c.query(
            `SELECT count(*)::INTEGER AS n FROM ${cat}.main.users`,
          );
          return Number(r.toArray()[0].toJSON().n);
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws.close();
        }
      });
      expect(rows).toBeGreaterThanOrEqual(3);
    },
    TIMEOUT,
  );

  test(
    "binary-split is non-destructive: the app keeps the socket",
    async () => {
      const res = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        let appSawBinary = 0;
        ws.addEventListener("message", (ev) => {
          if (ev.data instanceof ArrayBuffer) appSawBinary++;
        });
        const { wsId: id } = await t.registerWebsocket(ws, "");
        const c = await t.connect();
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          await c.query(`SELECT count(*) FROM ${cat}.main.users`);
          return { onmessageNull: ws.onmessage === null, appSawBinary };
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws.close();
        }
      }, name("wssplit"));
      expect(res.onmessageNull).toBe(true);
      expect(res.appSawBinary).toBeGreaterThan(0);
    },
    TIMEOUT,
  );

  test(
    "replaceWebsocket rebinds a live catalog without DETACH/ATTACH, even mid-request",
    async () => {
      const counts = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const { wsId: id } = await t.registerWebsocket(ws, "");
        const c = await t.connect();
        const count = async (): Promise<number> => {
          const r = await c.query(
            `SELECT count(*) AS n FROM ${cat}.main.users`,
          );
          return Number(r.toArray()[0]!.toJSON().n);
        };
        let ws2: WebSocket | undefined;
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          const before = await count();
          const inflight = count().catch(() => -1);
          ws.close();
          ws2 = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
          await t.replaceWebsocket(id, ws2, "");
          await inflight;
          const after = await count();
          return { before, after };
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws2?.close();
        }
      }, name("wsrepl"));
      expect(counts.before).toBe(3);
      expect(counts.after).toBe(3);
    },
    TIMEOUT,
  );

  test(
    "PUSH on a registered socket reaches the catalog after invalidate",
    async () => {
      const catA = name("pushA");
      const view = "pushed_view";
      // Step 1 (in-page): open+register socket, attach, prime the catalog.
      await h.page.evaluate(
        async ({ cat, wsUrlCatalog }) => {
          const t = globalThis.__n6kTest!;
          const ws = new WebSocket(wsUrlCatalog);
          const { wsId: id } = await t.registerWebsocket(ws, "");
          const c = await t.connect();
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          await c.query(`SELECT id FROM ${cat}.main.users`);
          const scratch = globalThis as unknown as {
            __pushC?: unknown;
            __pushWs?: WebSocket;
          };
          scratch.__pushC = c;
          scratch.__pushWs = ws;
        },
        { cat: catA, wsUrlCatalog: `${await wsUrl(h)}?catalog=${catA}` },
      );

      // Step 2 (node): create the view server-side; the local catalog is now stale.
      await serverExec(
        `CREATE VIEW ${catA}.main."${view}" AS SELECT 1 AS x`,
        catA,
      );
      const stale = await h.page.evaluate(
        async ({ cat, v }) => {
          const c = (
            globalThis as unknown as {
              __pushC: { query(s: string): Promise<unknown> };
            }
          ).__pushC;
          try {
            await c.query(`SELECT x FROM ${cat}.main."${v}"`);
            return false;
          } catch {
            return true;
          }
        },
        { cat: catA, v: view },
      );
      expect(stale).toBe(true);

      // Step 3 (node): invalidate; PUSH should refresh the local catalog.
      expect(await pushInvalidate(catA, "main")).toBe(1);

      const rows = await h.page.evaluate(
        async ({ cat, v }) => {
          const c = (
            globalThis as unknown as {
              __pushC: {
                query(s: string): Promise<{
                  toArray(): Array<{ toJSON(): Record<string, unknown> }>;
                }>;
              };
              __pushWs: WebSocket;
            }
          ).__pushC;
          let out: Record<string, unknown>[] = [];
          const deadline = performance.now() + 5000;
          while (performance.now() < deadline) {
            try {
              const res = await c.query(`SELECT x FROM ${cat}.main."${v}"`);
              out = res.toArray().map((r) => r.toJSON());
              if (out.length > 0) break;
            } catch {
              /* ignore */
            }
            await new Promise((r) => setTimeout(r, 50));
          }
          const s = globalThis as unknown as { __pushWs: WebSocket };
          await (c as unknown as { query(s: string): Promise<unknown> })
            .query(`DETACH ${cat}`)
            .catch(() => {});
          s.__pushWs.close();
          return out;
        },
        { cat: catA, v: view },
      );
      expect(rows).toEqual([{ x: 1 }]);
    },
    TIMEOUT,
  );

  test(
    "a bad token on /auth rejects registerWebsocket",
    async () => {
      const message = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(
          `${t.wsUrl.replace(/\/ws$/, "/auth/ws")}?catalog=${cat}`,
        );
        try {
          await t.registerWebsocket(ws, "wrong-token");
          return "resolved";
        } catch (error) {
          return String(error);
        } finally {
          ws.close();
        }
      }, name("wsbad"));
      expect(message).toContain("unauthorized");
    },
    TIMEOUT,
  );

  test(
    "a valid token on /auth attaches",
    async () => {
      const n = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(
          `${t.wsUrl.replace(/\/ws$/, "/auth/ws")}?catalog=${cat}`,
        );
        const { wsId: id } = await t.registerWebsocket(ws, "n6k-test-token");
        const c = await t.connect();
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          const r = await c.query(
            `SELECT count(*) AS n FROM ${cat}.main.users`,
          );
          return Number(r.toArray()[0]!.toJSON().n);
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws.close();
        }
      }, name("wsauth"));
      expect(n).toBe(3);
    },
    TIMEOUT,
  );

  test(
    "a refused replaceWebsocket keeps the live binding",
    async () => {
      const result = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const authUrl = `${t.wsUrl.replace(/\/ws$/, "/auth/ws")}?catalog=${cat}`;
        const ws = new WebSocket(authUrl);
        const { wsId: id } = await t.registerWebsocket(ws, "n6k-test-token");
        const c = await t.connect();
        const ws2 = new WebSocket(authUrl);
        const count = async (): Promise<number> => {
          const r = await c.query(
            `SELECT count(*) AS n FROM ${cat}.main.users`,
          );
          return Number(r.toArray()[0]!.toJSON().n);
        };
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
          let message = "resolved";
          try {
            await t.replaceWebsocket(id, ws2, "wrong-token");
          } catch (error) {
            message = String(error);
          }
          return { message, after: await count() };
        } finally {
          await c.query(`DETACH ${cat}`).catch(() => {});
          await c.close();
          ws.close();
          ws2.close();
        }
      }, name("wsreplbad"));
      expect(result.message).toContain("unauthorized");
      expect(result.after).toBe(3);
    },
    TIMEOUT,
  );

  test(
    "registerWebsocket on a closed socket rejects",
    async () => {
      const message = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        await new Promise((resolve) => {
          ws.addEventListener("open", () => ws.close(), { once: true });
          ws.addEventListener("close", resolve, { once: true });
        });
        try {
          await t.registerWebsocket(ws, "");
          return "resolved";
        } catch (error) {
          return String(error);
        }
      }, name("wsclosed"));
      expect(message).toContain("socket closed before FT_HELLO_ACK");
    },
    TIMEOUT,
  );

  test(
    "wsId with an inline token is refused",
    async () => {
      const message = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const { wsId: id } = await t.registerWebsocket(ws, "");
        const c = await t.connect();
        try {
          await c.query(
            `ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}', token 'x')`,
          );
          return "attached";
        } catch (error) {
          return String(error);
        } finally {
          await c.close();
          ws.close();
        }
      }, name("wstok"));
      expect(message).toContain("cannot be combined");
    },
    TIMEOUT,
  );

  test(
    "two catalogs share one registered socket; detaching one leaves the other",
    async () => {
      const a = name("wsmuxa");
      const b = name("wsmuxb");
      const res = await h.page.evaluate(
        async ({ a, b }) => {
          const t = globalThis.__n6kTest!;
          const ws = new WebSocket(
            `${t.wsUrl.replace(/\/ws$/, "/tuned/ws")}?catalogs=${a},${b}`,
          );
          const { wsId: id } = await t.registerWebsocket(ws, "");
          const c = await t.connect();
          const marker = async (cat: string): Promise<string> => {
            const r = await c.query(`SELECT catalog FROM ${cat}.main.marker`);
            return String(r.toArray()[0]!.toJSON().catalog);
          };
          try {
            await c.query(`ATTACH '' AS ${a} (TYPE n6k, wsId '${id}')`);
            await c.query(`ATTACH '' AS ${b} (TYPE n6k, wsId '${id}')`);
            const both = [await marker(a), await marker(b)];
            await c.query(`DETACH ${a}`);
            const after = await marker(b);
            return { both, after };
          } finally {
            await c.query(`DETACH ${b}`).catch(() => {});
            await c.close();
            ws.close();
          }
        },
        { a, b },
      );
      expect(res.both).toEqual([a, b]);
      expect(res.after).toBe(b);
    },
    TIMEOUT,
  );

  test(
    "listCatalogs returns every catalog served on the socket, pending included",
    async () => {
      const a = name("wslista");
      const b = name("wslistb");
      const listed = await h.page.evaluate(
        async ({ a, b }) => {
          const t = globalThis.__n6kTest!;
          const tuned = new WebSocket(
            `${t.wsUrl.replace(/\/ws$/, "/tuned/ws")}?catalogs=${a},${b}`,
          );
          const pending = new WebSocket(`${t.wsUrl}?catalog=slowopen_5000`);
          try {
            const { wsId: tunedId } = await t.registerWebsocket(tuned, "");
            const { wsId: pendingId } = await t.registerWebsocket(pending, "");
            return {
              tuned: await t.listCatalogs(tunedId),
              pending: await t.listCatalogs(pendingId),
            };
          } finally {
            tuned.close();
            pending.close();
          }
        },
        { a, b },
      );
      expect(listed.tuned).toEqual([a, b]);
      expect(listed.pending).toEqual(["slowopen_5000"]);
    },
    TIMEOUT,
  );

  test(
    "listCatalogs on a socket with a bound pump does not disturb its session",
    async () => {
      const a = name("wslistpump");
      const res = await h.page.evaluate(async (a: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${a}`);
        const { wsId: id } = await t.registerWebsocket(ws, "");
        const c = await t.connect();
        try {
          await c.query(`ATTACH '' AS ${a} (TYPE n6k, wsId '${id}')`);
          const [listed, count] = await Promise.all([
            t.listCatalogs(id),
            c.query(`SELECT count(*)::INTEGER AS n FROM ${a}.main.users`),
          ]);
          const again = await c.query(
            `SELECT count(*)::INTEGER AS n FROM ${a}.main.users`,
          );
          return {
            listed,
            n: Number(count.toArray()[0].toJSON().n),
            again: Number(again.toArray()[0].toJSON().n),
          };
        } finally {
          await c.query(`DETACH ${a}`).catch(() => {});
          await c.close();
          ws.close();
        }
      }, a);
      expect(res.listed).toEqual([a]);
      expect(res.n).toBe(3);
      expect(res.again).toBe(3);
    },
    TIMEOUT,
  );

  test(
    "listCatalogs rejects for an unknown wsId and for a closed socket",
    async () => {
      const a = name("wslistclose");
      const res = await h.page.evaluate(async (a: string) => {
        const t = globalThis.__n6kTest!;
        const unknown = await t
          .listCatalogs("ext_nope")
          .then(() => "resolved", String);
        const ws = new WebSocket(`${t.wsUrl}?catalog=${a}`);
        const { wsId: id } = await t.registerWebsocket(ws, "");
        const closed = new Promise((r) =>
          ws.addEventListener("close", r, { once: true }),
        );
        ws.close();
        await closed;
        const afterClose = await t
          .listCatalogs(id)
          .then(() => "resolved", String);
        return { unknown, afterClose };
      }, a);
      expect(res.unknown).toContain("unknown wsId");
      expect(res.afterClose).toContain("not open");
    },
    TIMEOUT,
  );

  test(
    "onCatalogsChanged reports catalogs the server adds and removes",
    async () => {
      const a = name("wscca");
      const b = name("wsccb");
      const id = await h.page.evaluate(async (a: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(
          `${t.wsUrl.replace(/\/ws$/, "/tuned/ws")}?catalogs=${a}`,
        );
        (globalThis as unknown as { __ccWs: WebSocket }).__ccWs = ws;
        const { wsId } = await t.registerWebsocket(ws, "");
        return wsId;
      }, a);

      expect(await addCatalog(a, b)).toBe(1);
      const added = await h.page.evaluate(
        async ({ id, b }) => {
          const t = globalThis.__n6kTest!;
          const deadline = performance.now() + 5000;
          while (
            !t.catalogsChanged.some((e) => e.wsId === id) &&
            performance.now() < deadline
          ) {
            await new Promise((r) => setTimeout(r, 20));
          }
          const c = await t.connect();
          try {
            await c.query(`ATTACH '' AS ${b} (TYPE n6k, wsId '${id}')`);
            const r = await c.query(`SELECT catalog FROM ${b}.main.marker`);
            return {
              changes: t.catalogsChanged.filter((e) => e.wsId === id),
              marker: String(r.toArray()[0]!.toJSON().catalog),
            };
          } finally {
            await c.close();
          }
        },
        { id, b },
      );
      expect(added.changes.map((e) => e.catalogs)).toEqual([[a, b]]);
      expect(added.marker).toBe(b);

      expect(await removeCatalog(b)).toBe(1);
      const removed = await h.page.evaluate(
        async ({ id, b }) => {
          const t = globalThis.__n6kTest!;
          const deadline = performance.now() + 5000;
          while (
            t.catalogsChanged.filter((e) => e.wsId === id).length < 2 &&
            performance.now() < deadline
          ) {
            await new Promise((r) => setTimeout(r, 20));
          }
          const c = await t.connect();
          try {
            const query = await c
              .query(`SELECT catalog FROM ${b}.main.marker`)
              .then(() => "resolved", String);
            return {
              changes: t.catalogsChanged.filter((e) => e.wsId === id),
              query,
            };
          } finally {
            await c.query(`DETACH ${b}`).catch(() => {});
            await c.close();
            (globalThis as unknown as { __ccWs: WebSocket }).__ccWs.close();
          }
        },
        { id, b },
      );
      expect(removed.changes.map((e) => e.catalogs)).toEqual([[a, b], [a]]);
      expect(removed.query).not.toBe("resolved");
    },
    TIMEOUT,
  );

  // Attach `cat` (over a registered socket when `wsUrlCatalog` is set, else the driver's own),
  // push an invalidate for it, and return what onInvalidated reported for it.
  async function invalidatedAfterPush(
    cat: string,
    wsUrlCatalog?: string,
  ): Promise<{ catalog: string; schemas: string[] }[]> {
    await h.page.evaluate(
      async ({ cat, wsUrlCatalog }) => {
        const t = globalThis.__n6kTest!;
        const c = await t.connect();
        if (wsUrlCatalog) {
          const ws = new WebSocket(wsUrlCatalog);
          (globalThis as unknown as { __invWs: WebSocket }).__invWs = ws;
          const { wsId: id } = await t.registerWebsocket(ws, "");
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${id}')`);
        } else {
          await c.query(`ATTACH '${t.serverUrl}' AS ${cat} (TYPE n6k)`);
        }
        await c.query(`SELECT id FROM ${cat}.main.users`);
        (globalThis as unknown as { __invC: unknown }).__invC = c;
      },
      { cat, wsUrlCatalog },
    );
    expect(await pushInvalidate(cat, "main")).toBe(1);
    return h.page.evaluate(async (cat: string) => {
      const t = globalThis.__n6kTest!;
      const deadline = performance.now() + 5000;
      while (
        !t.invalidated.some((e) => e.catalog === cat) &&
        performance.now() < deadline
      ) {
        await new Promise((r) => setTimeout(r, 20));
      }
      const s = globalThis as unknown as {
        __invC: { query(s: string): Promise<unknown>; close(): Promise<void> };
        __invWs?: WebSocket;
      };
      await s.__invC.query(`DETACH ${cat}`).catch(() => {});
      await s.__invC.close();
      s.__invWs?.close();
      return t.invalidated.filter((e) => e.catalog === cat);
    }, cat);
  }

  test(
    "onInvalidated reports a server invalidate on a registered socket",
    async () => {
      const cat = name("wsinv");
      const seen = await invalidatedAfterPush(
        cat,
        `${await wsUrl(h)}?catalog=${cat}`,
      );
      expect(seen).toEqual([{ catalog: cat, schemas: ["main"] }]);
    },
    TIMEOUT,
  );

  test(
    "onInvalidated reports a server invalidate on the driver's own socket",
    async () => {
      const cat = name("pumpinv");
      const seen = await invalidatedAfterPush(cat);
      expect(seen).toEqual([{ catalog: cat, schemas: ["main"] }]);
    },
    TIMEOUT,
  );

  test(
    "server PING is answered while two catalogs share one registered socket",
    async () => {
      const a = name("wspinga");
      const b = name("wspingb");
      const pongs = await h.page.evaluate(
        async ({ a, b, pong }) => {
          const t = globalThis.__n6kTest!;
          const ws = new WebSocket(
            `${t.wsUrl.replace(/\/ws$/, "/tuned/ws")}?catalogs=${a},${b}&ping_interval_ms=100`,
          );
          let counting = false;
          let pongs = 0;
          const send = ws.send.bind(ws);
          ws.send = (data) => {
            if (
              counting &&
              data instanceof Uint8Array &&
              t.unpackFrame(data).header.t === pong
            ) {
              pongs++;
            }
            send(data);
          };
          const { wsId: id } = await t.registerWebsocket(ws, "");
          const c = await t.connect();
          try {
            await c.query(`ATTACH '' AS ${a} (TYPE n6k, wsId '${id}')`);
            await c.query(`ATTACH '' AS ${b} (TYPE n6k, wsId '${id}')`);
            counting = true;
            await new Promise((r) => setTimeout(r, 1000));
            return pongs;
          } finally {
            await c.query(`DETACH ${a}`).catch(() => {});
            await c.query(`DETACH ${b}`).catch(() => {});
            await c.close();
            ws.close();
          }
        },
        { a, b, pong: FT.PONG },
      );
      expect(pongs).toBeGreaterThanOrEqual(3);
    },
    TIMEOUT,
  );

  test(
    "rpc: echo, error and a notification over the registered socket",
    async () => {
      const res = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(
          `${t.wsUrl.replace(/\/ws$/, "/rpc/ws")}?catalog=${cat}`,
        );
        const s = await t.registerWebsocket(ws, "");
        try {
          const echoed = await s.rpc.call("echo", { x: { a: [1, "b"] } });
          const error = await s.rpc.call("fail").then(
            () => null,
            (error_: {
              name: string;
              code: number;
              message: string;
              data: unknown;
            }) => ({
              name: error_.name,
              code: error_.code,
              message: error_.message,
              data: error_.data,
            }),
          );
          const notified = new Promise<unknown>((resolve) => {
            const off = s.rpc.on("hello", (params) => {
              off();
              resolve(params);
            });
          });
          await s.rpc.call("ping_me", { method: "hello" });
          return {
            caps: s.capabilities,
            echoed,
            error,
            notified: await notified,
          };
        } finally {
          s.close();
          ws.close();
        }
      }, name("rpc"));
      expect(res.caps).toContain("jsonrpc");
      expect(res.echoed).toEqual({ a: [1, "b"] });
      expect(res.error).toEqual({
        name: "JsonRpcError",
        code: 1234,
        message: "boom",
        data: { k: 1 },
      });
      expect(res.notified).toEqual({ ok: true });
    },
    TIMEOUT,
  );

  test(
    "rpc: a notification the server sends before any call reaches a handler added after registerWebsocket",
    async () => {
      const welcome = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(
          `${t.wsUrl.replace(/\/ws$/, "/rpc/ws")}?catalog=${cat}`,
        );
        const s = await t.registerWebsocket(ws, "");
        try {
          return await new Promise<unknown>((resolve) => {
            const off = s.rpc.on("welcome", (params) => {
              off();
              resolve(params);
            });
          });
        } finally {
          s.close();
          ws.close();
        }
      }, name("rpc_welcome"));
      expect(welcome).toEqual({ ok: true });
    },
    TIMEOUT,
  );

  test(
    "rpc: throws when the server does not grant jsonrpc",
    async () => {
      const message = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(`${t.wsUrl}?catalog=${cat}`);
        const s = await t.registerWebsocket(ws, "");
        try {
          return typeof s.rpc;
        } catch (error) {
          return String(error);
        } finally {
          s.close();
          ws.close();
        }
      }, name("rpcnocap"));
      expect(message).toContain("did not grant");
    },
    TIMEOUT,
  );

  test(
    "rpc: survives replaceWebsocket and the last DETACH; close() stops it",
    async () => {
      const res = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const url = `${t.wsUrl.replace(/\/ws$/, "/rpc/ws")}?catalog=${cat}`;
        const ws = new WebSocket(url);
        const s = await t.registerWebsocket(ws, "");
        const c = await t.connect();
        const ws2 = new WebSocket(url);
        try {
          await c.query(`ATTACH '' AS ${cat} (TYPE n6k, wsId '${s.wsId}')`);
          await t.replaceWebsocket(s.wsId, ws2, "");
          const afterReplace = await s.rpc.call("echo", { x: 1 });
          await c.query(`DETACH ${cat}`);
          const afterDetach = await s.rpc.call("echo", { x: 2 });
          s.close();
          const afterClose = await s.rpc
            .call("echo", { x: 3 }, { timeoutMs: 500 })
            .then(String, String);
          return { afterReplace, afterDetach, afterClose };
        } finally {
          await c.close();
          ws.close();
          ws2.close();
        }
      }, name("rpcrepl"));
      expect(res.afterReplace).toBe(1);
      expect(res.afterDetach).toBe(2);
      expect(res.afterClose).toContain("no reply");
    },
    TIMEOUT,
  );

  test(
    "rpc: a call after the socket closed rejects without waiting for the timeout",
    async () => {
      const message = await h.page.evaluate(async (cat: string) => {
        const t = globalThis.__n6kTest!;
        const ws = new WebSocket(
          `${t.wsUrl.replace(/\/ws$/, "/rpc/ws")}?catalog=${cat}`,
        );
        const s = await t.registerWebsocket(ws, "");
        try {
          const closed = new Promise((resolve) =>
            ws.addEventListener("close", resolve, { once: true }),
          );
          ws.close();
          await closed;
          return await s.rpc
            .call("echo", { x: 1 }, { timeoutMs: 30_000 })
            .then(String, String);
        } finally {
          s.close();
        }
      }, name("rpcclosed"));
      expect(message).toContain("not open");
    },
    TIMEOUT,
  );
});

async function wsUrl(h: BrowserHarness): Promise<string> {
  return h.page.evaluate(() => globalThis.__n6kTest!.wsUrl);
}
