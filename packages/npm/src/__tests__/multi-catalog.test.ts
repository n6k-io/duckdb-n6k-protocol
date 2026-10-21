import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import {
  SERVER,
  serverUp,
  serverExec,
  pushInvalidate,
} from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";
import type { ConnectionLike } from "../connection-shape";

type Row = { toJSON(): Record<string, unknown> };

async function countN(conn: ConnectionLike, sql: string): Promise<number> {
  const r = await conn.query(sql);
  return Number((r.toArray()[0] as Row).toJSON().n);
}

backendDescribe()(`multi catalog [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  });

  afterAll(async () => {
    await handle?.cleanup();
  });

  const teardownTest = backend?.caps.forceDrop ? test : test.skip;

  async function detach(...catalogs: string[]): Promise<void> {
    for (const c of catalogs) {
      try {
        await handle.conn.query(`DETACH ${c}`);
      } catch {
        /* ignore */
      }
    }
  }

  test("two catalogs serve independent data and isolated PUSH", async () => {
    const c1 = `mca_${Date.now()}`;
    const c2 = `mcb_${Date.now()}`;
    await handle.conn.query(`ATTACH '${N6K_URL}' AS ${c1} (TYPE n6k)`);
    await handle.conn.query(`ATTACH '${N6K_URL}' AS ${c2} (TYPE n6k)`);
    try {
      await serverExec(
        `INSERT INTO ${c1}.main.users VALUES (999, 'Zed', 99)`,
        c1,
      );
      expect(
        await countN(handle.conn, `SELECT count(*) AS n FROM ${c1}.main.users`),
      ).toBe(4);
      expect(
        await countN(handle.conn, `SELECT count(*) AS n FROM ${c2}.main.users`),
      ).toBe(3);

      const view = "iso_view";
      await serverExec(`CREATE VIEW ${c1}.main."${view}" AS SELECT 1 AS x`, c1);
      await pushInvalidate(c1, "main");

      let rows: unknown[] = [];
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        try {
          const res = await handle.conn.query(
            `SELECT x FROM ${c1}.main."${view}"`,
          );
          rows = res.toArray().map((r: Row) => r.toJSON());
          if (rows.length > 0) break;
        } catch {
          /* ignore */
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      expect(rows).toEqual([{ x: 1 }]);

      let c2HasView = true;
      try {
        await handle.conn.query(`SELECT x FROM ${c2}.main."${view}"`);
      } catch {
        c2HasView = false;
      }
      expect(c2HasView).toBe(false);
    } finally {
      await detach(c1, c2);
    }
  }, 30_000);

  test("a cross-catalog JOIN reads both catalogs in one statement", async () => {
    const c1 = `mcj1_${Date.now()}`;
    const c2 = `mcj2_${Date.now()}`;
    await handle.conn.query(`ATTACH '${N6K_URL}' AS ${c1} (TYPE n6k)`);
    await handle.conn.query(`ATTACH '${N6K_URL}' AS ${c2} (TYPE n6k)`);
    try {
      expect(
        await countN(
          handle.conn,
          `SELECT count(*) AS n FROM ${c1}.main.users a ` +
            `JOIN ${c2}.main.users b ON a.id = b.id`,
        ),
      ).toBe(3);
    } finally {
      await detach(c1, c2);
    }
  }, 30_000);

  teardownTest(
    "DETACH tears down the catalog (server handler drops)",
    async () => {
      const c = `mct_${Date.now()}`;
      await handle.conn.query(`ATTACH '${N6K_URL}' AS ${c} (TYPE n6k)`);
      await handle.conn.query(`SELECT count(*) AS n FROM ${c}.main.users`);
      expect(await pushInvalidate(c, "main")).toBe(1);

      await handle.conn.query(`DETACH ${c}`);

      let n = -1;
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        n = await pushInvalidate(c, "main");
        if (n === 0) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(n).toBe(0);
    },
    15_000,
  );
});
