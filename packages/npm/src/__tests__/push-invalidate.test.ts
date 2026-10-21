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

type ArrowRow = { toJSON(): Record<string, unknown> };

backendDescribe()(`push invalidate [${backendName}]`, () => {
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

  test("PUSH reveals a server-side view on a warm (cache-hit) catalog", async () => {
    const cat = `pinv${Date.now()}`;
    const view = "pushed_view";
    await handle.conn.query(`ATTACH '${N6K_URL}' AS ${cat} (TYPE n6k)`);
    try {
      await handle.conn.query(`SELECT id FROM ${cat}.main.users`);

      await serverExec(
        `CREATE VIEW ${cat}.main."${view}" AS SELECT 1 AS x`,
        cat,
      );

      let sawStale = false;
      try {
        await handle.conn.query(`SELECT x FROM ${cat}.main."${view}"`);
      } catch {
        sawStale = true;
      }
      expect(sawStale).toBe(true);

      expect(await pushInvalidate(cat, "main")).toBe(1);

      let rows: unknown[] = [];
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        try {
          const res = await handle.conn.query(
            `SELECT x FROM ${cat}.main."${view}"`,
          );
          rows = res.toArray().map((r: ArrowRow) => r.toJSON());
          if (rows.length > 0) break;
        } catch {
          /* ignore */
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      expect(rows).toEqual([{ x: 1 }]);
    } finally {
      try {
        await handle.conn.query(`DETACH ${cat}`);
      } catch {
        /* ignore */
      }
    }
  }, 30_000);
});
