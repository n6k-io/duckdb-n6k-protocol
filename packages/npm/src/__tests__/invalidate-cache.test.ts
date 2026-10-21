import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, serverExec } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const TIMEOUT = 30_000;

backendDescribe()(`invalidate cache [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
    await handle.conn.query(
      `SELECT table_name FROM information_schema.tables ` +
        `WHERE table_catalog = 'db' AND table_schema = 'main'`,
    );
  });

  afterAll(async () => {
    await handle?.cleanup();
  });

  async function invalidate(): Promise<number> {
    const r = await handle.conn.query(
      `SELECT * FROM n6k_invalidate_cache('db', 'main')`,
    );
    const row = (r.toArray()[0] as ArrowRow).toJSON();
    return Number(Object.values(row)[0]);
  }

  async function selectX(view: string): Promise<number> {
    const r = await handle.conn.query(`SELECT x FROM db.main."${view}"`);
    return Number((r.toArray()[0] as ArrowRow).toJSON().x);
  }

  async function throwsOnSelect(view: string): Promise<boolean> {
    try {
      await handle.conn.query(`SELECT x FROM db.main."${view}"`);
      return false;
    } catch {
      return true;
    }
  }

  test(
    "invalidate reveals a server-side new view",
    async () => {
      const v = `inv_${Date.now()}`;
      await serverExec(`CREATE VIEW db.main."${v}" AS SELECT 1 AS x`);
      try {
        expect(await throwsOnSelect(v)).toBe(true);
        expect(await invalidate()).toBe(1);
        expect(await selectX(v)).toBe(1);
      } finally {
        try {
          await serverExec(`DROP VIEW IF EXISTS db.main."${v}"`);
        } catch {
          /* ignore */
        }
      }
    },
    TIMEOUT,
  );

  test(
    "invalidate clears a server-side dropped view",
    async () => {
      const v = `drop_${Date.now()}`;
      await serverExec(`CREATE VIEW db.main."${v}" AS SELECT 1 AS x`);
      await invalidate();
      expect(await selectX(v)).toBe(1);

      await serverExec(`DROP VIEW db.main."${v}"`);
      await invalidate();
      expect(await throwsOnSelect(v)).toBe(true);
    },
    TIMEOUT,
  );
});
