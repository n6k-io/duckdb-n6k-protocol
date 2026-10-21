import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, serverExec } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

backendDescribe()(`table permissions [${backendName}]`, () => {
  let handle: BackendHandle;
  let tbl: string;
  let view: string;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
    const sfx = Math.random().toString(36).slice(2, 10);
    tbl = `perm_t_${sfx}`;
    view = `perm_v_${sfx}`;
    await serverExec(
      `CREATE TABLE db.main."${tbl}" (id INTEGER PRIMARY KEY, label VARCHAR)`,
    );
    await serverExec(
      `CREATE VIEW db.main."${view}" AS SELECT id FROM db.main."${tbl}"`,
    );
    await handle.conn.query(`SELECT * FROM n6k_invalidate_cache('db', 'main')`);
  });

  afterAll(async () => {
    try {
      await serverExec(`DROP VIEW IF EXISTS db.main."${view}"`);
      await serverExec(`DROP TABLE IF EXISTS db.main."${tbl}"`);
    } catch {
      /* ignore */
    }
    await handle?.cleanup();
  });

  async function permByName(): Promise<Map<string, Record<string, unknown>>> {
    const rows = await handle.conn.query(
      `SELECT name, kind, verbs, primary_key ` +
        `FROM n6k_table_permissions('db', schema := 'main') ` +
        `WHERE name IN ('${tbl}', '${view}')`,
    );
    return new Map(
      rows.toArray().map((r: ArrowRow) => {
        const o = r.toJSON();
        return [o.name as string, o];
      }),
    );
  }

  test("reports kind/verbs for a remote table and view", async () => {
    const byName = await permByName();
    expect(byName.get(tbl)?.kind).toBe("n6k_remote");
    expect([...(byName.get(tbl)?.verbs as Iterable<string>)]).toEqual([
      "select",
      "insert",
      "update",
      "delete",
      "alter",
      "drop",
    ]);
    expect(byName.get(view)?.kind).toBe("n6k_remote");
    expect([...(byName.get(view)?.verbs as Iterable<string>)]).toEqual([
      "select",
      "drop",
    ]);
  });

  test("reports primary_key (LIST column)", async () => {
    const byName = await permByName();
    expect([...(byName.get(tbl)?.primary_key as Iterable<string>)]).toEqual([
      "id",
    ]);
    expect([...(byName.get(view)?.primary_key as Iterable<string>)]).toEqual(
      [],
    );
  });
});
