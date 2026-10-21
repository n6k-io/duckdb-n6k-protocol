import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const TIMEOUT = 30_000;

backendDescribe()(`update/delete [${backendName}]`, () => {
  let handle: BackendHandle;
  const created: string[] = [];

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  });

  afterAll(async () => {
    for (const name of created) {
      try {
        await handle.conn.query(
          `SELECT * FROM n6k_catalog_exec('db', 'DROP TABLE IF EXISTS db.main."${name}"')`,
        );
      } catch {
        /* ignore */
      }
    }
    await handle?.cleanup();
  });

  async function seedTable(prefix: string): Promise<string> {
    const name = `${prefix}_${Date.now()}_${created.length}`;
    created.push(name);
    await handle.conn.query(
      `SELECT * FROM n6k_catalog_exec('db', 'CREATE TABLE db.main."${name}"(id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)')`,
    );
    await handle.conn.query(
      `SELECT * FROM n6k_catalog_exec('db', 'INSERT INTO db.main."${name}" VALUES (1, ''alice'', 30), (2, ''bob'', 25), (3, ''charlie'', 35)')`,
    );
    await handle.conn.query(`SELECT * FROM n6k_invalidate_cache('db', 'main')`);
    return name;
  }

  async function row(sql: string): Promise<Record<string, unknown>> {
    const r = await handle.conn.query(sql);
    return (r.toArray()[0] as ArrowRow).toJSON();
  }

  test(
    "DELETE ... WHERE removes the matching row",
    async () => {
      const t = await seedTable("ud_del");
      const beforeRow = await row(`SELECT count(*) AS n FROM db.main."${t}"`);
      const before = Number(beforeRow.n);
      await handle.conn.query(`DELETE FROM db.main."${t}" WHERE id = 1`);
      const afterRow = await row(`SELECT count(*) AS n FROM db.main."${t}"`);
      expect(Number(afterRow.n)).toBe(before - 1);
    },
    TIMEOUT,
  );

  test(
    "UPDATE ... SET constant WHERE",
    async () => {
      const t = await seedTable("ud_upd");
      await handle.conn.query(
        `UPDATE db.main."${t}" SET name = 'Updated' WHERE id = 2`,
      );
      const r = await row(`SELECT name FROM db.main."${t}" WHERE id = 2`);
      expect(r.name).toBe("Updated");
    },
    TIMEOUT,
  );

  test(
    "UPDATE ... SET expression WHERE",
    async () => {
      const t = await seedTable("ud_expr");
      const beforeRow = await row(
        `SELECT age FROM db.main."${t}" WHERE id = 3`,
      );
      const before = Number(beforeRow.age);
      await handle.conn.query(
        `UPDATE db.main."${t}" SET age = age + 10 WHERE id = 3`,
      );
      const afterRow = await row(`SELECT age FROM db.main."${t}" WHERE id = 3`);
      expect(Number(afterRow.age)).toBe(before + 10);
    },
    TIMEOUT,
  );
});
