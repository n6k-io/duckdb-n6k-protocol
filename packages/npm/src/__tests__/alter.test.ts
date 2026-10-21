import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const TIMEOUT = 30_000;

backendDescribe()(`alter table [${backendName}]`, () => {
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

  async function makeTable(prefix: string): Promise<string> {
    const name = `${prefix}_${Date.now()}_${created.length}`;
    created.push(name);
    await handle.conn.query(
      `CREATE TABLE db.main."${name}" AS SELECT * FROM VALUES ` +
        `(1, 'alice', 30), (2, 'bob', 25) t(id, name, age)`,
    );
    return name;
  }

  async function columnNames(name: string): Promise<string[]> {
    const r = await handle.conn.query(`DESCRIBE db.main."${name}"`);
    return r
      .toArray()
      .map((row: ArrowRow) => row.toJSON().column_name as string);
  }

  test(
    "ADD COLUMN reflects in next DESCRIBE",
    async () => {
      const ns = await makeTable("alter_add");
      await handle.conn.query(
        `ALTER TABLE db.main."${ns}" ADD COLUMN score DOUBLE`,
      );
      expect(await columnNames(ns)).toContain("score");
    },
    TIMEOUT,
  );

  test(
    "DROP COLUMN removes the column",
    async () => {
      const ns = await makeTable("alter_drop");
      await handle.conn.query(`ALTER TABLE db.main."${ns}" DROP COLUMN age`);
      const cols = await columnNames(ns);
      expect(cols).not.toContain("age");
      expect(cols).toContain("id");
      expect(cols).toContain("name");
    },
    TIMEOUT,
  );

  test(
    "RENAME COLUMN reflects in next DESCRIBE",
    async () => {
      const ns = await makeTable("alter_rename");
      await handle.conn.query(
        `ALTER TABLE db.main."${ns}" RENAME COLUMN name TO label`,
      );
      const cols = await columnNames(ns);
      expect(cols).toContain("label");
      expect(cols).not.toContain("name");
    },
    TIMEOUT,
  );

  test(
    "ALTER COLUMN TYPE rejected with 'not supported'",
    async () => {
      const ns = await makeTable("alter_bad");
      let caught: Error | null = null;
      try {
        await handle.conn.query(
          `ALTER TABLE db.main."${ns}" ALTER COLUMN age TYPE BIGINT`,
        );
      } catch (error) {
        caught = error as Error;
      }
      expect(caught).not.toBeNull();
      expect(caught!.message.toLowerCase()).toContain("not supported");
    },
    TIMEOUT,
  );
});
