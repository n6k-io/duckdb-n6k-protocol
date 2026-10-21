import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const TIMEOUT = 30_000;

backendDescribe()(`create/drop view [${backendName}]`, () => {
  let handle: BackendHandle;
  const tables: string[] = [];

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  });

  afterAll(async () => {
    for (const name of tables) {
      for (const sql of [
        `DROP VIEW IF EXISTS db.main."${name}_view"`,
        `DROP TABLE IF EXISTS db.main."${name}"`,
      ]) {
        try {
          await handle.conn.query(
            `SELECT * FROM n6k_catalog_exec('db', '${sql.replaceAll("'", "''")}')`,
          );
        } catch {
          /* ignore */
        }
      }
    }
    await handle?.cleanup();
  });

  async function seedTable(prefix: string): Promise<string> {
    const name = `${prefix}_${Date.now()}_${tables.length}`;
    tables.push(name);
    await handle.conn.query(
      `CREATE TABLE db.main."${name}" AS SELECT * FROM VALUES ` +
        `(1, 'alice', 30), (2, 'bob', 25), (3, 'charlie', 35) t(id, name, age)`,
    );
    return name;
  }

  test(
    "CREATE VIEW then SELECT applies the view's filter",
    async () => {
      const t = await seedTable("cv_sel");
      await handle.conn.query(
        `CREATE VIEW db.main."${t}_view" AS ` +
          `SELECT id, name FROM db.main."${t}" WHERE age > 28`,
      );
      const result = await handle.conn.query(
        `SELECT * FROM db.main."${t}_view"`,
      );
      const ids = new Set(
        result.toArray().map((r: ArrowRow) => Number(r.toJSON().id)),
      );
      expect(ids).toEqual(new Set([1, 3]));
    },
    TIMEOUT,
  );

  test(
    "CREATE OR REPLACE VIEW swaps the definition",
    async () => {
      const t = await seedTable("cv_rep");
      await handle.conn.query(
        `CREATE VIEW db.main."${t}_view" AS SELECT * FROM db.main."${t}"`,
      );
      await handle.conn.query(
        `CREATE OR REPLACE VIEW db.main."${t}_view" AS ` +
          `SELECT id FROM db.main."${t}" WHERE id = 2`,
      );
      const result = await handle.conn.query(
        `SELECT * FROM db.main."${t}_view"`,
      );
      const ids = result.toArray().map((r: ArrowRow) => Number(r.toJSON().id));
      expect(ids).toEqual([2]);
    },
    TIMEOUT,
  );

  test(
    "DROP VIEW removes it (subsequent SELECT fails)",
    async () => {
      const t = await seedTable("cv_drop");
      await handle.conn.query(
        `CREATE VIEW db.main."${t}_view" AS SELECT * FROM db.main."${t}"`,
      );
      const counted = await handle.conn.query(
        `SELECT count(*) AS n FROM db.main."${t}_view"`,
      );
      expect(Number((counted.toArray()[0] as ArrowRow).toJSON().n)).toBe(3);

      await handle.conn.query(`DROP VIEW db.main."${t}_view"`);

      let threw = false;
      try {
        await handle.conn.query(`SELECT * FROM db.main."${t}_view"`);
      } catch {
        threw = true;
      }
      expect(threw).toBe(true);
    },
    TIMEOUT,
  );

  async function throws(sql: string): Promise<boolean> {
    try {
      await handle.conn.query(sql);
      return false;
    } catch {
      return true;
    }
  }

  test(
    "CREATE / DROP VIEW through an alias that differs from the server catalog",
    async () => {
      await handle.conn.query(
        `ATTACH '${N6K_URL}' AS other (TYPE n6k, CATALOG 'db')`,
      );
      try {
        const v = `alias_view_${Date.now()}`;
        await handle.conn.query(
          `CREATE VIEW other.main."${v}" AS ` +
            `SELECT id, name FROM other.main.users WHERE age > 28`,
        );
        const result = await handle.conn.query(
          `SELECT id FROM other.main."${v}" ORDER BY id`,
        );
        expect(
          result.toArray().map((r: ArrowRow) => Number(r.toJSON().id)),
        ).toEqual([1, 3]);

        await handle.conn.query(`DROP VIEW other.main."${v}"`);
        expect(await throws(`SELECT * FROM other.main."${v}"`)).toBe(true);
      } finally {
        await handle.conn.query(`DETACH other`);
      }
    },
    TIMEOUT,
  );

  test(
    "a view created in a transaction is seen there and gone after ROLLBACK",
    async () => {
      const t = await seedTable("cv_txn");
      await handle.conn.query("BEGIN");
      await handle.conn.query(
        `CREATE VIEW db.main."${t}_view" AS SELECT id FROM db.main."${t}"`,
      );
      const counted = await handle.conn.query(
        `SELECT count(*) AS n FROM db.main."${t}_view"`,
      );
      expect(Number((counted.toArray()[0] as ArrowRow).toJSON().n)).toBe(3);
      await handle.conn.query("ROLLBACK");
      expect(await throws(`SELECT * FROM db.main."${t}_view"`)).toBe(true);
    },
    TIMEOUT,
  );

  test(
    "DROP VIEW IF EXISTS on a missing view does not throw",
    async () => {
      const t = await seedTable("cv_ifexists");
      const result = await handle.conn.query(
        `DROP VIEW IF EXISTS db.main."${t}_nonexistent"`,
      );
      expect(result).toBeDefined();
    },
    TIMEOUT,
  );
});
