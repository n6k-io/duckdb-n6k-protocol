import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const TIMEOUT = 30_000;
const URL = N6K_URL;

backendDescribe()(`custom catalog [${backendName}]`, () => {
  let handle: BackendHandle;
  let seq = 0;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  });

  afterAll(async () => {
    await handle?.cleanup();
  });

  function name(prefix: string): string {
    return `${prefix}_${Date.now()}_${seq++}`;
  }

  test(
    "ATTACH as a non-db name resolves SELECT server-side",
    async () => {
      const cat = name("foobar");
      await handle.conn.query(`ATTACH '${URL}' AS ${cat} (TYPE n6k)`);
      try {
        const r = await handle.conn.query(
          `SELECT id, name FROM ${cat}.main.users ORDER BY id`,
        );
        const rows = r
          .toArray()
          .map((x: ArrowRow) => [Number(x.toJSON().id), x.toJSON().name]);
        expect(rows).toEqual([
          [1, "Alice"],
          [2, "Bob"],
          [3, "Charlie"],
        ]);
      } finally {
        await handle.conn.query(`DETACH ${cat}`);
      }
    },
    TIMEOUT,
  );

  test(
    "SCHEMA_LIST reports schemas under the client-chosen name",
    async () => {
      const cat = name("foobar");
      await handle.conn.query(`ATTACH '${URL}' AS ${cat} (TYPE n6k)`);
      try {
        const r = await handle.conn.query(
          `SELECT * FROM n6k_catalog_query('${cat}', ` +
            `'SELECT schema_name FROM information_schema.schemata ` +
            `WHERE catalog_name = ''${cat}'' ORDER BY schema_name')`,
        );
        const schemas = new Set(
          r.toArray().map((x: ArrowRow) => x.toJSON().schema_name as string),
        );
        expect(schemas.has("main")).toBe(true);
        expect(schemas.has("test_schema")).toBe(true);
      } finally {
        await handle.conn.query(`DETACH ${cat}`);
      }
    },
    TIMEOUT,
  );

  test(
    "two attaches with different names are isolated",
    async () => {
      const a = name("foobar");
      const b = name("baz");
      await handle.conn.query(`ATTACH '${URL}' AS ${a} (TYPE n6k)`);
      await handle.conn.query(`ATTACH '${URL}' AS ${b} (TYPE n6k)`);
      try {
        await handle.conn.query(
          `SELECT * FROM n6k_catalog_exec('${a}', ` +
            `'INSERT INTO ${a}.main.users VALUES (42, ''Zoe'', 99)')`,
        );
        const fa = await handle.conn.query(
          `SELECT count(*) AS n FROM ${a}.main.users`,
        );
        const fb = await handle.conn.query(
          `SELECT count(*) AS n FROM ${b}.main.users`,
        );
        expect(Number((fa.toArray()[0] as ArrowRow).toJSON().n)).toBe(4);
        expect(Number((fb.toArray()[0] as ArrowRow).toJSON().n)).toBe(3);
      } finally {
        await handle.conn.query(`DETACH ${a}`);
        await handle.conn.query(`DETACH ${b}`);
      }
    },
    TIMEOUT,
  );
});
