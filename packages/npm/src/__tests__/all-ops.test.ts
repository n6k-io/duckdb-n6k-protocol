import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, resetCounts, getCounts } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const countsEnabled = backend?.caps.protocolCounts ?? false;

async function armCounts(): Promise<void> {
  if (countsEnabled) await resetCounts();
}

async function expectNoHttp(endpoint: string): Promise<void> {
  if (!countsEnabled) return;
  const c = await getCounts();
  expect(c.http[endpoint] || 0).toBe(0);
}

backendDescribe()(`all ops [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  });

  afterAll(async () => {
    try {
      await handle.conn.query(
        `SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id >= 1000')`,
      );
    } catch {
      /* ignore */
    }
    await handle?.cleanup();
  });

  test("tables_list", async () => {
    await armCounts();
    const result = await handle.conn.query(
      `SELECT table_schema, table_name FROM information_schema.tables ` +
        `WHERE table_catalog = 'db' AND table_schema NOT IN ('information_schema','pg_catalog')`,
    );
    const names = new Set(
      result.toArray().map((r: ArrowRow) => r.toJSON().table_name as string),
    );
    expect(names.has("users")).toBe(true);
    expect(names.has("products")).toBe(true);
    await expectNoHttp("GET /tables");
  });

  test("table schema probe", async () => {
    await armCounts();
    const described = await handle.conn.query("DESCRIBE db.main.users");
    const cols = new Set(
      described
        .toArray()
        .map((r: ArrowRow) => r.toJSON().column_name as string),
    );
    expect(cols.has("id")).toBe(true);
    expect(cols.has("name")).toBe(true);
    expect(cols.has("age")).toBe(true);
    await expectNoHttp("GET /main/users/schema");
  });

  test("exec", async () => {
    await armCounts();
    const result = await handle.conn.query(
      `SELECT * FROM n6k_catalog_exec('db', 'UPDATE db.main.users SET age = age WHERE id = 1')`,
    );
    expect(result.toArray().length).toBe(1);
    await expectNoHttp("POST /exec");
  });

  test("insert", async () => {
    await armCounts();
    await handle.conn.query(
      `INSERT INTO db.main.users VALUES (1777, 'Ned', 77)`,
    );
    await expectNoHttp("POST /main/users/rows");
    await handle.conn.query(
      `SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id = 1777')`,
    );
  });

  test("rpc scalar", async () => {
    await armCounts();
    const result = await handle.conn.query(
      `SELECT * FROM n6k_catalog_rpc('db', 'echo', 'hello')`,
    );
    expect(result.toArray().length).toBeGreaterThan(0);
    await expectNoHttp("POST /rpc/echo");
  });

  test("rpc table", async () => {
    await armCounts();
    const result = await handle.conn.query(
      `SELECT * FROM n6k_catalog_rpc_table('db', 'sum_table', ` +
        `(SELECT id FROM db.main.users LIMIT 3))`,
    );
    expect(result.toArray().length).toBe(1);
    await expectNoHttp("POST /rpc/sum_table");
  });
});
