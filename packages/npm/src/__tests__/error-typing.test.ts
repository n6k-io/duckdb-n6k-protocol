import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

async function queryError(handle: BackendHandle, sql: string): Promise<string> {
  try {
    await handle.conn.query(sql);
  } catch (error) {
    return String((error as Error).message);
  }
  throw new Error(`expected query to throw but it succeeded: ${sql}`);
}

backendDescribe()(`error typing [${backendName}]`, () => {
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

  test("EXEC against a missing table → Catalog Error with n6k tag", async () => {
    const msg = await queryError(
      handle,
      `SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM nonexistent_xyz WHERE 1=1')`,
    );
    expect(msg).toContain("Catalog Error");
    expect(msg).toContain("n6k[db]");
    expect(msg).toContain("EXEC");
    expect(msg).toContain("nonexistent_xyz");
  });

  test("EXEC with malformed SQL → Parser/Syntax Error, not IO", async () => {
    const msg = await queryError(
      handle,
      `SELECT * FROM n6k_catalog_exec('db', 'NOT VALID SQL AT ALL;')`,
    );
    expect(msg).toMatch(/Parser Error|Syntax Error/);
    expect(msg).not.toMatch(/^IO Error/);
    expect(msg).toContain("n6k[db]");
  });

  test("streaming QUERY first-frame error surfaces typed (not IO)", async () => {
    const msg = await queryError(
      handle,
      `SELECT * FROM n6k_catalog_query('db', 'SELECT bogus_col_xyz')`,
    );
    expect(msg).toContain("Binder Error");
    expect(msg).toContain("n6k[db]");
    expect(msg).toContain("QUERY");
    expect(msg).toContain("bogus_col_xyz");
  });

  test("streaming QUERY against a missing table → Catalog Error", async () => {
    const msg = await queryError(
      handle,
      `SELECT * FROM n6k_catalog_query('db', 'SELECT * FROM nonexistent_stream_xyz')`,
    );
    expect(msg).toContain("Catalog Error");
    expect(msg).toContain("n6k[db]");
    expect(msg).toContain("QUERY");
    expect(msg).toContain("nonexistent_stream_xyz");
  });
});
