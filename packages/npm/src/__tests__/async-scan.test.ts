import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, snap } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

backendDescribe()(`async scan [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
    await handle.conn.query(`ATTACH '${N6K_URL}' AS slowdb (TYPE n6k)`);
  }, 30_000);

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("full scan returns correct rows (multi-type)", async () => {
    const got = await snap(
      handle.conn,
      "SELECT * FROM db.main.users ORDER BY id",
    );
    expect(got.rows).toEqual([
      { id: 1, name: "Alice", age: 30 },
      { id: 2, name: "Bob", age: 25 },
      { id: 3, name: "Charlie", age: 35 },
    ]);
  });

  test("reordered projection returns correct columns", async () => {
    const got = await snap(
      handle.conn,
      "SELECT name, id FROM db.main.users ORDER BY id",
    );
    expect(got.rows).toEqual([
      { name: "Alice", id: 1 },
      { name: "Bob", id: 2 },
      { name: "Charlie", id: 3 },
    ]);
  });

  test("pushed filter returns correct rows", async () => {
    const got = await snap(
      handle.conn,
      "SELECT id, name FROM db.main.users WHERE age > 26 ORDER BY id",
    );
    expect(got.rows).toEqual([
      { id: 1, name: "Alice" },
      { id: 3, name: "Charlie" },
    ]);
  });

  test("projection + filter together", async () => {
    const got = await snap(
      handle.conn,
      "SELECT name FROM db.main.users WHERE id = 2",
    );
    expect(got.rows).toEqual([{ name: "Bob" }]);
  });

  test("a slow full scan completes", async () => {
    const got = await snap(
      handle.conn,
      "SELECT count(*) AS c FROM slowdb.main.slow",
    );
    expect(got.rows).toEqual([{ c: "bigint:3" }]);
  }, 15_000);

  test("a slow projected scan completes", async () => {
    const got = await snap(
      handle.conn,
      "SELECT value FROM slowdb.main.slow ORDER BY value",
    );
    expect(got.rows.length).toBe(3);
  }, 15_000);
});
