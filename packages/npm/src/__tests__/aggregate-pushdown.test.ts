import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, snap } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

backendDescribe()(`aggregate pushdown [${backendName}]`, () => {
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

  test("server capabilities are advertised", async () => {
    const r = await snap(
      handle.conn,
      "SELECT * FROM n6k_capabilities('db') ORDER BY 1",
    );
    expect(r.rows).toEqual([
      { capability: "catalog_plans", level: "open" },
      { capability: "crossing", level: "connect" },
      { capability: "has_function", level: "open" },
      { capability: "schema_ddl", level: "open" },
    ]);
  });

  test("provider-backed table stays correct", async () => {
    const got = await snap(
      handle.conn,
      "SELECT count(*) AS c, min(value) AS mn FROM slowdb.main.slow",
    );
    expect(got.rows).toEqual([{ c: "bigint:3", mn: "a" }]);
  });
});
