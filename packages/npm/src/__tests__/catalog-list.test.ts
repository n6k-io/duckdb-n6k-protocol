import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, resetCounts, getCounts } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

backendDescribe()(`catalog list [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    if (backend?.caps.protocolCounts) await resetCounts();
    handle = await backend!.setup();
  });

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("schemas discovered (main, test_schema)", async () => {
    const rows = await handle.conn.query(
      `SELECT schema_name FROM duckdb_schemas() ` +
        `WHERE database_name = 'db' AND schema_name NOT IN ('information_schema','pg_catalog')`,
    );
    const names = new Set(
      rows.toArray().map((r) => r.toJSON().schema_name as string),
    );
    expect(names.has("main")).toBe(true);
    expect(names.has("test_schema")).toBe(true);
  });

  const countTest = backend?.caps.protocolCounts ? test : test.skip;
  countTest("discovery routes over WS — 0 HTTP /schemas hits", async () => {
    const counts = await getCounts();
    expect(counts.http["GET /schemas"] || 0).toBe(0);
  });
});
