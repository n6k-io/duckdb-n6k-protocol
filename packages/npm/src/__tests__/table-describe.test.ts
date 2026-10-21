import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import { tableMetaFromDescribeRow } from "../react/use-table-meta";
import type { BackendHandle } from "./conftest/types";

const TIMEOUT = 30_000;

backendDescribe()(`table describe [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
    await handle.conn.query("CREATE TYPE mood AS ENUM ('happy', 'sad')");
    await handle.conn.query(
      "CREATE TABLE memory.main.t(id INTEGER PRIMARY KEY, " +
        "name VARCHAR NOT NULL, m mood, n INTEGER DEFAULT 5)",
    );
    await handle.conn.query(
      "CREATE VIEW memory.main.v AS SELECT id FROM memory.main.t",
    );
  }, TIMEOUT);

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("tableMetaFromDescribeRow parses columns/pk/enums", async () => {
    const res = await handle.conn.query(
      `SELECT columns, primary_key, verbs, enums ` +
        `FROM n6k_table_describe('memory', schema := 'main', "table" := 't')`,
    );
    const meta = tableMetaFromDescribeRow(res.toArray().at(0));

    expect(meta.columns.map((c) => c.name)).toEqual(["id", "name", "m", "n"]);
    const byName = Object.fromEntries(meta.columns.map((c) => [c.name, c]));
    expect(byName.name?.nullable).toBe(false);
    expect(byName.n?.nullable).toBe(true);
    expect(byName.n?.default).toBe("5");

    expect(meta.primaryKey).toEqual(["id"]);
    expect(meta.canInsert).toBe(true);
    expect(meta.canModifyColumns).toBe(true);
    expect(meta.canEditRows).toBe(true);
    expect(meta.enumValues).toEqual({ m: ["happy", "sad"] });
  });

  test("a view is not row-editable", async () => {
    const res = await handle.conn.query(
      `SELECT columns, primary_key, verbs, enums ` +
        `FROM n6k_table_describe('memory', schema := 'main', "table" := 'v')`,
    );
    const meta = tableMetaFromDescribeRow(res.toArray().at(0));

    expect(meta.canInsert).toBe(false);
    expect(meta.canEditRows).toBe(false);
    expect(meta.primaryKey).toEqual([]);
    expect(meta.columns.map((c) => c.name)).toEqual(["id"]);
  });
});
