import { test, expect, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import type { TableRef } from "../metadata/parse-sql-tables";

// n6k_parse_sql_get_tables runs the extension's local parser — in-page under coi.
describeCoi("n6k_parse_sql_get_tables [browser coi]", () => {
  let h: BrowserHarness;

  beforeAll(async () => {
    h = await setupCoiPage();
  }, 60_000);
  afterAll(async () => {
    await h?.cleanup();
  });

  async function parse(sql: string): Promise<TableRef[]> {
    return h.page.evaluate(async (s: string) => {
      const t = globalThis.__n6kTest!;
      const c = await t.connect();
      try {
        return (await t.parseSqlTables(c, s)) as unknown as Record<
          string,
          unknown
        >[];
      } finally {
        await c.close();
      }
    }, sql) as unknown as Promise<TableRef[]>;
  }

  test("unqualified: catalog/schema NULL", async () => {
    expect(await parse("SELECT * FROM foo")).toEqual([
      {
        catalog: null,
        schema: null,
        table: "foo",
        refType: "base_table",
        privilege: "select",
      },
    ]);
  });
});
