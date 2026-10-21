import { test, expect, describe, beforeAll, afterAll } from "bun:test";
import { describeCoi, setupCoiPage } from "./conftest/coi-page";
import type { BrowserHarness } from "./conftest/browser-harness";
import {
  catalogOf,
  stringToTableRef,
  tableRefsOverlap,
  type TableRef,
} from "../react/parse-sql-tables";

const foo = (catalog: string | null, schema: string | null): TableRef => ({
  catalog,
  schema,
  table: "foo",
  refType: "base_table",
  privilege: "select",
});

const privOf = (rows: TableRef[], table: string) =>
  rows.find((r) => r.table === table)?.privilege;

// Pure logic — no wasm; runs everywhere.
describe("catalogOf", () => {
  test("ambiguous table-function prefix pins the catalog", () => {
    expect(catalogOf({ ...foo(null, "db"), refType: "table_function" })).toBe(
      "db",
    );
  });

  test("unprefixed table function is unpinnable", () => {
    expect(
      catalogOf({ ...foo(null, null), refType: "table_function" }),
    ).toBeNull();
  });
});

describe("tableRefsOverlap", () => {
  test("exact match", () => {
    expect(tableRefsOverlap([foo("db", "main")], [foo("db", "main")])).toBe(
      true,
    );
  });

  test("NULL on left side acts as wildcard", () => {
    expect(tableRefsOverlap([foo(null, null)], [foo("db", "main")])).toBe(true);
  });

  test("NULL on right side acts as wildcard", () => {
    expect(tableRefsOverlap([foo("db", "main")], [foo(null, null)])).toBe(true);
  });

  test("different catalogs do not match", () => {
    expect(tableRefsOverlap([foo("db1", null)], [foo("db2", null)])).toBe(
      false,
    );
  });

  test("different tables do not match even with NULL wildcards", () => {
    expect(
      tableRefsOverlap(
        [foo(null, null)],
        [{ ...foo(null, null), table: "bar" }],
      ),
    ).toBe(false);
  });

  test("empty arrays do not overlap", () => {
    expect(tableRefsOverlap([], [foo("db", "main")])).toBe(false);
    expect(tableRefsOverlap([foo("db", "main")], [])).toBe(false);
  });

  test("stringToTableRef yields a NULL-wildcard ref", () => {
    expect(stringToTableRef("foo")).toEqual({
      catalog: null,
      schema: null,
      table: "foo",
      refType: "base_table",
      privilege: "select",
    });
  });

  describe("ambiguous prefix (NULL, X, T)", () => {
    const ambig: TableRef = {
      catalog: null,
      schema: "page_db",
      table: "foo",
      refType: "base_table",
      privilege: "select",
    };

    test("matches when prefix equals other side's catalog", () => {
      const fq: TableRef = {
        catalog: "page_db",
        schema: "public",
        table: "foo",
        refType: "base_table",
        privilege: "select",
      };
      expect(tableRefsOverlap([ambig], [fq])).toBe(true);
      expect(tableRefsOverlap([fq], [ambig])).toBe(true);
    });

    test("matches when prefix equals other side's schema", () => {
      const fq: TableRef = {
        catalog: "memory",
        schema: "page_db",
        table: "foo",
        refType: "base_table",
        privilege: "select",
      };
      expect(tableRefsOverlap([ambig], [fq])).toBe(true);
    });

    test("does NOT match when prefix matches neither slot", () => {
      const fq: TableRef = {
        catalog: "other_db",
        schema: "other_schema",
        table: "foo",
        refType: "base_table",
        privilege: "select",
      };
      expect(tableRefsOverlap([ambig], [fq])).toBe(false);
    });

    test("ambiguous on one side still matches 1-part wildcard on the other", () => {
      expect(tableRefsOverlap([ambig], [stringToTableRef("foo")])).toBe(true);
    });
  });
});

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

  test("user catalog preserved (not fabricated as memory)", async () => {
    const rows = await parse("SELECT * FROM page_db.foo");
    expect(rows).toHaveLength(1);
    expect(rows[0]!.table).toBe("foo");
    expect(
      [rows[0]!.catalog, rows[0]!.schema].filter((x) => x !== null),
    ).toContain("page_db");
    expect(rows[0]!.catalog).not.toBe("memory");
  });

  test("fully qualified: catalog.schema.table", async () => {
    expect(await parse("SELECT * FROM page_db.public.foo")).toEqual([
      {
        catalog: "page_db",
        schema: "public",
        table: "foo",
        refType: "base_table",
        privilege: "select",
      },
    ]);
  });

  test("CTE: alias filtered, body's underlying table returned", async () => {
    expect(
      await parse(
        "WITH cte AS (SELECT * FROM page_db.public.bar) SELECT * FROM cte",
      ),
    ).toEqual([
      {
        catalog: "page_db",
        schema: "public",
        table: "bar",
        refType: "base_table",
        privilege: "select",
      },
    ]);
  });

  test("table function emitted with ref_type=table_function", async () => {
    expect(await parse("SELECT * FROM duckdb_tables()")).toEqual([
      {
        catalog: null,
        schema: null,
        table: "duckdb_tables",
        refType: "table_function",
        privilege: "select",
      },
    ]);
  });

  test("prefixed table function keeps its prefix", async () => {
    expect(await parse("SELECT * FROM page_db.some_fn(1)")).toEqual([
      {
        catalog: null,
        schema: "page_db",
        table: "some_fn",
        refType: "table_function",
        privilege: "select",
      },
    ]);
  });

  test("quoted identifier with dot kept as single table name", async () => {
    expect(await parse('SELECT * FROM "weird.name"')).toEqual([
      {
        catalog: null,
        schema: null,
        table: "weird.name",
        refType: "base_table",
        privilege: "select",
      },
    ]);
  });

  test("JOIN: both sides collected", async () => {
    const rows = await parse("SELECT * FROM a JOIN b ON a.id = b.id");
    expect(rows.map((r) => r.table).toSorted()).toEqual(["a", "b"]);
  });

  test("INSERT target + source table", async () => {
    const rows = await parse("INSERT INTO target SELECT * FROM source");
    expect(rows.map((r) => r.table).toSorted()).toEqual(["source", "target"]);
    expect(privOf(rows, "target")).toBe("insert");
    expect(privOf(rows, "source")).toBe("select");
  });

  test("UPDATE target labeled update, FROM labeled select", async () => {
    const rows = await parse(
      "UPDATE foo SET x = bar.x FROM bar WHERE foo.id = bar.id",
    );
    expect(privOf(rows, "foo")).toBe("update");
    expect(privOf(rows, "bar")).toBe("select");
  });

  test("DELETE target labeled delete", async () => {
    expect(privOf(await parse("DELETE FROM foo WHERE id = 5"), "foo")).toBe(
      "delete",
    );
  });

  test("read+write of same table yields two rows", async () => {
    const rows = await parse("INSERT INTO t SELECT * FROM t");
    expect(
      rows
        .filter((r) => r.table === "t")
        .map((r) => r.privilege)
        .toSorted(),
    ).toEqual(["insert", "select"]);
  });

  test("CREATE TABLE target labeled create", async () => {
    expect(await parse("CREATE TABLE reports (id INT)")).toEqual([
      {
        catalog: null,
        schema: null,
        table: "reports",
        refType: "base_table",
        privilege: "create",
      },
    ]);
  });

  test("CREATE TABLE AS SELECT: target create + source select", async () => {
    const rows = await parse("CREATE TABLE r AS SELECT * FROM s");
    expect(privOf(rows, "r")).toBe("create");
    expect(privOf(rows, "s")).toBe("select");
  });

  test("CREATE VIEW: view create + body select", async () => {
    const rows = await parse("CREATE VIEW v AS SELECT * FROM t");
    expect(privOf(rows, "v")).toBe("create");
    expect(privOf(rows, "t")).toBe("select");
  });

  test("DROP TABLE target labeled drop", async () => {
    expect(await parse("DROP TABLE old_data")).toEqual([
      {
        catalog: null,
        schema: null,
        table: "old_data",
        refType: "base_table",
        privilege: "drop",
      },
    ]);
  });

  test("ALTER TABLE target labeled alter", async () => {
    expect(await parse("ALTER TABLE users ADD COLUMN email VARCHAR")).toEqual([
      {
        catalog: null,
        schema: null,
        table: "users",
        refType: "base_table",
        privilege: "alter",
      },
    ]);
  });

  test("DESCRIBE reports the table as a read", async () => {
    expect(await parse("DESCRIBE foo")).toEqual([
      {
        catalog: null,
        schema: null,
        table: "foo",
        refType: "base_table",
        privilege: "select",
      },
    ]);
  });

  test("SHOW TABLES is a catalog listing — no specific table", async () => {
    expect(await parse("SHOW TABLES")).toEqual([]);
  });
});
