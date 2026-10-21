import path from "node:path";
import { test, expect, beforeAll, afterAll } from "bun:test";
import {
  describeServer,
  SKIP_SERVER_TESTS,
  SERVER,
  N6K_URL,
} from "../../__tests__/_server-gate";
import {
  createNativeDuckDB,
  type CreatedNativeDuckDB,
} from "../create-native-duckdb";

const N6K_EXT = path.resolve(
  import.meta.dir,
  "../../../../../build/release/extension/n6k_client/n6k_client.duckdb_extension",
);

async function serverUp(): Promise<boolean> {
  try {
    const r = await fetch(`${SERVER}/debug/counts`, {
      signal: AbortSignal.timeout(2000),
    });
    return r.ok;
  } catch {
    return false;
  }
}

beforeAll(async () => {
  if (SKIP_SERVER_TESTS) return;
  if (!(await serverUp())) {
    throw new Error(`n6k test server not running on ${SERVER}`);
  }
});

describeServer("native n6k: extension + WS attach against test server", () => {
  let h: CreatedNativeDuckDB;

  beforeAll(async () => {
    h = await createNativeDuckDB({
      n6kExtensionPath: N6K_EXT,
      databases: { db: N6K_URL },
    });
  });

  afterAll(() => {
    h?.dispose();
  });

  test("ATTACH produced a TYPE n6k catalog", async () => {
    const reader = await h.conn.runAndReadAll(
      `SELECT database_name, type FROM duckdb_databases() WHERE database_name = 'db'`,
    );
    const rows = reader.getRowObjectsJS();
    expect(rows).toHaveLength(1);
    expect(rows[0]!.database_name).toBe("db");
    expect(rows[0]!.type).toBe("n6k");
  });

  test("n6k_parse_sql_get_tables works (extension symbol loaded)", async () => {
    const reader = await h.conn.runAndReadAll(
      `SELECT * FROM n6k_parse_sql_get_tables('SELECT * FROM db.main.users')`,
    );
    const rows = reader.getRowObjectsJS();
    expect(rows.length).toBeGreaterThan(0);
    const names = new Set(rows.map((r) => r.table_name as string));
    expect(names.has("users")).toBe(true);
  });

  test("information_schema lists seeded tables (catalog routes via WS)", async () => {
    const reader = await h.conn.runAndReadAll(
      `SELECT table_name FROM information_schema.tables ` +
        `WHERE table_catalog = 'db' AND table_schema NOT IN ('information_schema','pg_catalog')`,
    );
    const names = new Set(
      reader.getRowObjectsJS().map((r) => r.table_name as string),
    );
    expect(names.has("users")).toBe(true);
    expect(names.has("products")).toBe(true);
  });

  test("SELECT returns real rows with native types", async () => {
    const reader = await h.conn.runAndReadAll(
      `SELECT id, name, age FROM db.main.users WHERE id = 1`,
    );
    expect(reader.columnNames()).toEqual(["id", "name", "age"]);
    const rows = reader.getRowObjectsJS();
    expect(rows).toHaveLength(1);
    const row = rows[0]!;
    expect(row.name).toBe("Alice");
    expect(Number(row.id)).toBe(1);
    expect(Number(row.age)).toBe(30);
  });

  test("n6k_catalog_exec round-trips an UPDATE", async () => {
    const reader = await h.conn.runAndReadAll(
      `SELECT * FROM n6k_catalog_exec('db', 'UPDATE db.main.users SET age = age WHERE id = 1')`,
    );
    expect(reader.getRowObjectsJS().length).toBe(1);
  });

  test("DESCRIBE returns column metadata", async () => {
    const reader = await h.conn.runAndReadAll("DESCRIBE db.main.users");
    const cols = new Set(
      reader.getRowObjectsJS().map((r) => r.column_name as string),
    );
    expect(cols.has("id")).toBe(true);
    expect(cols.has("name")).toBe(true);
    expect(cols.has("age")).toBe(true);
  });

  test("INSERT then SELECT then DELETE round-trip", async () => {
    await h.conn.run(`INSERT INTO db.main.users VALUES (1999, 'Parity', 99)`);
    try {
      const reader = await h.conn.runAndReadAll(
        `SELECT name, age FROM db.main.users WHERE id = 1999`,
      );
      const rows = reader.getRowObjectsJS();
      expect(rows).toHaveLength(1);
      expect(rows[0]!.name).toBe("Parity");
      expect(Number(rows[0]!.age)).toBe(99);
    } finally {
      await h.conn.run(
        `SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id = 1999')`,
      );
    }
  });
});
