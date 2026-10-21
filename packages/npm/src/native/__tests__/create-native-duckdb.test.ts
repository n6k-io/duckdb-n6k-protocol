import { describe, it, expect } from "bun:test";
import { createNativeDuckDB } from "../create-native-duckdb";

describe("createNativeDuckDB", () => {
  it("opens an in-memory db and runs a query", async () => {
    const { conn, dispose } = await createNativeDuckDB();
    try {
      const reader = await conn.runAndReadAll("SELECT 1 AS x");
      expect(reader.getRowObjectsJS()[0]).toEqual({ x: 1 });
    } finally {
      dispose();
    }
  });

  it("dispose() is idempotent", async () => {
    const { dispose } = await createNativeDuckDB();
    dispose();
    expect(() => dispose()).not.toThrow();
  });

  it("attaches databases passed in the `databases` option", async () => {
    const tmp = `${process.env.TMPDIR ?? "/tmp"}/n6k-attach-test-${process.pid}-${Date.now()}.duckdb`;
    const { conn, dispose } = await createNativeDuckDB({
      databases: { extra: { path: tmp, options: { TYPE: "duckdb" } } },
    });
    try {
      const reader = await conn.runAndReadAll(
        "SELECT database_name FROM duckdb_databases() WHERE database_name = 'extra'",
      );
      const rows = reader.getRowObjectsJS();
      expect(rows).toHaveLength(1);
      expect(rows[0]!.database_name).toBe("extra");
    } finally {
      dispose();
    }
  });
});
