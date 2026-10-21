import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, snap, type Snapshot } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

const CASES: { name: string; sql: string }[] = [
  { name: "scalar int", sql: "SELECT 42 AS x" },
  { name: "scalar varchar", sql: "SELECT 'hello' AS s" },
  { name: "scalar bool", sql: "SELECT true AS b" },
  { name: "scalar null", sql: "SELECT NULL::INTEGER AS n" },
  { name: "scalar decimal", sql: "SELECT 1.23::DECIMAL(10,2) AS d" },
  {
    name: "users by id",
    sql: "SELECT id, name, age FROM db.main.users WHERE id = 1",
  },
  {
    name: "users ordered",
    sql: "SELECT id, name, age FROM db.main.users ORDER BY id",
  },
  {
    name: "products with double price",
    sql: "SELECT id, name, price FROM db.test_schema.products ORDER BY id",
  },
  {
    name: "information_schema tables filter",
    sql:
      "SELECT table_name FROM information_schema.tables " +
      "WHERE table_catalog = 'db' AND table_schema NOT IN ('information_schema','pg_catalog') " +
      "ORDER BY table_name",
  },
  {
    name: "n6k_parse_sql_get_tables",
    sql:
      "SELECT catalog, schema, table_name " +
      "FROM n6k_parse_sql_get_tables('SELECT * FROM db.main.users JOIN db.test_schema.products USING(id)') " +
      "ORDER BY table_name",
  },
];

const GOLDEN: Record<string, Snapshot> = {
  "scalar int": { fields: [{ name: "x", typeId: 2 }], rows: [{ x: 42 }] },
  "scalar varchar": {
    fields: [{ name: "s", typeId: 5 }],
    rows: [{ s: "hello" }],
  },
  "scalar bool": { fields: [{ name: "b", typeId: 6 }], rows: [{ b: true }] },
  "scalar null": { fields: [{ name: "n", typeId: 2 }], rows: [{ n: null }] },
  "scalar decimal": {
    fields: [{ name: "d", typeId: 7, scale: 2 }],
    rows: [{ d: "decimal:123" }],
  },
  "users by id": {
    fields: [
      { name: "id", typeId: 2 },
      { name: "name", typeId: 5 },
      { name: "age", typeId: 2 },
    ],
    rows: [{ id: 1, name: "Alice", age: 30 }],
  },
  "users ordered": {
    fields: [
      { name: "id", typeId: 2 },
      { name: "name", typeId: 5 },
      { name: "age", typeId: 2 },
    ],
    rows: [
      { id: 1, name: "Alice", age: 30 },
      { id: 2, name: "Bob", age: 25 },
      { id: 3, name: "Charlie", age: 35 },
    ],
  },
  "products with double price": {
    fields: [
      { name: "id", typeId: 2 },
      { name: "name", typeId: 5 },
      { name: "price", typeId: 3 },
    ],
    rows: [
      { id: 1, name: "Widget", price: 9.99 },
      { id: 2, name: "Gadget", price: 19.99 },
    ],
  },
  "information_schema tables filter": {
    fields: [{ name: "table_name", typeId: 5 }],
    rows: [{ table_name: "products" }, { table_name: "users" }],
  },
  n6k_parse_sql_get_tables: {
    fields: [
      { name: "catalog", typeId: 5 },
      { name: "schema", typeId: 5 },
      { name: "table_name", typeId: 5 },
    ],
    rows: [
      { catalog: "db", schema: "test_schema", table_name: "products" },
      { catalog: "db", schema: "main", table_name: "users" },
    ],
  },
};

backendDescribe()(`scalar parity [${backendName}]`, () => {
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

  for (const c of CASES) {
    test(`parity: ${c.name}`, async () => {
      const got = await snap(handle.conn, c.sql);
      expect(got).toEqual(GOLDEN[c.name]);
    });
  }
});
