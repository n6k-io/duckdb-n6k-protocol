import { test, expect } from "bun:test";
import { createCatalogRegistry } from "../workers/ws-registry";

test("ensure creates once per catalog and returns the same instance", () => {
  let made = 0;
  const reg = createCatalogRegistry<{ id: number }>(() => ({ id: ++made }));
  const a1 = reg.ensure("a");
  const a2 = reg.ensure("a");
  expect(a1).toBe(a2);
  expect(made).toBe(1);

  const b = reg.ensure("b");
  expect(b).not.toBe(a1);
  expect(reg.size()).toBe(2);
  expect(reg.catalogs().toSorted()).toEqual(["a", "b"]);
});

test("remove disposes exactly one catalog; others remain", () => {
  const disposed: string[] = [];
  const reg = createCatalogRegistry<string>(
    (c) => `res:${c}`,
    (_v, c) => disposed.push(c),
  );
  reg.ensure("a");
  reg.ensure("b");

  expect(reg.remove("a")).toBe(true);
  expect(reg.has("a")).toBe(false);
  expect(reg.has("b")).toBe(true);
  expect(reg.remove("a")).toBe(false);
  expect(disposed).toEqual(["a"]);
  expect(reg.size()).toBe(1);
});

test("clear disposes every catalog", () => {
  const disposed: string[] = [];
  const reg = createCatalogRegistry<string>(
    (c) => c,
    (v) => disposed.push(v),
  );
  reg.ensure("a");
  reg.ensure("b");
  reg.ensure("c");

  reg.clear();
  expect(reg.size()).toBe(0);
  expect(reg.catalogs()).toEqual([]);
  expect(disposed.toSorted()).toEqual(["a", "b", "c"]);
});

test("set replaces an existing entry and disposes the old one", () => {
  const disposed: string[] = [];
  const reg = createCatalogRegistry<string>(undefined, (v) => disposed.push(v));
  reg.set("a", "first");
  reg.set("a", "second");
  expect(reg.get("a")).toBe("second");
  expect(disposed).toEqual(["first"]);
  expect(reg.size()).toBe(1);
});

test("default dispose is a no-op", () => {
  const reg = createCatalogRegistry<number>(() => 1);
  reg.ensure("a");
  expect(() => reg.clear()).not.toThrow();
});

test("works with real SharedArrayBuffers (the duckdb-worker SAB use-case)", () => {
  const reg = createCatalogRegistry<SharedArrayBuffer>(
    () => new SharedArrayBuffer(8),
  );
  const a = reg.ensure("a");
  const b = reg.ensure("b");
  expect(a).toBeInstanceOf(SharedArrayBuffer);
  expect(a).not.toBe(b);
  expect(reg.get("a")).toBe(a);
});
