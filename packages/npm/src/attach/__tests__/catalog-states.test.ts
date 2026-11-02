import { describe, expect, it } from "bun:test";
import {
  catalogStatesOf,
  createCatalogStore,
  type CatalogInputs,
} from "../catalog-states";

const base: CatalogInputs = { invalidations: {}, errors: {} };
const unsubscribe = () => {};

describe("catalogStatesOf", () => {
  it("lists every alias, version 0 before any invalidation", () => {
    expect(catalogStatesOf(["a", "b"], base)).toEqual({
      a: { version: "0" },
      b: { version: "0" },
    });
  });

  it("moves the version with the invalidation count", () => {
    expect(
      catalogStatesOf(["a"], { ...base, invalidations: { a: 2 } }),
    ).toEqual({ a: { version: "2" } });
  });

  it("carries the alias's error", () => {
    expect(catalogStatesOf(["a"], { ...base, errors: { a: "boom" } })).toEqual({
      a: { version: "0", error: "boom" },
    });
  });
});

describe("createCatalogStore", () => {
  it("returns the same snapshot until the inputs change", () => {
    let inputs = base;
    const store = createCatalogStore(
      () => unsubscribe,
      ["a"],
      () => inputs,
    );
    const first = store.getSnapshot();
    expect(store.getSnapshot()).toBe(first);
    inputs = { ...base, invalidations: { a: 1 } };
    expect(store.getSnapshot()).not.toBe(first);
  });
});
