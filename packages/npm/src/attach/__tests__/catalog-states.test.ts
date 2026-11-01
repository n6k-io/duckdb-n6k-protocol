import { describe, expect, it } from "bun:test";
import { makeConfig } from "../attachment";
import {
  catalogStatesOf,
  createCatalogStore,
  type CatalogInputs,
} from "../catalog-states";

const cfg = makeConfig("n6k://h/a");
const unsubscribe = () => {};
const versionOf = (i: CatalogInputs) => catalogStatesOf(i).a?.version;

const base: CatalogInputs = {
  desired: {},
  attached: {},
  errors: {},
  connStatus: {},
  invalidations: {},
};

describe("catalogStatesOf", () => {
  it("goes pending → ready as the attach lands", () => {
    const pending = catalogStatesOf({ ...base, desired: { a: cfg } });
    expect(pending.a?.status).toBe("pending");
    const ready = catalogStatesOf({
      ...base,
      desired: { a: cfg },
      attached: { a: cfg },
    });
    expect(ready.a?.status).toBe("ready");
  });

  it("reports an attach error with its message", () => {
    const s = catalogStatesOf({
      ...base,
      desired: { a: cfg },
      errors: { a: { config: cfg, message: "refused" } },
    });
    expect(s.a).toMatchObject({ status: "error", message: "refused" });
  });

  it("reflects socket status on an attached catalog", () => {
    const s = catalogStatesOf({
      ...base,
      desired: { a: cfg },
      attached: { a: cfg },
      connStatus: { a: "reconnecting" },
    });
    expect(s.a?.status).toBe("reconnecting");
  });

  it("changes version on invalidation, re-attach and socket status", () => {
    const ready = { ...base, desired: { a: cfg }, attached: { a: cfg } };
    const v0 = versionOf(ready);
    expect(versionOf({ ...ready, invalidations: { a: 1 } })).not.toBe(v0);
    expect(
      versionOf({ ...ready, attached: { a: makeConfig("n6k://h/b") } }),
    ).not.toBe(v0);
    expect(versionOf({ ...ready, connStatus: { a: "connected" } })).not.toBe(
      v0,
    );
  });

  it("only covers desired catalogs", () => {
    expect(catalogStatesOf({ ...base, attached: { a: cfg } })).toEqual({});
  });
});

describe("createCatalogStore", () => {
  it("returns the same snapshot until the inputs change", () => {
    let inputs: CatalogInputs = { ...base, desired: { a: cfg } };
    const store = createCatalogStore(
      () => unsubscribe,
      () => inputs,
    );
    const first = store.getSnapshot();
    expect(store.getSnapshot()).toBe(first);
    inputs = { ...inputs, attached: { a: cfg } };
    expect(store.getSnapshot()).not.toBe(first);
  });
});
