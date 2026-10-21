import { describe, it, expect } from "bun:test";
import {
  statusOf,
  errorOf,
  makeConfig,
  configEqual,
  configFingerprint,
  renderAttachSql,
  type AttachState,
} from "../react/attachment";

const empty = (): AttachState => ({ desired: {}, attached: {}, errors: {} });

describe("makeConfig", () => {
  it("defaults to TYPE: n6k", () => {
    const c = makeConfig("p1");
    expect(c).toEqual({ path: "p1", options: { TYPE: "n6k" } });
  });

  it("shallow-merges user options over defaults", () => {
    const c = makeConfig("p1", { token: "tok" });
    expect(c.options).toEqual({ TYPE: "n6k", token: "tok" });
  });

  it("user TYPE overrides default", () => {
    const c = makeConfig("p1", { TYPE: "postgres" });
    expect(c.options).toEqual({ TYPE: "postgres" });
  });
});

describe("configEqual / configFingerprint", () => {
  it("equal configs have equal fingerprints regardless of key order", () => {
    const a = makeConfig("p1", { token: "t", region: "us" });
    const b = makeConfig("p1", { region: "us", token: "t" });
    expect(configEqual(a, b)).toBe(true);
    expect(configFingerprint(a)).toBe(configFingerprint(b));
  });

  it("different paths are unequal", () => {
    expect(configEqual(makeConfig("p1"), makeConfig("p2"))).toBe(false);
  });

  it("different option values are unequal", () => {
    expect(
      configEqual(
        makeConfig("p1", { token: "a" }),
        makeConfig("p1", { token: "b" }),
      ),
    ).toBe(false);
  });
});

describe("renderAttachSql", () => {
  it("emits TYPE unquoted, other options quoted", () => {
    const sql = renderAttachSql("db", makeConfig("p1", { token: "tok" }));
    expect(sql).toBe("ATTACH 'p1' AS db (TYPE n6k, token 'tok');");
  });

  it("escapes single quotes in path and option values", () => {
    const sql = renderAttachSql("db", makeConfig("p'q", { token: "a'b" }));
    expect(sql).toBe("ATTACH 'p''q' AS db (TYPE n6k, token 'a''b');");
  });
});

describe("statusOf", () => {
  it("returns undefined for unmanaged catalogs", () => {
    expect(statusOf(empty(), "memory")).toBeUndefined();
  });

  it("returns ready when attached matches desired", () => {
    const c = makeConfig("p1");
    const s: AttachState = {
      desired: { db: c },
      attached: { db: c },
      errors: {},
    };
    expect(statusOf(s, "db")).toBe("ready");
  });

  it("returns pending when desired set but not attached", () => {
    const s: AttachState = {
      desired: { db: makeConfig("p1") },
      attached: {},
      errors: {},
    };
    expect(statusOf(s, "db")).toBe("pending");
  });

  it("returns pending when desired differs from attached (mid-swap)", () => {
    const s: AttachState = {
      desired: { db: makeConfig("p2") },
      attached: { db: makeConfig("p1") },
      errors: {},
    };
    expect(statusOf(s, "db")).toBe("pending");
  });

  it("returns pending when detaching (desired removed but still attached)", () => {
    const s: AttachState = {
      desired: {},
      attached: { db: makeConfig("p1") },
      errors: {},
    };
    expect(statusOf(s, "db")).toBe("pending");
  });

  it("returns error when error.config matches desired", () => {
    const c = makeConfig("p2");
    const s: AttachState = {
      desired: { db: c },
      attached: {},
      errors: { db: { config: c, message: "bad" } },
    };
    expect(statusOf(s, "db")).toBe("error");
  });

  it("ignores stale errors from a previous desired value", () => {
    const s: AttachState = {
      desired: { db: makeConfig("p3") },
      attached: {},
      errors: { db: { config: makeConfig("p2"), message: "bad" } },
    };
    expect(statusOf(s, "db")).toBe("pending");
  });
});

describe("errorOf", () => {
  it("returns the error message when current", () => {
    const c = makeConfig("p2");
    const s: AttachState = {
      desired: { db: c },
      attached: {},
      errors: { db: { config: c, message: "boom" } },
    };
    expect(errorOf(s, "db")).toBe("boom");
  });

  it("returns undefined when no error", () => {
    expect(errorOf(empty(), "db")).toBeUndefined();
  });

  it("returns undefined for stale error against new desired", () => {
    const s: AttachState = {
      desired: { db: makeConfig("p3") },
      attached: {},
      errors: { db: { config: makeConfig("p2"), message: "boom" } },
    };
    expect(errorOf(s, "db")).toBeUndefined();
  });
});
