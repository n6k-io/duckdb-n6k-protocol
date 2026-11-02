import { describe, it, expect } from "bun:test";
import { makeConfig, renderAttachSql } from "../attach/attachment";

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
