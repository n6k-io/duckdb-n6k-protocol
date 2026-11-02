import { describe, expect, it } from "bun:test";
import { N6kDuckDB } from "../n6k-duckdb";

const ws = () => ({}) as WebSocket;

// Never subscribed or connected, so the engine never boots.
describe("N6kDuckDB sockets/databases", () => {
  it("rejects a database on an unknown socket", () => {
    expect(
      () => new N6kDuckDB({ databases: { db: { socket: "main" } } }),
    ).toThrow("unknown socket 'main'");
  });

  it("rejects token/wsId options on a socket-backed database", () => {
    for (const key of ["token", "wsId"]) {
      expect(
        () =>
          new N6kDuckDB({
            sockets: { main: { connect: ws, token: "" } },
            databases: { db: { socket: "main", options: { [key]: "x" } } },
          }),
      ).toThrow("set the token on the socket");
    }
  });

  it("lists every database in the catalog store before boot", () => {
    const duckdb = new N6kDuckDB({ databases: { a: "x.db", b: "y.db" } });
    expect(duckdb.metadataFns.catalogs.getSnapshot()).toEqual({
      a: { version: "0" },
      b: { version: "0" },
    });
  });
});
