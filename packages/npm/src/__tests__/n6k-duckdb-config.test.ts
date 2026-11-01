import { describe, expect, it } from "bun:test";
import { N6kDuckDB } from "../n6k-duckdb";

const ws = () => ({}) as WebSocket;

// Never subscribed or connected, so the engine never boots.
describe("N6kDuckDB sockets/databases", () => {
  it("rejects a database on an unknown socket", () => {
    expect(() =>
      new N6kDuckDB().setDatabases({ db: { socket: "main" } }),
    ).toThrow("unknown socket 'main'");
  });

  it("rejects a socket with both connect and socket", () => {
    expect(() =>
      new N6kDuckDB().setSockets({
        main: { connect: ws, socket: null, token: "" } as never,
      }),
    ).toThrow("exactly one of 'connect' or 'socket'");
  });

  it("rejects token/wsId options on a socket-backed database", () => {
    for (const key of ["token", "wsId"]) {
      const duckdb = new N6kDuckDB();
      duckdb.setSockets({ main: { connect: ws, token: "" } });
      expect(() =>
        duckdb.setDatabases({
          db: { socket: "main", options: { [key]: "x" } },
        }),
      ).toThrow("set the token on the socket");
    }
  });

  it("replaces only the aliases from the previous setDatabases", () => {
    const duckdb = new N6kDuckDB();
    duckdb.setDesired("own", { path: ":memory:", options: { TYPE: "duckdb" } });
    duckdb.setDatabases({ a: "x.db", b: "y.db" });
    duckdb.setDatabases({ b: "y.db", c: "z.db" });
    expect(Object.keys(duckdb.getSnapshot().desired).toSorted()).toEqual([
      "b",
      "c",
      "own",
    ]);
  });

  it("keeps the desired object when nothing changed", () => {
    const duckdb = new N6kDuckDB();
    duckdb.setDatabases({ a: "x.db" });
    const before = duckdb.getSnapshot().desired;
    duckdb.setDatabases({ a: "x.db" });
    expect(duckdb.getSnapshot().desired).toBe(before);
  });
});
