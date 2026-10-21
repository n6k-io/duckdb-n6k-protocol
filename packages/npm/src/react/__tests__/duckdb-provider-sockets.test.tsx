import { describe, it, expect } from "bun:test";
import { renderToString } from "react-dom/server";
import { DuckDBProvider } from "../duckdb-provider";

const ws = () => ({}) as WebSocket;

describe("DuckDBProvider sockets validation", () => {
  it("rejects a database on an unknown socket", () => {
    expect(() =>
      renderToString(
        <DuckDBProvider databases={{ db: { socket: "main" } }}>
          {null}
        </DuckDBProvider>,
      ),
    ).toThrow("unknown socket 'main'");
  });

  it("rejects a socket with both connect and socket", () => {
    expect(() =>
      renderToString(
        <DuckDBProvider
          sockets={{
            main: { connect: ws, socket: null, token: "" } as never,
          }}
        >
          {null}
        </DuckDBProvider>,
      ),
    ).toThrow("exactly one of 'connect' or 'socket'");
  });

  it("rejects token/wsId options on a socket-backed database", () => {
    for (const key of ["token", "wsId"]) {
      expect(() =>
        renderToString(
          <DuckDBProvider
            sockets={{ main: { connect: ws, token: "" } }}
            databases={{ db: { socket: "main", options: { [key]: "x" } } }}
          >
            {null}
          </DuckDBProvider>,
        ),
      ).toThrow("set the token on the socket");
    }
  });

  it("renders a valid declaration", () => {
    expect(() =>
      renderToString(
        <DuckDBProvider
          sockets={{ main: { connect: ws, token: "" } }}
          databases={{ db: { socket: "main", catalog: "warehouse" } }}
        >
          {null}
        </DuckDBProvider>,
      ),
    ).not.toThrow();
  });
});
