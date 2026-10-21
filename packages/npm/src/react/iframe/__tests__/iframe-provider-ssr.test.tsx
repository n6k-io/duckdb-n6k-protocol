import { describe, it, expect } from "bun:test";
import { useContext } from "react";
import { renderToString } from "react-dom/server";
import { IFrameDuckDBProvider } from "../iframe-duckdb-provider";
import { DuckDBContext, type DuckDBContextValue } from "../../duckdb-context";

function Capture({ on }: { on: (v: DuckDBContextValue | null) => void }) {
  on(useContext(DuckDBContext));
  return null;
}

function captureContext(): DuckDBContextValue | null {
  let value: DuckDBContextValue | null = null;
  renderToString(
    <IFrameDuckDBProvider parentOrigin="https://parent.example.com">
      <Capture
        on={(v) => {
          value = v;
        }}
      />
    </IFrameDuckDBProvider>,
  );
  return value;
}

describe("IFrameDuckDBProvider (SSR)", () => {
  it("renders children", () => {
    const html = renderToString(
      <IFrameDuckDBProvider parentOrigin="https://parent.example.com">
        <div>hello-iframe</div>
      </IFrameDuckDBProvider>,
    );
    expect(html).toContain("hello-iframe");
  });

  it("seeds an initializing mirror with a usable conn", () => {
    const v = captureContext();
    expect(v).not.toBeNull();
    expect(v!.status).toBe("initializing");
    expect(v!.error).toBeNull();
    expect(v!.conn).not.toBeNull();
    expect(typeof v!.conn!.query).toBe("function");
    expect(v!.connStatus).toEqual({});
    expect(v!.desired).toEqual({});
    expect(v!.attached).toEqual({});
    expect(v!.errors).toEqual({});
  });

  it("rejects registerWebsocket/replaceWebsocket (parent owns sockets)", async () => {
    const v = captureContext();
    await expect(v!.registerWebsocket({} as WebSocket, "t")).rejects.toThrow(
      /owns sockets/,
    );
    await expect(
      v!.replaceWebsocket("id", {} as WebSocket, "t"),
    ).rejects.toThrow(/owns sockets/);
  });

  it("setDesired/removeDesired/reconnect are safe before connect (no port)", () => {
    const v = captureContext();
    expect(() =>
      v!.setDesired("db", { path: "n6k://x", options: { TYPE: "n6k" } }),
    ).not.toThrow();
    expect(() => v!.removeDesired("db")).not.toThrow();
    expect(() => v!.reconnect("db")).not.toThrow();
  });

  it("a query before connect rejects rather than hanging", async () => {
    const v = captureContext();
    await expect(v!.conn!.query("SELECT 1")).rejects.toThrow(
      /not connected to parent/,
    );
  });
});
