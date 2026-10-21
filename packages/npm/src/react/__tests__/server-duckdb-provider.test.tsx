import { describe, it, expect } from "bun:test";
import { useContext } from "react";
import { renderToString } from "react-dom/server";
import { ServerDuckDBProvider } from "../server-duckdb-provider";
import { DuckDBContext, type DuckDBContextValue } from "../duckdb-context";
import { useDuckDB } from "../use-duckdb";
import type { ConnectionLike, ArrowLikeResult } from "../../connection-shape";

function stubConn(): ConnectionLike {
  return {
    query: async (): Promise<ArrowLikeResult> => ({
      schema: { fields: [] },
      toArray: () => [],
    }),
    close: async () => {},
  };
}

function Capture({ on }: { on: (v: DuckDBContextValue | null) => void }) {
  on(useContext(DuckDBContext));
  return null;
}

function ProbeDuckDB({ on }: { on: (conn: unknown) => void }) {
  on(useDuckDB().conn);
  return null;
}

function ProbeOutsideProvider() {
  useDuckDB();
  return null;
}

function captureContext(
  wrap: (probe: React.ReactNode) => React.ReactElement,
): DuckDBContextValue | null {
  let value: DuckDBContextValue | null = null;
  renderToString(
    wrap(
      <Capture
        on={(v) => {
          value = v;
        }}
      />,
    ),
  );
  return value;
}

describe("ServerDuckDBProvider", () => {
  it("exposes the conn and a ready status via context", () => {
    const conn = stubConn();
    const v = captureContext((probe) => (
      <ServerDuckDBProvider conn={conn}>{probe}</ServerDuckDBProvider>
    ));
    expect(v).not.toBeNull();
    expect(v!.status).toBe("ready");
    expect(v!.conn).toBe(conn);
    expect(v!.error).toBeNull();
    expect(v!.connStatus).toEqual({});
    expect(v!.errors).toEqual({});
  });

  it("seeds desired and attached from the databases prop", () => {
    const v = captureContext((probe) => (
      <ServerDuckDBProvider
        conn={stubConn()}
        databases={{
          app: "n6k://localhost:8099",
          warehouse: {
            path: "/tmp/wh.duckdb",
            options: { TYPE: "duckdb", READ_ONLY: "1" },
          },
        }}
      >
        {probe}
      </ServerDuckDBProvider>
    ));
    expect(v!.desired).toEqual({
      app: { path: "n6k://localhost:8099", options: { TYPE: "n6k" } },
      warehouse: {
        path: "/tmp/wh.duckdb",
        options: { TYPE: "duckdb", READ_ONLY: "1" },
      },
    });
    expect(v!.attached).toEqual(v!.desired);
  });

  it("empty databases prop → empty desired/attached, errors all empty", () => {
    const v = captureContext((probe) => (
      <ServerDuckDBProvider conn={stubConn()}>{probe}</ServerDuckDBProvider>
    ));
    expect(v!.desired).toEqual({});
    expect(v!.attached).toEqual({});
    expect(v!.errors).toEqual({});
  });

  it("setDesired/removeDesired are no-ops (do not throw)", () => {
    const v = captureContext((probe) => (
      <ServerDuckDBProvider conn={stubConn()}>{probe}</ServerDuckDBProvider>
    ));
    expect(() =>
      v!.setDesired("x", { path: "n6k://x", options: { TYPE: "n6k" } }),
    ).not.toThrow();
    expect(() => v!.removeDesired("x")).not.toThrow();
  });

  it("renderToString produces the children output", () => {
    const html = renderToString(
      <ServerDuckDBProvider conn={stubConn()}>
        <div>hello-ssr</div>
      </ServerDuckDBProvider>,
    );
    expect(html).toContain("hello-ssr");
  });

  it("useDuckDB throws when used outside any provider (sanity)", () => {
    expect(() => renderToString(<ProbeOutsideProvider />)).toThrow();
  });

  it("useDuckDB returns the conn supplied to ServerDuckDBProvider", () => {
    const conn = stubConn();
    let got: unknown = null;
    renderToString(
      <ServerDuckDBProvider conn={conn}>
        <ProbeDuckDB
          on={(c) => {
            got = c;
          }}
        />
      </ServerDuckDBProvider>,
    );
    expect(got).toBe(conn);
  });
});
