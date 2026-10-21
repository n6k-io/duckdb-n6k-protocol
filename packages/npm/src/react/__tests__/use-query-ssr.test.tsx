import { describe, it, expect } from "bun:test";
import { renderToString } from "react-dom/server";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ServerDuckDBProvider } from "../server-duckdb-provider";
import { useQuery } from "../use-sql-query";
import type { ConnectionLike, ArrowLikeResult } from "../../connection-shape";

function stubConn(): ConnectionLike {
  return {
    query: async (): Promise<ArrowLikeResult> => ({
      schema: { fields: [{ name: "x", type: { typeId: 0 } }] },
      toArray: () => [{ toJSON: () => ({ x: 1 }) }],
    }),
    close: async () => {},
  };
}

function Probe({ on }: { on: (status: string) => void }) {
  const r = useQuery("SELECT 1 AS x");
  on(r.status);
  return null;
}

describe("useQuery under ServerDuckDBProvider (SSR sanity)", () => {
  it("returns status='loading' on first server render", () => {
    let status = "";
    renderToString(
      <QueryClientProvider
        client={
          new QueryClient({ defaultOptions: { queries: { retry: false } } })
        }
      >
        <ServerDuckDBProvider conn={stubConn()}>
          <Probe on={(s) => (status = s)} />
        </ServerDuckDBProvider>
      </QueryClientProvider>,
    );
    expect(status).toBe("loading");
  });
});
