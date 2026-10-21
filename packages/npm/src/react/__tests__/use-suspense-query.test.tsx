import path from "node:path";
import { Suspense } from "react";
import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { renderToReadableStream } from "react-dom/server";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  describeServer,
  SKIP_SERVER_TESTS,
  SERVER,
  N6K_URL,
} from "../../__tests__/_server-gate";
import {
  createNativeDuckDB,
  type CreatedNativeDuckDB,
} from "../../native/create-native-duckdb";
import { toWasmShape } from "../../native-wasm-adapter/adapter";
import { ServerDuckDBProvider } from "../server-duckdb-provider";
import { useSuspenseQuery } from "../use-suspense-query";
import type { ConnectionLike, ArrowLikeResult } from "../../connection-shape";

const N6K_EXT = path.resolve(
  import.meta.dir,
  "../../../../../build/release/extension/n6k_client/n6k_client.duckdb_extension",
);

async function serverUp(): Promise<boolean> {
  try {
    const r = await fetch(`${SERVER}/debug/counts`, {
      signal: AbortSignal.timeout(2000),
    });
    return r.ok;
  } catch {
    return false;
  }
}

async function streamToString(s: ReadableStream<Uint8Array>): Promise<string> {
  const reader = s.getReader();
  const chunks: Uint8Array[] = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) chunks.push(value);
  }
  return new TextDecoder().decode(Buffer.concat(chunks));
}

async function ssr(node: React.ReactElement): Promise<string> {
  const stream = await renderToReadableStream(node);
  await stream.allReady;
  return streamToString(stream);
}

function freshClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: false } } });
}

function UsersRow() {
  const r = useSuspenseQuery("SELECT id, name FROM db.main.users WHERE id = 1");
  const row = r.rows[0]!;
  return (
    <span data-testid="user">
      {String(row.id)}|{String(row.name)}
    </span>
  );
}

function Scalar() {
  const r = useSuspenseQuery("SELECT 'hello-suspense' AS s");
  return <span data-testid="scalar">{String(r.rows[0]!.s)}</span>;
}

function UnattachedCatalog() {
  useSuspenseQuery("SELECT * FROM nope_catalog.main.t");
  return <span>should not render</span>;
}

function EmptySql() {
  useSuspenseQuery("");
  return null;
}

beforeAll(async () => {
  if (SKIP_SERVER_TESTS) return;
  if (!(await serverUp())) {
    throw new Error(`n6k test server not running on ${SERVER}`);
  }
});

describeServer("useSuspenseQuery (SSR with ServerDuckDBProvider)", () => {
  let native: CreatedNativeDuckDB;
  let conn: ConnectionLike;

  beforeAll(async () => {
    native = await createNativeDuckDB({
      n6kExtensionPath: N6K_EXT,
      databases: { db: N6K_URL },
    });
    conn = toWasmShape(native.conn);
  });

  afterAll(() => {
    native?.dispose();
  });

  it("scalar query renders into the SSR HTML", async () => {
    const html = await ssr(
      <QueryClientProvider client={freshClient()}>
        <ServerDuckDBProvider conn={conn}>
          <Suspense fallback={<span>loading</span>}>
            <Scalar />
          </Suspense>
        </ServerDuckDBProvider>
      </QueryClientProvider>,
    );
    expect(html).toContain("hello-suspense");
    expect(html).not.toContain("loading");
  });

  it("attached catalog query renders real row into HTML", async () => {
    const html = await ssr(
      <QueryClientProvider client={freshClient()}>
        <ServerDuckDBProvider conn={conn} databases={{ db: N6K_URL }}>
          <Suspense fallback={<span>loading</span>}>
            <UsersRow />
          </Suspense>
        </ServerDuckDBProvider>
      </QueryClientProvider>,
    );
    // React interleaves adjacent text+expr with HTML comment markers; assert fragments separately.
    expect(html).toContain(">1");
    expect(html).toContain("Alice</span>");
  });

  it("query referencing an unattached catalog throws (does not hang)", async () => {
    let onErr: Error | null = null;
    const stream = await renderToReadableStream(
      <QueryClientProvider client={freshClient()}>
        <ServerDuckDBProvider conn={conn}>
          <Suspense fallback={<span>loading</span>}>
            <UnattachedCatalog />
          </Suspense>
        </ServerDuckDBProvider>
      </QueryClientProvider>,
      {
        onError(err) {
          onErr = err as Error;
        },
      },
    );
    try {
      await stream.allReady;
    } catch {
      /* empty */
    }
    expect(onErr).not.toBeNull();
    expect(onErr!.message).toMatch(/not attached|nope_catalog/);
  });
});

describe("useSuspenseQuery (unit)", () => {
  it("throws when there is no conn (helpful error)", async () => {
    const stubConn: ConnectionLike = {
      query: async (): Promise<ArrowLikeResult> => ({
        schema: { fields: [] },
        toArray: () => [],
      }),
      close: async () => {},
    };
    let onErr: Error | null = null;
    const stream = await renderToReadableStream(
      <QueryClientProvider client={freshClient()}>
        <ServerDuckDBProvider conn={stubConn}>
          <Suspense fallback={<span>loading</span>}>
            <EmptySql />
          </Suspense>
        </ServerDuckDBProvider>
      </QueryClientProvider>,
      {
        onError(err) {
          onErr = err as Error;
        },
      },
    );
    try {
      await stream.allReady;
    } catch {
      /* empty */
    }
    expect(onErr).not.toBeNull();
    expect(onErr!.message).toMatch(/non-empty SQL/);
  });
});
