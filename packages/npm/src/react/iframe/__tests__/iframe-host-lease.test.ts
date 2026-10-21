import { describe, it, expect } from "bun:test";
import { tableFromArrays, tableFromIPC, tableToIPC } from "apache-arrow";
import type {
  ArrowLikeResult,
  ConnectionLike,
} from "../../../connection-shape";
import { createIframeClient } from "../iframe-client";
import { createHostCore } from "../iframe-host-core";
import { makeRunQueryIPC } from "../host-lease";

const decodeIPC = (bytes: Uint8Array): ArrowLikeResult =>
  tableFromIPC(bytes) as unknown as ArrowLikeResult;

const ROWS = 3;

// Fresh buffers per call: the host transfers (detaches) what it posts.
const ipcOf = (ids: number[]): Uint8Array =>
  tableToIPC(tableFromArrays({ id: Int32Array.from(ids) }), "stream");

const tick = (ms = 0): Promise<void> =>
  new Promise<void>((r) => setTimeout(r, ms));

// One shared query slot: readers arriving mid-drain pull from the same batch queue,
// so awaiting hands control to a concurrent reader — where the row stealing happens.
function stealingConn() {
  let pending: number[] | null = null;
  let readers = 0;
  return {
    async query(): Promise<Uint8Array> {
      readers++;
      pending ??= Array.from({ length: ROWS }, (_, i) => i + 1);
      const mine: number[] = [];
      while (pending.length > 0) {
        mine.push(pending.shift()!);
        await tick(1);
      }
      if (--readers === 0) pending = null;
      return ipcOf(mine);
    },
    async close(): Promise<void> {},
  };
}

const fakeDb = { connect: async () => stealingConn() };

const runFake = (conn: ConnectionLike): Promise<Uint8Array> =>
  (conn as unknown as { query(): Promise<Uint8Array> }).query();

const leaseFrom = async (): Promise<ConnectionLike> =>
  (await fakeDb.connect()) as unknown as ConnectionLike;

async function concurrentRowCounts(
  runQueryIPC: (sql: string) => Promise<Uint8Array>,
): Promise<number[]> {
  const ch = new MessageChannel();
  const host = createHostCore({ runQueryIPC });
  const client = createIframeClient({ decodeIPC });
  host.bind(ch.port1);
  client.attachPort(ch.port2);
  try {
    const results = await Promise.all([
      client.conn.query("SELECT 1"),
      client.conn.query("SELECT 2"),
    ]);
    return results.map((r) => r.toArray().length);
  } finally {
    client.teardown();
    host.dispose();
  }
}

describe("iframe host connection leasing", () => {
  // Characterization: proves the hazard is real so the guard below isn't vacuous.
  it("a shared connection splits one query's rows between concurrent RPCs", async () => {
    const shared = stealingConn();
    const counts = await concurrentRowCounts(() => shared.query());

    expect(counts.every((n) => n < ROWS)).toBe(true);
    expect(counts.reduce((a, b) => a + b, 0)).toBe(ROWS);
  });

  it("leasing a connection per RPC gives both concurrent reads all their rows", async () => {
    const counts = await concurrentRowCounts(
      makeRunQueryIPC(leaseFrom, runFake),
    );

    expect(counts).toEqual([ROWS, ROWS]);
  });

  it("closes every leased connection, including when the query throws", async () => {
    let opened = 0;
    let closed = 0;
    const db = {
      connect: async () => {
        opened++;
        return {
          query: async (): Promise<Uint8Array> => {
            throw new Error("boom");
          },
          close: async () => {
            closed++;
          },
        };
      },
    };

    const ch = new MessageChannel();
    const host = createHostCore({
      runQueryIPC: makeRunQueryIPC(
        async () => (await db.connect()) as unknown as ConnectionLike,
        runFake,
      ),
    });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    await expect(client.conn.query("SELECT 1")).rejects.toThrow("boom");
    client.teardown();
    host.dispose();

    expect(opened).toBe(1);
    expect(closed).toBe(1);
  });
});
