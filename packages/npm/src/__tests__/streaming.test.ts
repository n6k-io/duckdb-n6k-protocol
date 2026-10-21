import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, resetCounts, getCounts } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const countsEnabled = backend?.caps.protocolCounts ?? false;

async function armCounts(): Promise<void> {
  if (countsEnabled) await resetCounts();
}

async function purgeStreamRows(handle: BackendHandle): Promise<void> {
  await handle.conn.query(
    `SELECT * FROM n6k_catalog_exec('db', 'DELETE FROM db.main.users WHERE id >= 10000')`,
  );
}

backendDescribe()(`streaming [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  });

  afterAll(async () => {
    try {
      await purgeStreamRows(handle);
    } catch {
      /* ignore */
    }
    await handle?.cleanup();
  });

  test("scan streams over WS", async () => {
    await armCounts();
    const result = await handle.conn.query(
      `SELECT id FROM db.main.users ORDER BY id LIMIT 3`,
    );
    expect(result.toArray().length).toBe(3);
    if (countsEnabled) {
      const c = await getCounts();
      const scanHits = Object.entries(c.http)
        .filter(([k]) => k.startsWith("POST /main/users/scan"))
        .reduce((s, [, v]) => s + v, 0);
      expect(scanHits).toBe(0);
    }
  });

  test("query streams over WS", async () => {
    await armCounts();
    const result = await handle.conn.query(
      `SELECT * FROM n6k_catalog_query('db', 'SELECT id FROM db.main.users ORDER BY id LIMIT 3')`,
    );
    expect(result.toArray().length).toBe(3);
    if (countsEnabled) {
      const c = await getCounts();
      expect(c.http["POST /query"] || 0).toBe(0);
    }
  });

  test("streaming rpc drips over WS", async () => {
    const result = await handle.conn.query(
      `SELECT n FROM db.stream_counter(5) ORDER BY n`,
    );
    const ns = result.toArray().map((r) => Number((r as ArrowRow).toJSON().n));
    expect(ns).toEqual([0, 1, 2, 3, 4]);
  });

  test("credit backpressure under large scan", async () => {
    await armCounts();
    // Must fetch rows, not an aggregate: a count() is pushed to the server and
    // comes back as a single row, which streams nothing to apply backpressure to.
    // Row count is what exhausts the credit window: the server sends a whole
    // DuckDB vector per chunk, so 40 rows would be a single chunk against a
    // budget of 8 and never pause.
    const rows = await handle.conn.query(
      `SELECT n FROM db.stream_counter(40000)`,
    );
    expect(rows.toArray().length).toBe(40_000);
    if (countsEnabled) {
      const c = await getCounts();
      expect((c.ws?.credit_pauses as number) || 0).toBeGreaterThan(0);
    }
  });
});
