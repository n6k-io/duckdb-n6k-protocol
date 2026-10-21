import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, wsCloseAll, waitFor } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";
import type { WsStatus } from "../types";

type ArrowRow = { toJSON(): Record<string, unknown> };

backendDescribe("status", "forceDrop")(`disconnect [${backendName}]`, () => {
  let handle: BackendHandle;
  const seen: WsStatus[] = [];

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup({
      onStatus: (_catalog, status) => seen.push(status),
    });
  });

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("server force-close emits a 'disconnected' status", async () => {
    expect(await waitFor(() => seen.includes("connected"), 5000)).toBe(true);
    const r = await handle.conn.query(
      `SELECT count(*) AS n FROM db.main.users`,
    );
    expect(Number((r.toArray()[0] as ArrowRow).toJSON().n)).toBeGreaterThan(0);

    await wsCloseAll();

    expect(await waitFor(() => seen.includes("disconnected"), 5000)).toBe(true);
  }, 30_000);
});
