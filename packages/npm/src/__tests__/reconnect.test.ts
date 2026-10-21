import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, wsCloseAll, waitFor } from "./conftest/server";
import type { BackendHandle } from "./conftest/types";
import type { WsStatus } from "../types";
import type { ConnectionLike } from "../connection-shape";

type ArrowRow = { toJSON(): Record<string, unknown> };

async function countUsers(conn: ConnectionLike): Promise<number> {
  const r = await conn.query(`SELECT count(*) AS n FROM db.main.users`);
  return Number((r.toArray()[0] as ArrowRow).toJSON().n);
}

backendDescribe(
  "reconnect",
  "status",
  "forceDrop",
)(`reconnect [${backendName}]`, () => {
  let handle: BackendHandle;
  const seen: Array<{ catalog: string; status: WsStatus }> = [];
  const dbStatuses = () =>
    seen.filter((e) => e.catalog === "db").map((e) => e.status);

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup({
      onStatus: (catalog, status) => seen.push({ catalog, status }),
    });
  });

  afterAll(async () => {
    await handle?.cleanup();
  });

  test("reconnect(catalog) recovers a dropped socket", async () => {
    expect(await waitFor(() => dbStatuses().includes("connected"), 5000)).toBe(
      true,
    );
    expect(await countUsers(handle.conn)).toBeGreaterThan(0);

    await wsCloseAll();
    expect(
      await waitFor(() => dbStatuses().includes("disconnected"), 5000),
    ).toBe(true);

    const before = seen.length;
    handle.reconnect("db");
    expect(
      await waitFor(
        () =>
          seen
            .slice(before)
            .some((e) => e.catalog === "db" && e.status === "reconnecting"),
        5000,
      ),
    ).toBe(true);
    expect(
      await waitFor(
        () =>
          seen
            .slice(before)
            .some((e) => e.catalog === "db" && e.status === "connected"),
        5000,
      ),
    ).toBe(true);

    expect(await countUsers(handle.conn)).toBeGreaterThan(0);
  }, 30_000);
});
