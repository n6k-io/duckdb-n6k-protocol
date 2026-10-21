import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp, wsCloseAll } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const ATTACH = `ATTACH '${N6K_URL}' AS db (TYPE n6k)`;

backendDescribe()(`drop [${backendName}]`, () => {
  let handle: BackendHandle;

  beforeAll(async () => {
    if (!(await serverUp())) {
      throw new Error(`n6k test server not running on ${SERVER}`);
    }
    handle = await backend!.setup();
  });

  afterAll(async () => {
    await handle?.cleanup();
  });

  async function countUsers(): Promise<number> {
    const r = await handle.conn.query(
      `SELECT count(*) AS n FROM db.main.users`,
    );
    return Number((r.toArray()[0] as ArrowRow).toJSON().n);
  }

  test("DETACH + re-ATTACH tears down and reopens", async () => {
    expect(await countUsers()).toBeGreaterThan(0);
    await handle.conn.query(`DETACH db`);
    await handle.conn.query(ATTACH);
    expect(await countUsers()).toBeGreaterThan(0);
  }, 15_000);

  const forceDropTest = backend?.caps.forceDrop ? test : test.skip;

  forceDropTest(
    "server force-close errors the next query, then DETACH + re-ATTACH recovers",
    async () => {
      expect(await countUsers()).toBeGreaterThan(0);

      await wsCloseAll();

      let caught: Error | null = null;
      try {
        await handle.conn.query(`SELECT count(*) FROM db.main.users`);
      } catch (error) {
        caught = error as Error;
      }
      expect(caught).not.toBeNull();
      expect(caught!.message.length).toBeGreaterThan(0);

      try {
        await handle.conn.query(`DETACH db`);
      } catch {
        /* ignore */
      }
      await handle.conn.query(ATTACH);
      expect(await countUsers()).toBeGreaterThan(0);
    },
    30_000,
  );
});
