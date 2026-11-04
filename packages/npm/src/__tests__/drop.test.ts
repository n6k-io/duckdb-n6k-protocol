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
    "after a server force-close, queries recover with no DETACH",
    async () => {
      expect(await countUsers()).toBeGreaterThan(0);

      await wsCloseAll();

      // One issued before the client sees the drop may fail; the next redials.
      let users = 0;
      const deadline = Date.now() + 10_000;
      while (users === 0 && Date.now() < deadline) {
        users = await countUsers().catch(() => 0);
      }
      expect(users).toBeGreaterThan(0);
    },
    30_000,
  );
});
