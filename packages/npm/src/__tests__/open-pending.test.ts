import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

const TIMEOUT = 30_000;
const URL = N6K_URL;
// Past the client's 5s connect timeout, so only FT_OPEN_PENDING keeps the ATTACH alive.
const SLOW_MS = 7000;

backendDescribe()(`open pending [${backendName}]`, () => {
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

  test(
    "ATTACH waits out a slow open, then serves",
    async () => {
      const cat = `slowopen_${SLOW_MS}`;
      const t0 = Date.now();
      await handle.conn.query(`ATTACH '${URL}' AS ${cat} (TYPE n6k)`);
      try {
        expect(Date.now() - t0).toBeGreaterThanOrEqual(SLOW_MS);
        const r = await handle.conn.query(
          `SELECT count(*)::INTEGER AS n FROM ${cat}.main.users`,
        );
        expect(Number(r.toArray()[0].toJSON().n)).toBeGreaterThanOrEqual(3);
      } finally {
        await handle.conn.query(`DETACH ${cat}`);
      }
    },
    TIMEOUT,
  );

  test(
    "ATTACH waits out a slow authenticate, then serves",
    async () => {
      const cat = `slowauth_${SLOW_MS}`;
      const t0 = Date.now();
      await handle.conn.query(`ATTACH '${URL}' AS ${cat} (TYPE n6k)`);
      try {
        expect(Date.now() - t0).toBeGreaterThanOrEqual(SLOW_MS);
        const r = await handle.conn.query(
          `SELECT count(*)::INTEGER AS n FROM ${cat}.main.users`,
        );
        expect(Number(r.toArray()[0].toJSON().n)).toBeGreaterThanOrEqual(3);
      } finally {
        await handle.conn.query(`DETACH ${cat}`);
      }
    },
    TIMEOUT,
  );

  test(
    "a failed slow open fails the ATTACH with the backend reason",
    async () => {
      await expect(
        handle.conn.query(
          `ATTACH '${URL}' AS slowopen_fail_${SLOW_MS} (TYPE n6k)`,
        ),
      ).rejects.toThrow(/slowopen backend failed/);
    },
    TIMEOUT,
  );
});
