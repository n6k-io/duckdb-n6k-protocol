import { expect, beforeAll, afterAll } from "bun:test";
import {
  backend,
  backendName,
  backendDescribe,
  testFailing,
} from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import { N6K_URL } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

type ArrowRow = { toJSON(): Record<string, unknown> };

const ISSUER = SERVER;
const AUDIENCE = "n6k-data-service";
const OAUTH_URL = `${N6K_URL}/oauth`;
const TIMEOUT = 30_000;
// n6k_login runs the device flow natively; in the browser it's host-app-driven
// and unavailable, so browser-threads xfails.
const XFAIL = "browser-threads";

backendDescribe()(`oauth device flow [${backendName}]`, () => {
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

  testFailing(
    XFAIL,
    "n6k_login device flow then authenticated ATTACH to /oauth",
    async () => {
      const login = await handle.conn.query(
        `SELECT status FROM n6k_login('${ISSUER}', resource := '${AUDIENCE}')`,
      );
      expect((login.toArray()[0] as ArrowRow).toJSON().status).toBe("ok");

      await handle.conn.query(`ATTACH '${OAUTH_URL}' AS oauth_db (TYPE n6k)`);
      try {
        const r = await handle.conn.query(
          `SELECT count(*) AS n FROM oauth_db.main.users`,
        );
        expect(Number((r.toArray()[0] as ArrowRow).toJSON().n)).toBe(3);
      } finally {
        try {
          await handle.conn.query(`DETACH oauth_db`);
        } catch {
          /* ignore */
        }
      }
    },
    TIMEOUT,
  );
});
