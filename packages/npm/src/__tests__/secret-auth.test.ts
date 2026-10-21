import { test, expect, beforeAll, afterAll } from "bun:test";
import { backend, backendName, backendDescribe } from "./conftest/backend";
import { SERVER, serverUp } from "./conftest/server";
import { N6K_URL, HOST } from "./_server-gate";
import type { BackendHandle } from "./conftest/types";

type Row = { toJSON(): Record<string, unknown> };

const AUTH_TOKEN = "n6k-test-token";
const AUTH_URL = `${N6K_URL}/auth`;

backendDescribe()(`secret auth [${backendName}]`, () => {
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

  test("a TYPE n6k secret authenticates a token-less ATTACH to /auth", async () => {
    const c = `sec_ok_${Date.now()}`;
    await handle.conn.query(
      `CREATE OR REPLACE SECRET sec_auth ` +
        `(TYPE n6k, TOKEN '${AUTH_TOKEN}', SCOPE '${HOST}')`,
    );
    await handle.conn.query(`ATTACH '${AUTH_URL}' AS ${c} (TYPE n6k)`);
    const res = await handle.conn.query(
      `SELECT count(*) AS n FROM ${c}.main.users`,
    );
    expect(Number((res.toArray()[0] as Row).toJSON().n)).toBe(3);
    await handle.conn.query(`DETACH ${c}`);
    await handle.conn.query(`DROP SECRET sec_auth`);
  });

  test("a token-less ATTACH to /auth with no secret is rejected", async () => {
    const c = `sec_bad_${Date.now()}`;
    let thrown: Error | null = null;
    try {
      await handle.conn.query(`ATTACH '${AUTH_URL}' AS ${c} (TYPE n6k)`);
    } catch (error) {
      thrown = error as Error;
    }
    expect(thrown).not.toBeNull();
    expect(String(thrown!.message)).toMatch(
      /ATTACH_ERROR|timeout|unauthorized/i,
    );
  }, 15_000);
});
