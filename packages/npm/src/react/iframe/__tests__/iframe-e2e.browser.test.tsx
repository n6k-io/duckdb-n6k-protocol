import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import type { Frame } from "playwright-core";
import { BROWSER_AVAILABLE } from "../../../__tests__/conftest/browser-gate";
import {
  setupIframeBrowser,
  type IframeBrowserHarness,
} from "../../../__tests__/conftest/iframe-browser-harness";

const describeBrowser = BROWSER_AVAILABLE ? describe : describe.skip;
// Governs Playwright's internal waits AND each test's bun timeout (passed as the third arg to
// it() below) -- bun defaults to 5s, which would abort before a 15s Playwright wait resolved.
// beforeAll gets 2x: cold bundling + Chromium launch precede its own 15s wait.
const TIMEOUT = 15_000;

async function waitChildReady(frame: Frame): Promise<void> {
  await frame.waitForFunction(
    () => globalThis.__child?.status === "ready",
    undefined,
    {
      timeout: TIMEOUT,
    },
  );
}

describeBrowser("iframe <-> parent two-frame e2e (no n6k server)", () => {
  let h: IframeBrowserHarness;

  beforeAll(async () => {
    h = await setupIframeBrowser();
    await h.page.waitForFunction(
      () => globalThis.__parentReady === true,
      undefined,
      {
        timeout: TIMEOUT,
      },
    );
    const err = await h.page.evaluate(() => globalThis.__parentError);
    if (err) throw new Error(`parent boot failed: ${err}`);
  }, 2 * TIMEOUT);

  afterAll(async () => {
    await h?.cleanup();
  });

  async function childFrame(): Promise<Frame> {
    const handle = await h.page.waitForSelector("iframe", { timeout: TIMEOUT });
    const frame = await handle.contentFrame();
    if (!frame) throw new Error("no child frame");
    return frame;
  }

  it(
    "resolves an iframe useQuery from the parent's table (decimal included)",
    async () => {
      const frame = await childFrame();
      await waitChildReady(frame);
      const rows = await frame.evaluate(() => globalThis.__child!.rows);
      expect(rows).toEqual([
        { id: 1, name: "a", amt: 1.5 },
        { id: 2, name: "b", amt: 2.25 },
      ]);
    },
    TIMEOUT,
  );

  it(
    "survives a parent re-render — port not dropped (bug a)",
    async () => {
      await h.page.evaluate(() => globalThis.__bumpParent!());
      const frame = await childFrame();
      const rows = await frame.evaluate(() =>
        globalThis.__child!.query("SELECT 42 AS n"),
      );
      expect(rows).toEqual([{ n: 42 }]);
    },
    TIMEOUT,
  );

  it(
    "re-handshakes after an iframe reload (bug b)",
    async () => {
      await h.page.evaluate(() => {
        document.querySelector("iframe")!.contentWindow!.location.reload();
      });
      // The reload replaces contentFrame; re-fetch and wait for it to reconnect.
      const frame = await childFrame();
      await waitChildReady(frame);
      const rows = await frame.evaluate(() => globalThis.__child!.rows);
      expect(rows).toEqual([
        { id: 1, name: "a", amt: 1.5 },
        { id: 2, name: "b", amt: 2.25 },
      ]);
    },
    TIMEOUT,
  );

  // Opaque-origin regression: without the targetOrigin "*" fallback the host's
  // postMessage to a "null"-origin sandboxed srcdoc throws and never hands over the port.
  it(
    "resolves a useQuery from an OPAQUE-origin sandboxed iframe (origin null)",
    async () => {
      const handle = await h.page.waitForSelector(
        'iframe[title="child-opaque"]',
        {
          timeout: TIMEOUT,
        },
      );
      const frame = await handle.contentFrame();
      if (!frame) throw new Error("no opaque child frame");
      const origin = await frame.evaluate(() => globalThis.location.origin);
      expect(origin).toBe("null");
      await frame.waitForFunction(
        () => globalThis.__childOpaque?.status === "ready",
        undefined,
        { timeout: TIMEOUT },
      );
      const rows = await frame.evaluate(() => globalThis.__childOpaque!.rows);
      expect(rows).toEqual([
        { id: 1, name: "a", amt: 1.5 },
        { id: 2, name: "b", amt: 2.25 },
      ]);
    },
    TIMEOUT,
  );
});
