import { describe, it, expect } from "bun:test";
import { tableFromArrays, tableFromIPC, tableToIPC } from "apache-arrow";
import type { ArrowLikeResult } from "../../../connection-shape";
import type { AttachConfig } from "../../attachment";
import { statusOf } from "../../attachment";
import { createIframeClient } from "../iframe-client";
import { createHostCore } from "../iframe-host-core";

const decodeIPC = (bytes: Uint8Array): ArrowLikeResult =>
  tableFromIPC(bytes) as unknown as ArrowLikeResult;

// Fresh buffer per call: the host detaches (transfers) it, so it can't be shared.
function fixtureIPC(): Uint8Array {
  const table = tableFromArrays({
    id: Int32Array.from([1, 2, 3]),
    name: ["a", "b", "c"],
  });
  return tableToIPC(table, "stream");
}

const CFG: AttachConfig = { path: "n6k://x", options: { TYPE: "n6k" } };

const tick = (): Promise<void> => new Promise<void>((r) => setTimeout(r, 0));

describe("iframe transport core", () => {
  it("round-trips query rows over the channel as Arrow IPC", async () => {
    const ch = new MessageChannel();
    const host = createHostCore({ runQueryIPC: async () => fixtureIPC() });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    const res = await client.conn.query("SELECT 1");
    expect(res.schema.fields.map((f) => f.name)).toEqual(["id", "name"]);
    expect(res.toArray().map((row) => row.toJSON())).toEqual([
      { id: 1, name: "a" },
      { id: 2, name: "b" },
      { id: 3, name: "c" },
    ]);
  });

  it("mirrors parent state so the catalog gate unblocks (statusOf)", async () => {
    const ch = new MessageChannel();
    const host = createHostCore({ runQueryIPC: async () => fixtureIPC() });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    expect(client.getMirror().status).toBe("initializing");
    expect(statusOf(client.getMirror(), "db")).toBeUndefined();

    host.pushState({
      status: "ready",
      error: null,
      connStatus: {},
      desired: { db: CFG },
      attached: {},
      errors: {},
    });
    await tick();
    expect(client.getMirror().status).toBe("ready");
    expect(statusOf(client.getMirror(), "db")).toBe("pending");

    host.pushState({
      status: "ready",
      error: null,
      connStatus: {},
      desired: { db: CFG },
      attached: { db: CFG },
      errors: {},
    });
    await tick();
    expect(statusOf(client.getMirror(), "db")).toBe("ready");
  });

  it("mirrors server invalidations, defaulting to none from an older parent", async () => {
    const ch = new MessageChannel();
    const host = createHostCore({ runQueryIPC: async () => fixtureIPC() });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    host.pushState({
      status: "ready",
      error: null,
      connStatus: {},
      desired: {},
      attached: {},
      errors: {},
    } as unknown as Parameters<typeof host.pushState>[0]);
    await tick();
    expect(client.getMirror().invalidations).toEqual({});

    host.pushState({
      status: "ready",
      error: null,
      connStatus: {},
      invalidations: { db: 2 },
      desired: {},
      attached: {},
      errors: {},
    });
    await tick();
    expect(client.getMirror().invalidations).toEqual({ db: 2 });
  });

  it("notifies subscribers on state change", async () => {
    const ch = new MessageChannel();
    const host = createHostCore({ runQueryIPC: async () => fixtureIPC() });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    let notifications = 0;
    const unsub = client.subscribe(() => {
      notifications++;
    });
    host.pushState({
      status: "ready",
      error: null,
      connStatus: {},
      desired: {},
      attached: {},
      errors: {},
    });
    await tick();
    expect(notifications).toBe(1);
    unsub();
    host.pushState({
      status: "ready",
      error: "boom",
      connStatus: {},
      desired: {},
      attached: {},
      errors: {},
    });
    await tick();
    expect(notifications).toBe(1);
  });

  it("rejects a query the parent never answers (timeout)", async () => {
    const ch = new MessageChannel();
    const host = createHostCore({
      runQueryIPC: () => new Promise<Uint8Array>(() => {}),
    });
    let fire: (() => void) | null = null;
    const client = createIframeClient({
      decodeIPC,
      queryTimeoutMs: 1000,
      setTimeoutFn: (fn) => {
        fire = fn;
        return 0 as unknown as ReturnType<typeof setTimeout>;
      },
      clearTimeoutFn: () => {
        fire = null;
      },
    });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    const p = client.conn.query("SELECT 1");
    expect(fire).not.toBeNull();
    fire!();
    await expect(p).rejects.toThrow(/timed out after 1000ms/);
  });

  it("teardown rejects all pending queries and blocks new ones", async () => {
    const ch = new MessageChannel();
    const host = createHostCore({
      runQueryIPC: () => new Promise<Uint8Array>(() => {}),
    });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    const p1 = client.conn.query("a");
    const p2 = client.conn.query("b");
    client.teardown("gone");
    await expect(p1).rejects.toThrow("gone");
    await expect(p2).rejects.toThrow("gone");
    await expect(client.conn.query("c")).rejects.toThrow(/torn down/);
  });

  it("rejects when no port is attached yet", async () => {
    const client = createIframeClient({ decodeIPC });
    await expect(client.conn.query("SELECT 1")).rejects.toThrow(
      /not connected to parent/,
    );
  });

  it("routes queries to a freshly swapped port", async () => {
    const ch1 = new MessageChannel();
    const ch2 = new MessageChannel();
    const host2 = createHostCore({ runQueryIPC: async () => fixtureIPC() });
    const client = createIframeClient({ decodeIPC });

    client.attachPort(ch1.port2);
    host2.bind(ch2.port1);
    client.attachPort(ch2.port2);

    const res = await client.conn.query("SELECT 1");
    expect(res.toArray().length).toBe(3);
  });

  it("forwards setDesired/removeDesired/reconnect to the host", async () => {
    const ch = new MessageChannel();
    const calls: string[] = [];
    let desired: [string, AttachConfig] | null = null;
    const host = createHostCore({
      runQueryIPC: async () => fixtureIPC(),
      onSetDesired: (c, cfg) => {
        desired = [c, cfg];
        calls.push("set");
      },
      onRemoveDesired: (c) => calls.push(`remove:${c}`),
      onReconnect: (c) => calls.push(`reconnect:${c}`),
    });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    client.setDesired("db", CFG);
    client.removeDesired("db");
    client.reconnect("db");
    await tick();
    expect(desired).toEqual(["db", CFG]);
    expect(calls).toEqual(["set", "remove:db", "reconnect:db"]);
  });

  it("propagates a host query error to the iframe", async () => {
    const ch = new MessageChannel();
    const host = createHostCore({
      runQueryIPC: async () => {
        throw new Error("boom from parent");
      },
    });
    const client = createIframeClient({ decodeIPC });
    host.bind(ch.port1);
    client.attachPort(ch.port2);

    await expect(client.conn.query("SELECT 1")).rejects.toThrow(
      "boom from parent",
    );
  });
});
