import { test, expect, describe } from "bun:test";
import { wsWorkerEndpoint, channelBytes, type ChannelLayout } from "./channel";
import { createVsockRegistry } from "./duckdb-glue";

const LAYOUT: ChannelLayout = {
  duckdbToWsCapacity: 1024,
  wsToDuckdbCapacity: 1024,
};

const OFFSET = 512;

function heapSetup() {
  const mem = new WebAssembly.Memory({ initial: 1, maximum: 4, shared: true });
  const reg = createVsockRegistry();
  reg.register("cat", mem.buffer, LAYOUT, OFFSET);
  const ws = wsWorkerEndpoint(mem.buffer, LAYOUT, OFFSET);
  return { reg, ws };
}

describe("duckdb vsock registry", () => {
  test("ring region fits the shared heap at the offset", () => {
    expect(OFFSET + channelBytes(LAYOUT)).toBeLessThanOrEqual(1 << 16);
  });

  test("close closes the ws side's inbound ring", () => {
    const { reg, ws } = heapSetup();
    expect(ws.inboundClosed()).toBe(false);
    reg.close("cat");
    expect(ws.inboundClosed()).toBe(true);
  });

  test("unregister closes the channel", () => {
    const { reg, ws } = heapSetup();
    reg.unregister("cat");
    expect(ws.inboundClosed()).toBe(true);
  });

  test("close/unregister on an unknown catalog do not throw", () => {
    const { reg } = heapSetup();
    expect(() => reg.close("nope")).not.toThrow();
    expect(() => reg.unregister("nope")).not.toThrow();
  });
});
