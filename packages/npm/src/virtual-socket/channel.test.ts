import { test, expect, describe } from "bun:test";
import {
  duckdbEndpoint,
  wsWorkerEndpoint,
  channelBytes,
  DEFAULT_CHANNEL_LAYOUT,
  type ChannelLayout,
} from "./channel";

const LAYOUT: ChannelLayout = {
  duckdbToWsCapacity: 256,
  wsToDuckdbCapacity: 256,
};

function freshChannel(layout: ChannelLayout = LAYOUT) {
  const sab = new SharedArrayBuffer(channelBytes(layout));
  return {
    duck: duckdbEndpoint(sab, layout),
    ws: wsWorkerEndpoint(sab, layout),
  };
}

function bytes(...v: number[]): Uint8Array {
  return Uint8Array.from(v);
}

describe("virtual-socket channel", () => {
  test("duckdb→ws: a frame sent by duckdb is received by ws", () => {
    const { duck, ws } = freshChannel();
    expect(duck.send(bytes(1, 2, 3))).toBe(true);
    const got = ws.tryRecv();
    expect(got).not.toBeNull();
    expect([...got!]).toEqual([1, 2, 3]);
    expect(ws.tryRecv()).toBeNull();
  });

  test("ws→duckdb: a frame sent by ws is received by duckdb", () => {
    const { duck, ws } = freshChannel();
    expect(ws.send(bytes(9, 8, 7, 6))).toBe(true);
    const got = duck.tryRecv();
    expect(got).not.toBeNull();
    expect([...got!]).toEqual([9, 8, 7, 6]);
    expect(duck.tryRecv()).toBeNull();
  });

  test("the two directions are independent (full-duplex)", () => {
    const { duck, ws } = freshChannel();
    duck.send(bytes(10));
    ws.send(bytes(20));
    duck.send(bytes(11));
    ws.send(bytes(21));
    expect([...ws.tryRecv()!]).toEqual([10]);
    expect([...ws.tryRecv()!]).toEqual([11]);
    expect(ws.tryRecv()).toBeNull();
    expect([...duck.tryRecv()!]).toEqual([20]);
    expect([...duck.tryRecv()!]).toEqual([21]);
    expect(duck.tryRecv()).toBeNull();
  });

  test("a duckdb sender does not see its own frames (no loopback)", () => {
    const { duck } = freshChannel();
    duck.send(bytes(1, 2, 3));
    expect(duck.tryRecv()).toBeNull();
  });

  test("multiplexes many interleaved frames both ways, in order", () => {
    const { duck, ws } = freshChannel();
    for (let i = 0; i < 200; i++) {
      duck.send(bytes(i & 0xff, 0xd));
      ws.send(bytes(i & 0xff, 0xf));
      expect([...ws.tryRecv()!]).toEqual([i & 0xff, 0xd]);
      expect([...duck.tryRecv()!]).toEqual([i & 0xff, 0xf]);
    }
  });

  test("close lets a subsequent recv report drained (null)", () => {
    const { duck, ws } = freshChannel();
    ws.send(bytes(5));
    duck.close();
    expect([...duck.tryRecv()!]).toEqual([5]);
    expect(duck.tryRecv()).toBeNull();
  });

  test("default layout sizes both directions at 1 MB", () => {
    expect(DEFAULT_CHANNEL_LAYOUT.duckdbToWsCapacity).toBe(1 << 20);
    expect(DEFAULT_CHANNEL_LAYOUT.wsToDuckdbCapacity).toBe(1 << 20);
  });
});

// Phase 1: the channel is carved out of duckdb's shared WebAssembly.Memory at a
// non-zero offset (the C++ malloc pointer) rather than a dedicated SharedArrayBuffer.
describe("virtual-socket channel over shared WebAssembly.Memory at an offset", () => {
  const OFFSET = 256; // non-zero, 4-aligned, mimicking a malloc ptr

  function heapChannel(layout: ChannelLayout = LAYOUT) {
    const mem = new WebAssembly.Memory({
      initial: 1,
      maximum: 4,
      shared: true,
    });
    return {
      mem,
      duck: duckdbEndpoint(mem.buffer, layout, OFFSET),
      ws: wsWorkerEndpoint(mem.buffer, layout, OFFSET),
    };
  }

  test("full-duplex round-trip at a non-zero baseOffset", () => {
    const { duck, ws } = heapChannel();
    expect(duck.send(bytes(1, 2, 3))).toBe(true);
    expect(ws.send(bytes(9, 8))).toBe(true);
    expect([...ws.tryRecv()!]).toEqual([1, 2, 3]);
    expect([...duck.tryRecv()!]).toEqual([9, 8]);
    expect(ws.tryRecv()).toBeNull();
    expect(duck.tryRecv()).toBeNull();
  });

  test("endpoints keep working across a heap grow", () => {
    const { mem, duck, ws } = heapChannel();
    expect(duck.send(bytes(4, 4))).toBe(true);
    mem.grow(1);
    expect([...ws.tryRecv()!]).toEqual([4, 4]);
    expect(ws.send(bytes(5, 5, 5))).toBe(true);
    expect([...duck.tryRecv()!]).toEqual([5, 5, 5]);
  });

  test("throws when the region would overrun the buffer", () => {
    const mem = new WebAssembly.Memory({
      initial: 1,
      maximum: 4,
      shared: true,
    });
    const tooHigh = mem.buffer.byteLength - 8; // no room for a full channel
    expect(() => duckdbEndpoint(mem.buffer, LAYOUT, tooHigh)).toThrow(
      /exceeds buffer length/,
    );
  });
});
