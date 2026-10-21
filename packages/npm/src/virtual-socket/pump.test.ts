import { test, expect, describe } from "bun:test";
import {
  duckdbEndpoint,
  wsWorkerEndpoint,
  channelBytes,
  type ChannelLayout,
  type Endpoint,
} from "./channel";
import {
  drainOutbound,
  acceptInbound,
  flushInbound,
  createInboundBuffer,
  runPump,
} from "./pump";

const LAYOUT: ChannelLayout = {
  duckdbToWsCapacity: 256,
  wsToDuckdbCapacity: 256,
};

function freshChannel() {
  const sab = new SharedArrayBuffer(channelBytes(LAYOUT));
  return {
    duck: duckdbEndpoint(sab, LAYOUT),
    ws: wsWorkerEndpoint(sab, LAYOUT),
  };
}

function fakeSocket() {
  const sent: number[][] = [];
  return {
    sent,
    send(frame: Uint8Array) {
      sent.push([...frame]);
    },
  };
}

function bytes(...v: number[]): Uint8Array {
  return Uint8Array.from(v);
}

function drainDuck(duck: Endpoint): number[][] {
  const out: number[][] = [];
  for (;;) {
    const f = duck.tryRecv();
    if (f === null) break;
    out.push([...f]);
  }
  return out;
}

const tick = () => new Promise((r) => setTimeout(r, 5));

describe("byte pump", () => {
  test("drainOutbound forwards all queued duckdb→ws frames, in order", () => {
    const { duck, ws } = freshChannel();
    const sock = fakeSocket();
    duck.send(bytes(1));
    duck.send(bytes(2, 3));
    duck.send(bytes(4, 5, 6));
    const n = drainOutbound(ws, sock);
    expect(n).toBe(3);
    expect(sock.sent).toEqual([[1], [2, 3], [4, 5, 6]]);
    expect(drainOutbound(ws, sock)).toBe(0);
  });

  test("acceptInbound delivers a WS frame to the duckdb side (fast path)", () => {
    const { duck, ws } = freshChannel();
    const buf = createInboundBuffer();
    expect(acceptInbound(ws, buf, bytes(9, 8, 7))).toBe(true);
    expect(buf.queue.length).toBe(0);
    const got = duck.tryRecv();
    expect(got).not.toBeNull();
    expect([...got!]).toEqual([9, 8, 7]);
  });

  test("acceptInbound overflows to the buffer when the RX ring is full, in order", () => {
    const { duck, ws } = freshChannel();
    const buf = createInboundBuffer();
    const frames: number[][] = [];
    for (let i = 0; i < 40; i++) {
      const f = Array.from({ length: 10 }, () => i & 0xff);
      frames.push(f);
      expect(acceptInbound(ws, buf, Uint8Array.from(f))).toBe(true);
    }
    expect(buf.queue.length).toBeGreaterThan(0);

    const got: number[][] = [];
    for (let guard = 0; guard < 100 && got.length < 40; guard++) {
      got.push(...drainDuck(duck));
      flushInbound(ws, buf);
    }
    expect(buf.queue.length).toBe(0);
    expect(got).toEqual(frames);
  });

  test("acceptInbound returns false (fail-fast) past the overflow ceiling", () => {
    const { ws } = freshChannel();
    const buf = createInboundBuffer();
    const ceiling = 64;
    let rejected = false;
    for (let i = 0; i < 100; i++) {
      if (!acceptInbound(ws, buf, bytes(1, 2, 3, 4, 5, 6, 7, 8), ceiling)) {
        rejected = true;
        break;
      }
    }
    expect(rejected).toBe(true);
    expect(buf.bytes).toBeLessThanOrEqual(ceiling);
  });

  test("runPump forwards frames then exits when stopped", async () => {
    const { duck, ws } = freshChannel();
    const sock = fakeSocket();
    const buf = createInboundBuffer();
    duck.send(bytes(11));
    duck.send(bytes(22));

    let stop = false;
    const pump = runPump(ws, sock, buf, () => stop);

    await tick();
    expect(sock.sent).toEqual([[11], [22]]);

    duck.send(bytes(33));
    await tick();
    expect(sock.sent).toEqual([[11], [22], [33]]);

    stop = true;
    duck.send(bytes(44));
    duck.send(bytes(55));
    await pump;
    expect(sock.sent).toContainEqual([44]);
    expect(sock.sent).toContainEqual([55]);
  });

  test("runPump flushes inbound overflow as duckdb frees ring space", async () => {
    const { duck, ws } = freshChannel();
    const sock = fakeSocket();
    const buf = createInboundBuffer();

    let stop = false;
    const pump = runPump(ws, sock, buf, () => stop);

    const frames: number[][] = [];
    for (let i = 0; i < 40; i++) {
      const f = Array.from({ length: 10 }, () => i & 0xff);
      frames.push(f);
      expect(acceptInbound(ws, buf, Uint8Array.from(f))).toBe(true);
    }
    expect(buf.queue.length).toBeGreaterThan(0);

    const got: number[][] = [];
    for (let guard = 0; guard < 200 && got.length < 40; guard++) {
      got.push(...drainDuck(duck));
      await tick();
    }
    stop = true;
    duck.send(bytes(0));
    await pump;

    got.push(...drainDuck(duck));
    expect(got.slice(0, 40)).toEqual(frames);
    expect(buf.queue.length).toBe(0);
  });

  test("no deadlock: both directions saturated, single-consumer duckdb drains via the pump", async () => {
    // A blocking inbound write would deadlock when RX+TX saturate; the overflow buffer prevents it.
    const { duck, ws } = freshChannel();
    const sock = fakeSocket();
    const buf = createInboundBuffer();

    let stop = false;
    const pump = runPump(ws, sock, buf, () => stop);

    const rx: number[][] = [];
    for (let i = 0; i < 60; i++) {
      duck.send(bytes(i & 0xff));
      const f = [i & 0xff, (i + 1) & 0xff];
      rx.push(f);
      acceptInbound(ws, buf, Uint8Array.from(f));
    }

    const gotRx: number[][] = [];
    for (let guard = 0; guard < 400 && gotRx.length < 60; guard++) {
      gotRx.push(...drainDuck(duck));
      await tick();
    }

    stop = true;
    duck.send(bytes(0));
    await pump;
    gotRx.push(...drainDuck(duck));

    expect(gotRx.slice(0, 60)).toEqual(rx);
    expect(sock.sent.length).toBeGreaterThan(0);
    expect(buf.queue.length).toBe(0);
  });

  test("outbound pump preserves order across many frames and wakeups", async () => {
    const { duck, ws } = freshChannel();
    const sock = fakeSocket();
    const buf = createInboundBuffer();
    let stop = false;
    const pump = runPump(ws, sock, buf, () => stop);

    for (let i = 0; i < 50; i++) {
      duck.send(bytes(i));
      if (i % 10 === 0) await new Promise((r) => setTimeout(r, 1));
    }
    await new Promise((r) => setTimeout(r, 10));
    stop = true;
    duck.send(bytes(255));
    await pump;

    const flat = sock.sent.map((f) => f[0]);
    expect(flat.slice(0, 50)).toEqual(Array.from({ length: 50 }, (_, i) => i));
  });
});
