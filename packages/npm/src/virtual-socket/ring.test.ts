import { test, expect, describe } from "bun:test";
import {
  ringViews,
  ringBytes,
  ringTryWrite,
  ringTryRead,
  ringUsed,
  RING_HEAD,
  RING_TAIL,
} from "./ring";

const CAP = 64;

function freshRing(capacity = CAP) {
  const sab = new SharedArrayBuffer(ringBytes(capacity));
  return ringViews(sab, 0, capacity);
}

function bytes(...vals: number[]): Uint8Array {
  return Uint8Array.from(vals);
}

describe("ws-ring", () => {
  test("write then read returns the same frame", () => {
    const r = freshRing();
    const frame = bytes(1, 2, 3, 4, 5);
    expect(ringTryWrite(r, frame)).toBe(true);
    const got = ringTryRead(r);
    expect(got).not.toBeNull();
    expect([...got!]).toEqual([1, 2, 3, 4, 5]);
    expect(ringTryRead(r)).toBeNull();
  });

  test("preserves frame boundaries and order across multiple frames", () => {
    const r = freshRing();
    ringTryWrite(r, bytes(10));
    ringTryWrite(r, bytes(20, 21));
    ringTryWrite(r, bytes(30, 31, 32));
    expect([...ringTryRead(r)!]).toEqual([10]);
    expect([...ringTryRead(r)!]).toEqual([20, 21]);
    expect([...ringTryRead(r)!]).toEqual([30, 31, 32]);
    expect(ringTryRead(r)).toBeNull();
  });

  test("empty ring reads null", () => {
    const r = freshRing();
    expect(ringTryRead(r)).toBeNull();
    expect(ringUsed(r)).toBe(0);
  });

  test("zero-length frame round-trips (length prefix only)", () => {
    const r = freshRing();
    expect(ringTryWrite(r, new Uint8Array(0))).toBe(true);
    const got = ringTryRead(r);
    expect(got).not.toBeNull();
    expect(got!.byteLength).toBe(0);
  });

  test("backpressure: returns false when full, succeeds after a read frees space", () => {
    const r = freshRing();
    const payload = new Uint8Array(12).fill(0xaa);
    expect(ringTryWrite(r, payload)).toBe(true);
    expect(ringTryWrite(r, payload)).toBe(true);
    expect(ringTryWrite(r, payload)).toBe(true);
    expect(ringTryWrite(r, payload)).toBe(true);
    expect(ringTryWrite(r, payload)).toBe(false);
    expect(ringTryRead(r)!.byteLength).toBe(12);
    expect(ringTryWrite(r, payload)).toBe(true);
    expect(ringTryWrite(r, payload)).toBe(false);
  });

  test("wrap-around: a frame straddling the capacity boundary is intact", () => {
    const r = freshRing();
    const filler = new Uint8Array(24).fill(1);
    ringTryWrite(r, filler);
    ringTryRead(r);
    ringTryWrite(r, filler);
    ringTryRead(r);

    const straddle = Uint8Array.from({ length: 20 }, (_, i) => i + 100);
    expect(ringTryWrite(r, straddle)).toBe(true);
    const got = ringTryRead(r);
    expect(got).not.toBeNull();
    expect([...got!]).toEqual([...straddle]);
  });

  test("throws when a frame can never fit", () => {
    const r = freshRing();
    expect(() => ringTryWrite(r, new Uint8Array(CAP))).toThrow(/exceeds/);
  });

  test("counters advance by prefix + payload", () => {
    const r = freshRing();
    ringTryWrite(r, bytes(1, 2, 3));
    expect(Atomics.load(r.ctrl, RING_TAIL)).toBe(7);
    expect(Atomics.load(r.ctrl, RING_HEAD)).toBe(0);
    ringTryRead(r);
    expect(Atomics.load(r.ctrl, RING_HEAD)).toBe(7);
  });

  test("many sequential frames stress wrap repeatedly", () => {
    const r = freshRing();
    for (let i = 0; i < 500; i++) {
      const n = (i % 7) + 1;
      const payload = Uint8Array.from({ length: n }, (_, k) => (i + k) & 0xff);
      expect(ringTryWrite(r, payload)).toBe(true);
      const got = ringTryRead(r);
      expect(got).not.toBeNull();
      expect([...got!]).toEqual([...payload]);
    }
    expect(ringUsed(r)).toBe(0);
  });

  test("power-of-two capacity is enforced", () => {
    const sab = new SharedArrayBuffer(ringBytes(64));
    expect(() => ringViews(sab, 0, 48)).toThrow(/power of two/);
  });
});

// The Phase-1 substrate: rings live in duckdb's shared WebAssembly.Memory at a non-zero
// offset (the C++ malloc pointer), not a dedicated SharedArrayBuffer at offset 0.
describe("ws-ring over shared WebAssembly.Memory at an offset", () => {
  // A non-zero, 4-aligned offset (Int32Array needs 4-alignment), mimicking a malloc ptr.
  const OFFSET = 128;

  function heapRing(capacity = CAP) {
    const mem = new WebAssembly.Memory({
      initial: 1,
      maximum: 4,
      shared: true,
    });
    return { mem, r: ringViews(mem.buffer, OFFSET, capacity) };
  }

  test("write then read round-trips at a non-zero baseOffset", () => {
    const { r } = heapRing();
    expect(ringTryWrite(r, bytes(7, 8, 9))).toBe(true);
    expect([...ringTryRead(r)!]).toEqual([7, 8, 9]);
    expect(ringTryRead(r)).toBeNull();
  });

  test("frames written below the offset region are not visible to the ring", () => {
    const { mem, r } = heapRing();
    // Scribble in the control/data area of a hypothetical ring at offset 0; the ring at
    // OFFSET must be unaffected (proves byteOffset is honored, not ignored).
    new Uint8Array(mem.buffer, 0, OFFSET).fill(0xff);
    expect(ringUsed(r)).toBe(0);
    expect(ringTryRead(r)).toBeNull();
    expect(ringTryWrite(r, bytes(1, 2))).toBe(true);
    expect([...ringTryRead(r)!]).toEqual([1, 2]);
  });

  test("views survive WebAssembly.Memory.grow() mid-stream", () => {
    const { mem, r } = heapRing();
    // A frame is in flight when the heap grows...
    expect(ringTryWrite(r, bytes(1, 2, 3))).toBe(true);
    mem.grow(2); // shared grow: must NOT detach the cached ctrl/data views
    // ...and the pre-grow views still read it and keep working afterward.
    expect([...ringTryRead(r)!]).toEqual([1, 2, 3]);
    expect(ringTryWrite(r, bytes(4, 5))).toBe(true);
    expect([...ringTryRead(r)!]).toEqual([4, 5]);
  });
});
