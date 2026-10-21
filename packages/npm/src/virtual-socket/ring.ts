// SPSC byte ring over a SharedArrayBuffer; frames are [u32 len][bytes]. Capacity MUST be a power of two.
// head/tail: free-running byte counters masked by (capacity-1); the tail store/load pair is release/acquire.

export const RING_HEAD = 0;
export const RING_TAIL = 1;
export const RING_CLOSED = 2;
// Reset counter; a lazily-polling peer reads it to notice a reset that restored HEAD/TAIL.
export const RING_EPOCH = 3;
export const RING_CTRL_I32 = 4; // HEAD, TAIL, CLOSED, EPOCH; keeps data 8-byte aligned
export const RING_CTRL_BYTES = RING_CTRL_I32 * 4;

const LEN_BYTES = 4;

export type Ring = {
  ctrl: Int32Array;
  data: Uint8Array;
  capacity: number;
};

function isPow2(n: number): boolean {
  return n > 0 && (n & (n - 1)) === 0;
}

export function ringBytes(capacity: number): number {
  return RING_CTRL_BYTES + capacity;
}

export function ringViews(
  sab: ArrayBufferLike,
  byteOffset: number,
  capacity: number,
): Ring {
  if (!isPow2(capacity)) {
    throw new RangeError(`ring capacity ${capacity} must be a power of two`);
  }
  return {
    ctrl: new Int32Array(sab, byteOffset, RING_CTRL_I32),
    data: new Uint8Array(sab, byteOffset + RING_CTRL_BYTES, capacity),
    capacity,
  };
}

function u32(n: number): number {
  return n >>> 0;
}

export function ringUsed(r: Ring): number {
  return u32(Atomics.load(r.ctrl, RING_TAIL) - Atomics.load(r.ctrl, RING_HEAD));
}

function ringFree(r: Ring): number {
  return r.capacity - ringUsed(r);
}

// Copy `src` into the circular data area at counter `pos`, splitting at the wrap.
function writeAt(r: Ring, pos: number, src: Uint8Array): void {
  const mask = r.capacity - 1;
  const start = pos & mask;
  const first = Math.min(src.byteLength, r.capacity - start);
  r.data.set(src.subarray(0, first), start);
  if (first < src.byteLength) {
    r.data.set(src.subarray(first), 0);
  }
}

// Read len bytes from the circular area at counter pos into a fresh exact-fit buffer (safe for a DataView).
function readAt(r: Ring, pos: number, len: number): Uint8Array<ArrayBuffer> {
  const mask = r.capacity - 1;
  const start = pos & mask;
  const out = new Uint8Array(len);
  const first = Math.min(len, r.capacity - start);
  out.set(r.data.subarray(start, start + first), 0);
  if (first < len) {
    out.set(r.data.subarray(0, len - first), first);
  }
  return out;
}

// Write one frame; returns false if no room, throws if the frame can never fit.
export function ringTryWrite(r: Ring, frame: Uint8Array): boolean {
  const need = LEN_BYTES + frame.byteLength;
  if (need > r.capacity) {
    throw new RangeError(
      `frame ${frame.byteLength}B + prefix exceeds ring capacity ${r.capacity}`,
    );
  }
  if (ringFree(r) < need) return false;

  const tail = u32(Atomics.load(r.ctrl, RING_TAIL));
  const lenBuf = new Uint8Array(LEN_BYTES);
  new DataView(lenBuf.buffer).setUint32(0, frame.byteLength, true);
  writeAt(r, tail, lenBuf);
  writeAt(r, tail + LEN_BYTES, frame);
  // Publish last, then wake a blocked reader.
  Atomics.store(r.ctrl, RING_TAIL, u32(tail + need));
  Atomics.notify(r.ctrl, RING_TAIL);
  return true;
}

// Length of the next queued frame without consuming it, or -1 if none.
export function ringPeekLen(r: Ring): number {
  if (ringUsed(r) < LEN_BYTES) return -1;
  const head = u32(Atomics.load(r.ctrl, RING_HEAD));
  const lenBuf = readAt(r, head, LEN_BYTES);
  return new DataView(lenBuf.buffer).getUint32(0, true);
}

// Wait up to timeoutMs for the next frame (worker threads only); does not read.
export function ringWaitTimed(r: Ring, timeoutMs: number): void {
  if (ringUsed(r) >= LEN_BYTES) return;
  const tail = Atomics.load(r.ctrl, RING_TAIL);
  Atomics.wait(r.ctrl, RING_TAIL, tail, timeoutMs);
}

// Read one frame, or null if none is fully available. Non-blocking.
export function ringTryRead(r: Ring): Uint8Array<ArrayBuffer> | null {
  if (ringUsed(r) < LEN_BYTES) return null;
  const head = u32(Atomics.load(r.ctrl, RING_HEAD));
  const lenBuf = readAt(r, head, LEN_BYTES);
  const len = new DataView(lenBuf.buffer).getUint32(0, true);
  if (ringUsed(r) < LEN_BYTES + len) return null; // partial (rare under SPSC)
  const frame = readAt(r, head + LEN_BYTES, len);
  // Release space, then wake a blocked writer.
  Atomics.store(r.ctrl, RING_HEAD, u32(head + LEN_BYTES + len));
  Atomics.notify(r.ctrl, RING_HEAD);
  return frame;
}

// Blocking write (worker threads only). Waits for the consumer to free space.
// Closed-guard: once CLOSED no consumer will free space, so bail instead of spinning forever
// (mirrors VsockRing::BlockingWrite in n6k_vsock_ring.hpp).
export function ringBlockingWrite(r: Ring, frame: Uint8Array): void {
  for (;;) {
    if (ringTryWrite(r, frame)) return;
    if (ringIsClosed(r)) return;
    const head = Atomics.load(r.ctrl, RING_HEAD);
    if (ringTryWrite(r, frame)) return;
    Atomics.wait(r.ctrl, RING_HEAD, head);
  }
}

export function ringIsClosed(r: Ring): boolean {
  return Atomics.load(r.ctrl, RING_CLOSED) === 1;
}

// Reset to empty+open for reuse; only safe when both sides idle. Bumps epoch so a polling peer sees reuse.
export function ringReset(r: Ring): void {
  Atomics.store(r.ctrl, RING_HEAD, 0);
  Atomics.store(r.ctrl, RING_TAIL, 0);
  Atomics.store(r.ctrl, RING_CLOSED, 0);
  Atomics.add(r.ctrl, RING_EPOCH, 1);
}

export function ringEpoch(r: Ring): number {
  return Atomics.load(r.ctrl, RING_EPOCH);
}

// Mark the ring closed and wake any blocked reader/writer so it can exit.
export function ringClose(r: Ring): void {
  Atomics.store(r.ctrl, RING_CLOSED, 1);
  Atomics.notify(r.ctrl, RING_TAIL);
  Atomics.notify(r.ctrl, RING_HEAD);
}

// Async wait for the next frame (event-loop consumer); ringTryRead first, then await, loop.
export function ringWaitAsync(r: Ring) {
  const tail = Atomics.load(r.ctrl, RING_TAIL);
  return Atomics.waitAsync(r.ctrl, RING_TAIL, tail);
}

// Async wait for freed space (event-loop producer); ringTryWrite first, then await, loop.
export function ringWaitSpaceAsync(r: Ring) {
  const head = Atomics.load(r.ctrl, RING_HEAD);
  return Atomics.waitAsync(r.ctrl, RING_HEAD, head);
}
