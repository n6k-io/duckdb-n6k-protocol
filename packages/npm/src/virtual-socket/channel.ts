// Duplex channel: one SAB, two SPSC rings (one per direction); each side gets a mirror-image endpoint.

import {
  type Ring,
  ringViews,
  ringBytes,
  ringTryWrite,
  ringTryRead,
  ringPeekLen,
  ringBlockingWrite,
  ringWaitAsync,
  ringWaitSpaceAsync,
  ringWaitTimed,
  ringIsClosed,
  ringReset,
  ringClose,
  ringEpoch,
} from "./ring";

// Per-direction data capacities (bytes, power of two).
export type ChannelLayout = {
  duckdbToWsCapacity: number;
  wsToDuckdbCapacity: number;
};

export const DEFAULT_CHANNEL_LAYOUT: ChannelLayout = {
  duckdbToWsCapacity: 1 << 20,
  wsToDuckdbCapacity: 1 << 20,
};

export function channelBytes(layout: ChannelLayout): number {
  return (
    ringBytes(layout.duckdbToWsCapacity) + ringBytes(layout.wsToDuckdbCapacity)
  );
}

export type Endpoint = {
  send(frame: Uint8Array): boolean;
  sendBlocking(frame: Uint8Array): void;
  tryRecv(): Uint8Array<ArrayBuffer> | null;
  peekLen(): number;
  // Async wait for the next inbound frame; drain with tryRecv first, then await, loop.
  recvWaitAsync(): ReturnType<typeof ringWaitAsync>;
  // Async wait for outbound space to free; send first, then await.
  sendWaitAsync(): ReturnType<typeof ringWaitSpaceAsync>;
  // Wait up to `timeoutMs` for the next inbound frame (worker threads only).
  recvWaitTimed(timeoutMs: number): void;
  inboundClosed(): boolean;
  // Reset counter; a lazily-polling consumer reads it to notice a swap.
  epoch(): number;
  close(): void;
};

function makeEndpoint(outbound: Ring, inbound: Ring): Endpoint {
  return {
    send: (frame) => ringTryWrite(outbound, frame),
    sendBlocking: (frame) => ringBlockingWrite(outbound, frame),
    tryRecv: () => ringTryRead(inbound),
    peekLen: () => ringPeekLen(inbound),
    recvWaitAsync: () => ringWaitAsync(inbound),
    sendWaitAsync: () => ringWaitSpaceAsync(outbound),
    recvWaitTimed: (timeoutMs) => ringWaitTimed(inbound, timeoutMs),
    inboundClosed: () => ringIsClosed(inbound),
    epoch: () => ringEpoch(outbound),
    close: () => {
      ringClose(outbound);
      ringClose(inbound);
    },
  };
}

// `buf` is the ring-region backing store: duckdb's shared wasm heap (the SharedArrayBuffer
// of a threaded WebAssembly.Memory), or a dedicated SharedArrayBuffer in unit tests.
// `baseOffset` is where this channel begins in `buf` — the C++ malloc pointer in the heap.
function rings(
  buf: ArrayBufferLike,
  layout: ChannelLayout,
  baseOffset: number,
): {
  duckdbToWs: Ring;
  wsToDuckdb: Ring;
} {
  // Growth safety: the region must lie fully within the (possibly grown) heap. A
  // shared WebAssembly.Memory.grow() never detaches cached views, so passing here at
  // build time holds for the region's whole life — it's malloc'd once, before any grow.
  const end = baseOffset + channelBytes(layout);
  if (end > buf.byteLength) {
    throw new RangeError(
      `ring region [${baseOffset}, ${end}) exceeds buffer length ${buf.byteLength}`,
    );
  }
  const duckdbToWs = ringViews(buf, baseOffset, layout.duckdbToWsCapacity);
  const wsToDuckdb = ringViews(
    buf,
    baseOffset + ringBytes(layout.duckdbToWsCapacity),
    layout.wsToDuckdbCapacity,
  );
  return { duckdbToWs, wsToDuckdb };
}

export function duckdbEndpoint(
  buf: ArrayBufferLike,
  layout: ChannelLayout = DEFAULT_CHANNEL_LAYOUT,
  baseOffset = 0,
): Endpoint {
  const { duckdbToWs, wsToDuckdb } = rings(buf, layout, baseOffset);
  return makeEndpoint(duckdbToWs, wsToDuckdb);
}

// The ws-worker's endpoint (mirror image).
export function wsWorkerEndpoint(
  buf: ArrayBufferLike,
  layout: ChannelLayout = DEFAULT_CHANNEL_LAYOUT,
  baseOffset = 0,
): Endpoint {
  const { duckdbToWs, wsToDuckdb } = rings(buf, layout, baseOffset);
  return makeEndpoint(wsToDuckdb, duckdbToWs);
}

// Reset both rings for reuse across a reconnect; only safe when both sides are idle.
export function resetChannel(
  buf: ArrayBufferLike,
  layout: ChannelLayout = DEFAULT_CHANNEL_LAYOUT,
  baseOffset = 0,
): void {
  const { duckdbToWs, wsToDuckdb } = rings(buf, layout, baseOffset);
  ringReset(duckdbToWs);
  ringReset(wsToDuckdb);
}
