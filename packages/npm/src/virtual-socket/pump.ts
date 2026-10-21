// The ws-worker byte pump: moves raw frames both ways between the WebSocket and the vsock channel; no parsing.
// CRITICAL: never block the loop — a full RX ring deadlocks the duckdb peer; inbound overflows to a buffer, not Atomics.wait.

import type { Endpoint } from "./channel";

export type PumpSocket = {
  send(frame: Uint8Array<ArrayBuffer>): void;
};

// Overflow ceiling; hitting it is a hard fault (caller closes the socket), not unbounded growth.
export const DEFAULT_MAX_OVERFLOW_BYTES = 64 << 20; // 64 MiB

// Inbound frames that didn't fit the RX ring, flushed FIFO; kick wakes a pump parked without a space-wait.
export type InboundBuffer = {
  queue: Uint8Array[];
  bytes: number;
  kick: (() => void) | null;
};

export function createInboundBuffer(): InboundBuffer {
  return { queue: [], bytes: 0, kick: null };
}

// Push queued overflow frames into the RX ring oldest-first until one doesn't fit; returns count flushed.
export function flushInbound(endpoint: Endpoint, buf: InboundBuffer): number {
  let n = 0;
  for (let f = buf.queue[0]; f !== undefined; f = buf.queue[0]) {
    if (!endpoint.send(f)) break; // ring full again
    buf.queue.shift();
    buf.bytes -= f.byteLength;
    n++;
  }
  return n;
}

// Forward one inbound frame without blocking; overflow to buffer when full. False = past ceiling, caller closes socket.
export function acceptInbound(
  endpoint: Endpoint,
  buf: InboundBuffer,
  frame: Uint8Array,
  maxOverflowBytes: number = DEFAULT_MAX_OVERFLOW_BYTES,
): boolean {
  // Clear any backlog first so the fast path stays available.
  flushInbound(endpoint, buf);
  if (buf.queue.length === 0 && endpoint.send(frame)) return true;
  if (buf.bytes + frame.byteLength > maxOverflowBytes) return false;
  buf.queue.push(frame);
  buf.bytes += frame.byteLength;
  buf.kick?.(); // wake a loop parked without a space-wait
  return true;
}

// Forward every queued outbound frame to the socket. Returns the count forwarded.
export function drainOutbound(endpoint: Endpoint, socket: PumpSocket): number {
  let n = 0;
  for (;;) {
    const frame = endpoint.tryRecv();
    if (frame === null) break;
    socket.send(frame);
    n++;
  }
  return n;
}

// Drive both directions non-blocking: drain+flush, then park until outbound frame, freed RX space, or kick.
export async function runPump(
  endpoint: Endpoint,
  socket: PumpSocket,
  buf: InboundBuffer,
  stopped: () => boolean,
): Promise<void> {
  while (!stopped()) {
    drainOutbound(endpoint, socket);
    flushInbound(endpoint, buf);
    if (stopped()) break;

    const recv = endpoint.recvWaitAsync();
    if (!recv.async) continue; // outbound frame already landed
    const space = buf.queue.length > 0 ? endpoint.sendWaitAsync() : null;
    if (space && !space.async) continue; // RX space already freed

    // Setting `kick` happens-before the await, so a later acceptInbound can't be lost.
    const kicked = new Promise<void>((resolve) => {
      buf.kick = resolve;
    });
    const waits: Promise<unknown>[] = [recv.value, kicked];
    if (space) waits.push(space.value);
    await Promise.race(waits);
    buf.kick = null;
  }
  // Final drain/flush so frames queued during the last wait aren't stranded.
  drainOutbound(endpoint, socket);
  flushInbound(endpoint, buf);
}
