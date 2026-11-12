import { wsWorkerEndpoint, type Endpoint } from "./channel";

// The pump side of a channel in duckdb's heap. Every successful inbound publish and every close rings the n6k
// I/O thread's doorbell, so it wakes to drain or to notice the drop instead of waiting out its bounded park.
export function doorbellPumpEndpoint(
  heap: ArrayBufferLike,
  ringOffset: number,
  doorbellOffset: number,
): Endpoint {
  const base = wsWorkerEndpoint(heap, undefined, ringOffset);
  const doorbell = new Int32Array(heap, doorbellOffset, 1);
  const ring = () => {
    Atomics.add(doorbell, 0, 1);
    Atomics.notify(doorbell, 0);
  };
  return {
    ...base,
    send: (frame: Uint8Array) => {
      const ok = base.send(frame);
      if (ok) ring();
      return ok;
    },
    close: () => {
      base.close();
      ring();
    },
  };
}
