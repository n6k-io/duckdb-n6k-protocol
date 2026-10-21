// Inbound demux for a socket shared by many sessions; routes frames by ns (untagged → the sole pump).

import { unpackFrame, FT, type FrameHeader } from "../ws-framing";

// A pump the demux hands a frame to (Worker or test double); the ArrayBuffer is transferred, not copied.
export interface PumpTarget {
  postMessage(
    message: { type: "ws-in"; data: ArrayBuffer },
    transfer: Transferable[],
  ): void;
}

// Per-socket routing: pumps keyed by session ns, plus a buffer for frames that arrived before a pump bound.
export interface DemuxState<T extends PumpTarget = PumpTarget> {
  inbound: ArrayBuffer[];
  targets: Map<number, T>;
}

// The frame's session ns, or undefined if the header omits it or won't decode (falls back to the sole pump).
export function frameNs(data: ArrayBuffer): number | undefined {
  try {
    const { header } = unpackFrame(data);
    return typeof header.ns === "number" ? header.ns : undefined;
  } catch {
    return undefined;
  }
}

// Id of a connection-scoped reply (untagged RESP_CHUNK/RESP_END/ERR); PONG also carries an untagged id.
export function connectionReplyId(header: FrameHeader): number | undefined {
  if (typeof header.ns === "number" || typeof header.id !== "number") {
    return undefined;
  }
  return header.t === FT.RESP_CHUNK ||
    header.t === FT.RESP_END ||
    header.t === FT.ERR
    ? header.id
    : undefined;
}

// Pick the pump: ns-tagged → only that ns's pump (no fallback); untagged → the sole pump if exactly one.
export function pickTarget<T extends PumpTarget>(
  state: DemuxState<T>,
  ns: number | undefined,
): T | undefined {
  if (ns !== undefined) return state.targets.get(ns);
  if (state.targets.size === 1) return state.targets.values().next().value;
  return undefined;
}

// Route one inbound frame to its pump, or buffer it until a matching pump binds.
export function deliverInbound<T extends PumpTarget>(
  state: DemuxState<T>,
  data: ArrayBuffer,
): void {
  const target = pickTarget(state, frameNs(data));
  if (target) target.postMessage({ type: "ws-in", data }, [data]);
  else state.inbound.push(data);
}

// Retry every buffered frame after the pump set changed; still-unroutable frames re-buffer.
export function flushInbound<T extends PumpTarget>(state: DemuxState<T>): void {
  if (state.inbound.length === 0) return;
  const pending = state.inbound;
  state.inbound = [];
  for (const data of pending) deliverInbound(state, data);
}

// Bind a pump under its session ns, then drain buffered frames that now route to it.
export function bindPump<T extends PumpTarget>(
  state: DemuxState<T>,
  ns: number,
  worker: T,
): void {
  state.targets.set(ns, worker);
  flushInbound(state);
}
