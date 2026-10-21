import { test, expect, describe } from "bun:test";
import { packFrame, unpackFrame, FT } from "../../ws-framing";
import {
  bindPump,
  connectionReplyId,
  deliverInbound,
  frameNs,
  type DemuxState,
  type PumpTarget,
} from "../ws-demux";

class FakePump implements PumpTarget {
  received: number[] = [];
  postMessage(message: { type: "ws-in"; data: ArrayBuffer }): void {
    this.received.push(unpackFrame(message.data).header.id as number);
  }
}

function frame(ns: number | undefined, id: number): ArrayBuffer {
  const header: Record<string, unknown> = { t: FT.RESP_END, id };
  if (ns !== undefined) header.ns = ns;
  return packFrame(header).buffer as ArrayBuffer;
}

function newState(): DemuxState<FakePump> {
  return { inbound: [], targets: new Map() };
}

describe("ws-demux", () => {
  test("frameNs reads the session ns, or undefined when absent/garbage", () => {
    expect(frameNs(frame(7, 1))).toBe(7);
    expect(frameNs(frame(undefined, 1))).toBeUndefined();
    expect(frameNs(new Uint8Array([0xff, 0xff, 0xff]).buffer)).toBeUndefined();
  });

  test("routes ns-tagged frames to the matching pump", () => {
    const state = newState();
    const foo = new FakePump();
    const bar = new FakePump();
    bindPump(state, 1, foo);
    bindPump(state, 2, bar);

    deliverInbound(state, frame(1, 10));
    deliverInbound(state, frame(2, 20));
    deliverInbound(state, frame(1, 11));

    expect(foo.received).toEqual([10, 11]);
    expect(bar.received).toEqual([20]);
  });

  test("untagged frame goes to the sole pump (single-session / pre-mux)", () => {
    const state = newState();
    const only = new FakePump();
    bindPump(state, 0, only);
    deliverInbound(state, frame(undefined, 5));
    expect(only.received).toEqual([5]);
  });

  test("untagged frame with multiple pumps is not misrouted (buffered)", () => {
    const state = newState();
    bindPump(state, 1, new FakePump());
    bindPump(state, 2, new FakePump());
    deliverInbound(state, frame(undefined, 9));
    expect(state.inbound.length).toBe(1);
  });

  test("frames buffered before a pump binds are flushed on bind", () => {
    const state = newState();
    deliverInbound(state, frame(5, 1));
    deliverInbound(state, frame(5, 2));
    expect(state.inbound.length).toBe(2);

    const late = new FakePump();
    bindPump(state, 5, late);
    expect(late.received).toEqual([1, 2]);
    expect(state.inbound.length).toBe(0);
  });

  test("detaching one session leaves its sibling routing intact", () => {
    const state = newState();
    const foo = new FakePump();
    const bar = new FakePump();
    bindPump(state, 1, foo);
    bindPump(state, 2, bar);

    state.targets.delete(1);

    deliverInbound(state, frame(2, 30));
    expect(bar.received).toEqual([30]);

    deliverInbound(state, frame(1, 31));
    expect(bar.received).toEqual([30]);
    expect(state.inbound.length).toBe(1);
  });

  test("connectionReplyId matches only untagged RESP_CHUNK/RESP_END/ERR", () => {
    expect(connectionReplyId({ t: FT.RESP_CHUNK, id: 3 })).toBe(3);
    expect(connectionReplyId({ t: FT.RESP_END, id: 3 })).toBe(3);
    expect(connectionReplyId({ t: FT.ERR, id: 3 })).toBe(3);
    expect(connectionReplyId({ t: FT.RESP_END, id: 3, ns: 1 })).toBeUndefined();
    expect(connectionReplyId({ t: FT.PONG, id: 3 })).toBeUndefined();
    expect(connectionReplyId({ t: FT.RESP_END })).toBeUndefined();
  });
});
