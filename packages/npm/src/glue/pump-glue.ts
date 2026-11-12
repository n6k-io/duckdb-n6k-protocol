// Runs on the extension's pump pthread: owns every dialed WebSocket and moves frames between it and its ring.
// The thread stays in its JS event loop, so sockets keep flowing while duckdb's main worker is busy.

import { createLogger } from "../logger";
import type { Endpoint } from "../virtual-socket/channel";
import { doorbellPumpEndpoint } from "../virtual-socket/doorbell-endpoint";
import {
  acceptInbound,
  createInboundBuffer,
  runPump,
  type InboundBuffer,
} from "../virtual-socket/pump";
import { PUMP_CLOSED, PUMP_OPENED, PUMP_RELEASED } from "./pump-status";

const log = createLogger("pump");

export type N6kPumpApi = {
  // "OK", or "ERROR: <reason>" when the socket could not be created.
  dial(
    key: string,
    ringPtr: number,
    doorbellPtr: number,
    statusPtr: number,
    wsUrl: string,
  ): string;
  // Only once C++ is off the rings: drain, close the socket, free the ring.
  close(key: string): void;
};

declare global {
  var n6kPump: N6kPumpApi | undefined;
}

type Pump = {
  endpoint: Endpoint;
  inbound: InboundBuffer;
  status: Int32Array;
  ringPtr: number;
  ws: WebSocket;
  stopped: boolean;
  // close() was called: the socket's own close event is not a drop.
  closing: boolean;
  done: Promise<void>;
};

export function installPumpGlue(
  memory: WebAssembly.Memory,
  free: (ptr: number) => void,
): void {
  if (globalThis.n6kPump) return;
  const pumps = new Map<string, Pump>();

  const mark = (p: Pump, bit: number) => {
    Atomics.or(p.status, 0, bit);
    Atomics.notify(p.status, 0);
  };

  const drop = (p: Pump) => {
    if (p.closing || p.stopped) return;
    p.stopped = true;
    p.endpoint.close();
    mark(p, PUMP_CLOSED);
  };

  globalThis.n6kPump = {
    dial(key, ringPtr, doorbellPtr, statusPtr, wsUrl) {
      let ws: WebSocket;
      try {
        ws = new WebSocket(wsUrl);
      } catch (error) {
        return `ERROR: ${(error as Error).message || String(error)}`;
      }
      ws.binaryType = "arraybuffer";
      const heap = memory.buffer;
      const p: Pump = {
        endpoint: doorbellPumpEndpoint(heap, ringPtr, doorbellPtr),
        inbound: createInboundBuffer(),
        status: new Int32Array(heap, statusPtr, 1),
        ringPtr,
        ws,
        stopped: false,
        closing: false,
        done: Promise.resolve(),
      };
      pumps.set(key, p);

      ws.addEventListener("open", () => {
        log.debug(`socket open: ${key} url=${wsUrl}`);
        p.done = runPump(
          p.endpoint,
          { send: (frame) => p.ws.send(frame) },
          p.inbound,
          () => p.stopped,
        ).catch((error: unknown) => {
          log.error(`pump failed: ${key}`, error);
        });
        mark(p, PUMP_OPENED);
      });
      ws.addEventListener("message", (ev: MessageEvent) => {
        if (typeof ev.data === "string" || p.stopped) return;
        if (acceptInbound(p.endpoint, p.inbound, new Uint8Array(ev.data))) {
          return;
        }
        drop(p);
        try {
          p.ws.close(1009, "n6k inbound overflow");
        } catch {
          /* already closing */
        }
      });
      ws.addEventListener("close", () => drop(p));
      ws.addEventListener("error", () => drop(p));
      return "OK";
    },

    close(key) {
      const p = pumps.get(key);
      if (!p) return;
      pumps.delete(key);
      p.closing = true;
      p.stopped = true;
      // Wakes a pump parked on rings C++ already closed.
      p.inbound.kick?.();
      void p.done.then(() => {
        p.endpoint.close();
        try {
          p.ws.close(1000, "detach");
        } catch {
          /* ignore */
        }
        free(p.ringPtr);
        mark(p, PUMP_RELEASED);
      });
    },
  };
}
