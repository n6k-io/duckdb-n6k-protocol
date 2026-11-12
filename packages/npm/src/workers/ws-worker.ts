// One byte-pump ws-worker per adopted transport: bridges an app-registered socket the page owns. Dialed sockets are
// pumped inside the extension (glue/pump-glue.ts).

import { applyHostDebugFlag, createLogger } from "../logger";
import type { Endpoint } from "../virtual-socket/channel";
import { doorbellPumpEndpoint } from "../virtual-socket/doorbell-endpoint";
import {
  runPump,
  acceptInbound,
  createInboundBuffer,
  type InboundBuffer,
} from "../virtual-socket/pump";

const log = createLogger("ws-worker");

let pumpEndpoint: Endpoint | null = null;
let pumpStopped = false;
let pumpDone: Promise<void> = Promise.resolve();
// Overflow buffer for inbound frames that didn't fit the RX ring (see pump.ts).
let pumpInbound: InboundBuffer = createInboundBuffer();

// Outbound relays to the page's socket via postMessage.
type Transport = {
  send: (frame: Uint8Array<ArrayBuffer>) => void;
  isOpen: () => boolean;
};
let transport: Transport | null = null;
let externalOpen = false;

function postErr(message: string) {
  (globalThis as unknown as Worker).postMessage({ type: "error", message });
}

function sendBytes(frame: Uint8Array<ArrayBuffer>) {
  if (!transport || !transport.isOpen()) {
    postErr("cannot send frame: socket not open");
    return;
  }
  transport.send(frame);
}

// Start the byte pump; frames queued before open buffer in the TX ring and flush here.
function startPump() {
  if (!pumpEndpoint) return;
  pumpDone = runPump(
    pumpEndpoint,
    { send: (frame) => sendBytes(frame) },
    pumpInbound,
    () => pumpStopped,
  ).catch((error: unknown) => {
    log.error("pump failed:", error);
  });
}

// Forward one inbound frame without blocking; on overflow ceiling, tear down socket + channel.
function forwardPumpInbound(frame: Uint8Array) {
  if (!pumpEndpoint) return;
  if (acceptInbound(pumpEndpoint, pumpInbound, frame)) return;
  pumpStopped = true;
  pumpEndpoint.close();
  externalOpen = false;
  (globalThis as unknown as Worker).postMessage({
    type: "closed",
    reason: "inbound overflow",
  });
}

(globalThis as unknown as Worker).addEventListener(
  "message",
  function (e: MessageEvent) {
    const msg = e.data;
    if (!msg || typeof msg !== "object") return;
    switch (msg.type) {
      case "init-channel": {
        applyHostDebugFlag(msg.debug);
        pumpEndpoint = doorbellPumpEndpoint(
          (msg.wasmMemory as WebAssembly.Memory).buffer,
          msg.ringOffset as number,
          msg.doorbellOffset as number,
        );
        pumpInbound = createInboundBuffer(); // fresh per channel
        break;
      }
      case "open-pump-external": {
        // Pump over an app-owned socket: send via ws-out, inbound via ws-in.
        externalOpen = true;
        transport = {
          send: (frame) =>
            (globalThis as unknown as Worker).postMessage(
              { type: "ws-out", data: frame },
              [frame.buffer],
            ),
          isOpen: () => externalOpen,
        };
        startPump();
        (globalThis as unknown as Worker).postMessage({ type: "ready" });
        break;
      }
      case "ws-in": {
        // Inbound binary frame from the app's main-thread socket.
        forwardPumpInbound(new Uint8Array(msg.data as ArrayBuffer));
        break;
      }
      case "ws-closed": {
        // App's socket dropped (external mode): mirror the internal "close".
        externalOpen = false;
        pumpStopped = true;
        pumpEndpoint?.close();
        (globalThis as unknown as Worker).postMessage({
          type: "closed",
          reason: msg.reason || "app socket closed",
        });
        break;
      }
      case "close": {
        // Wakes a pump parked on rings C++ already closed, whose own wake was spent before this.
        pumpStopped = true;
        pumpInbound.kick?.();
        // After the final drain, so queued frames still reach the socket. Nothing here touches the ring after
        // this ack; the driver may free it. Without pumpEndpoint.close(), C++ never sees reconnect()'s drop.
        void pumpDone.then(() => {
          pumpEndpoint?.close();
          pumpEndpoint = null;
          externalOpen = false;
          (globalThis as unknown as Worker).postMessage({
            type: "pump-ring-released",
          });
        });
        break;
      }
      default: {
        postErr(`unknown message type: ${msg.type}`);
      }
    }
  },
);
