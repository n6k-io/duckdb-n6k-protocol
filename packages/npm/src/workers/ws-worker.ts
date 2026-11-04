// One byte-pump ws-worker per connection transport: internal (owns a WebSocket) or external (app socket bridged).

import { applyHostDebugFlag, createLogger } from "../logger";
import { wsWorkerEndpoint, type Endpoint } from "../virtual-socket/channel";
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

let ws: WebSocket | null = null;
// Outbound seam: internal sends on the WebSocket, external relays via postMessage.
type Transport = {
  send: (frame: Uint8Array<ArrayBuffer>) => void;
  isOpen: () => boolean;
};
let transport: Transport | null = null;
let externalOpen = false;
let label: string | null = null;

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
  try {
    ws?.close(1009, "n6k inbound overflow"); // 1009 = Message Too Big
  } catch {
    /* socket already closing */
  }
  (globalThis as unknown as Worker).postMessage({
    type: "closed",
    reason: "inbound overflow",
  });
  ws = null;
}

// Pump-mode dial: open the socket and wire it to the channel; C++ drives the protocol.
function openPump(url: string, subprotocol?: string) {
  if (ws) {
    postErr("ws-worker already opened");
    return;
  }
  log.debug(`dialing socket (pump): ${label} url=${url}`);
  ws = subprotocol ? new WebSocket(url, subprotocol) : new WebSocket(url);
  ws.binaryType = "arraybuffer";
  transport = {
    send: (frame) => ws!.send(frame),
    isOpen: () => !!ws && ws.readyState === 1 /* OPEN */,
  };
  ws.addEventListener("open", function () {
    log.debug(`socket open (pump): ${label}`);
    startPump();
    // Transport up; report "ready" so its catalogs flip to connected.
    (globalThis as unknown as Worker).postMessage({ type: "ready" });
  });
  ws.addEventListener("message", function (ev: MessageEvent) {
    if (typeof ev.data === "string") return; // an internal socket has no JSON-RPC consumer
    forwardPumpInbound(new Uint8Array(ev.data as ArrayBuffer));
  });
  ws.addEventListener("close", function (ev: CloseEvent) {
    const reason = ev.reason || `code=${ev.code}`;
    pumpStopped = true;
    pumpEndpoint?.close();
    (globalThis as unknown as Worker).postMessage({ type: "closed", reason });
    ws = null;
  });
  ws.addEventListener("error", function () {
    pumpStopped = true;
    pumpEndpoint?.close();
    (globalThis as unknown as Worker).postMessage({
      type: "closed",
      reason: "ws error",
    });
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
        if (typeof msg.label === "string") label = msg.label;
        // Rings live in duckdb's shared WebAssembly.Memory at the C++ malloc offset.
        const memory = (msg.wasmMemory as WebAssembly.Memory).buffer;
        const base = wsWorkerEndpoint(
          memory,
          undefined,
          msg.ringOffset as number,
        );
        // Wake the shared n6k I/O thread by ringing its doorbell (the loop parks on it)
        // whenever this channel's RX ring changes state — after publishing an inbound frame, and on
        // close. send is the single publish choke point (acceptInbound/flushInbound/forwardPumpInbound
        // all route through it); ringing on close makes the I/O thread notice a drop without waiting out
        // its bounded park.
        const doorbell = new Int32Array(
          memory,
          msg.doorbellOffset as number,
          1,
        );
        const ringDoorbell = () => {
          Atomics.add(doorbell, 0, 1);
          Atomics.notify(doorbell, 0);
        };
        pumpEndpoint = {
          ...base,
          send: (frame: Uint8Array) => {
            const ok = base.send(frame);
            if (ok) ringDoorbell();
            return ok;
          },
          // The C++ I/O thread reports this as a drop once the ring is drained.
          close: () => {
            base.close();
            ringDoorbell();
          },
        };
        pumpInbound = createInboundBuffer(); // fresh per channel
        break;
      }
      case "open-pump": {
        openPump(msg.url, msg.subprotocol);
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
          try {
            ws?.close(1000, "detach");
          } catch {
            /* ignore */
          }
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
