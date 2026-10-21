// One byte-pump ws-worker per attach: internal (owns a WebSocket) or external (app socket bridged).

import { applyHostDebugFlag, createLogger } from "../logger";
import { wsWorkerEndpoint, type Endpoint } from "../virtual-socket/channel";
import {
  runPump,
  acceptInbound,
  createInboundBuffer,
  type InboundBuffer,
} from "../virtual-socket/pump";
import { invalidatedSchemas, tryHeader } from "../ws-framing";

const log = createLogger("ws-worker");

// Owns the real WebSocket, moves opaque frames to/from the vsock channel; parses headers only to report invalidations.
let pumpEndpoint: Endpoint | null = null;
let pumpStopped = false;
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
let catalogName: string | null = null;

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
  void runPump(
    pumpEndpoint,
    { send: (frame) => sendBytes(frame) },
    pumpInbound,
    () => pumpStopped,
  ).then(() => {
    // runPump has exited, so its last ring access (final drain + the ringClose that
    // woke it) is done and nothing here touches the ring again. Tell the driver the
    // heap-backed region is safe to free — it gates the free on this ack.
    (globalThis as unknown as Worker).postMessage({
      type: "pump-ring-released",
    });
  });
}

// Forward one inbound frame without blocking; on overflow ceiling, tear down socket + channel.
function forwardPumpInbound(frame: Uint8Array) {
  if (!pumpEndpoint) return;
  if (acceptInbound(pumpEndpoint, pumpInbound, frame)) return;
  pumpStopped = true;
  pumpEndpoint.close(); // wakes a C++ WaitUntil parked on the channel
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

// Pump-mode dial: open the socket and wire it to the channel; C++ WsClient drives the protocol.
function openPump(url: string, subprotocol?: string) {
  if (ws) {
    postErr("ws-worker already opened");
    return;
  }
  log.debug(`dialing socket (pump): catalog=${catalogName} url=${url}`);
  ws = subprotocol ? new WebSocket(url, subprotocol) : new WebSocket(url);
  ws.binaryType = "arraybuffer";
  transport = {
    send: (frame) => ws!.send(frame),
    isOpen: () => !!ws && ws.readyState === 1 /* OPEN */,
  };
  ws.addEventListener("open", function () {
    log.debug(`socket open (pump): catalog=${catalogName}`);
    startPump();
    // Transport up; report "ready" so the catalog flips to connected.
    (globalThis as unknown as Worker).postMessage({ type: "ready" });
  });
  ws.addEventListener("message", function (ev: MessageEvent) {
    if (typeof ev.data === "string") return; // text belongs to the app
    const frame = new Uint8Array(ev.data as ArrayBuffer);
    // Only this own-socket path: in external mode the main thread already saw the frame.
    const schemas = invalidatedSchemas(tryHeader(frame));
    // Forward first, so the engine has the PUSH before anything refetches on this notice.
    forwardPumpInbound(frame);
    if (schemas) {
      (globalThis as unknown as Worker).postMessage({
        type: "invalidated",
        schemas,
      });
    }
  });
  ws.addEventListener("close", function (ev: CloseEvent) {
    const reason = ev.reason || `code=${ev.code}`;
    pumpStopped = true;
    pumpEndpoint?.close(); // wakes a C++ WaitUntil parked on the channel
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
        if (typeof msg.catalog === "string") catalogName = msg.catalog;
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
        // all route through it); ringing on close makes the I/O thread notice a drop promptly (it would
        // otherwise wait out its 250ms poll — long enough for a reconnect's ring reset to bump the
        // epoch first, so the stale registration reports "channel reset" instead of "socket closed" and
        // races the reconnected request).
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
        pumpEndpoint?.close(); // wakes a C++ WaitUntil parked on the channel
        (globalThis as unknown as Worker).postMessage({
          type: "closed",
          reason: msg.reason || "app socket closed",
        });
        break;
      }
      case "close": {
        // Retire this worker: (internal) send a WS close so the server drops its handler.
        externalOpen = false;
        try {
          ws?.close(1000, "detach");
        } catch {
          /* ignore */
        }
        break;
      }
      default: {
        postErr(`unknown message type: ${msg.type}`);
      }
    }
  },
);
