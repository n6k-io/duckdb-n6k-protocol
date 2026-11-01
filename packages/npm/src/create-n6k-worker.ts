/* eslint-disable unicorn/prefer-global-this */
import {
  packFrame,
  FT,
  OP,
  HELLO_CAPS,
  N6K_PROTOCOL_VERSION,
  invalidatedSchemas,
  tryHeader,
  type FrameHeader,
} from "./ws-framing";
import { resetChannel } from "./virtual-socket/channel";
import {
  bindPump,
  connectionReplyId,
  deliverInbound,
} from "./virtual-socket/ws-demux";
import { createCatalogRegistry } from "./workers/ws-registry";
import { logger as log, isDebugEnabled } from "./logger";

let liveWorkers = 0;
const trackedWorkers = new WeakSet<Worker>();
function trackWorker(w: Worker, label: string): void {
  trackedWorkers.add(w);
  liveWorkers++;
  log.debug(`worker+ ${label} (live workers: ${liveWorkers})`);
}
function untrackWorker(w: Worker, label: string): void {
  if (!trackedWorkers.delete(w)) return;
  liveWorkers--;
  log.debug(`worker- ${label} (live workers: ${liveWorkers})`);
}
import { CAP_JSONRPC } from "./protocol-generated";
import {
  createJsonRpcClient,
  type JsonRpcClient,
  type JsonRpcEndpoint,
} from "./jsonrpc";
import type { WsStatus } from "./types";

export type CreateN6kWorkerOpts = {
  mainWorkerUrl: string;
  duckdbWorkerUrl: string | URL;
  wsWorkerUrl: string | URL;
  onStatus?: (catalog: string, status: WsStatus) => void;
  // The server added or removed catalogs on a registered wsId; `catalogs` is the full new list.
  onCatalogsChanged?: (wsId: string, catalogs: string[]) => void;
  // The server invalidated `schemas` of an attached catalog (e.g. it swapped the catalog); cached reads are stale.
  onInvalidated?: (catalog: string, schemas: string[]) => void;
  instanceId?: number;
};

export class HelloRefusedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "HelloRefusedError";
  }
}

export type RegisteredWebsocket = {
  wsId: string;
  // From the latest HELLO_ACK; replaceWebsocket updates it.
  capabilities: string[];
  // Throws if the server did not grant `jsonrpc`.
  readonly rpc: JsonRpcClient;
  // Fails pending calls; the socket stays registered until its last catalog is detached. Idempotent.
  // Trap: a held `rpc` still sends after close() but its replies are dropped, so calls end only by timeout
  // or abort.
  close: () => void;
};

export type N6kWorkerHandle = {
  worker: Worker;
  // Reopen an attached catalog's socket without DETACH/ATTACH; no-op for unknown; status via onStatus.
  reconnect: (catalog: string) => void;
  // Resolves on HELLO_ACK; rejects on HELLO_ERR (HelloRefusedError), a protocol_version mismatch, the HELLO
  // send throwing, or the socket closing or erroring first. No deadline. Binary frames are n6k, text frames are JSON-RPC.
  registerWebsocket: (
    socket: WebSocket,
    token: string,
  ) => Promise<RegisteredWebsocket>;
  // Swap the socket behind a wsId, FT_HELLO it, then re-open every attached catalog. On success, in-flight
  // reqs and rpc calls fail and rpc moves to the new socket. On handshake failure it rethrows and keeps the
  // previous socket if still OPEN; otherwise the previous socket's close handling runs.
  replaceWebsocket: (
    wsId: string,
    socket: WebSocket,
    token: string,
  ) => Promise<void>;
  // Catalog names the server serves on a registered wsId (OP_CATALOG_LIST; no ATTACH needed).
  listCatalogs: (wsId: string) => Promise<string[]>;
  // Terminate the ws-workers and close the control channel. Does NOT touch the
  // duckdb `worker` (owned by AsyncDuckDB.terminate()); without it those handles pin the host loop.
  dispose: () => void;
};

export function createN6kWorker({
  mainWorkerUrl,
  duckdbWorkerUrl,
  wsWorkerUrl,
  onStatus,
  onCatalogsChanged,
  onInvalidated,
  instanceId = 0,
}: CreateN6kWorkerOpts): N6kWorkerHandle {
  if (typeof SharedArrayBuffer === "undefined") {
    throw new TypeError(
      "n6k requires cross-origin isolation (SharedArrayBuffer is not available). " +
        "crossOriginIsolated = " +
        (typeof self === "undefined" ? "unknown" : self.crossOriginIsolated),
    );
  }

  const notifyStatus = onStatus || function () {};

  // Snapshot page debug state and forward into every spawned worker (workers can't read localStorage).
  const debug = isDebugEnabled();

  // Ring-region carrier forwarded to the pump ws-worker: duckdb's shared WebAssembly.Memory +
  // the C++ malloc offset. The Memory object is structured-cloneable and shares by reference
  // across postMessage.
  type ChannelCarrier = {
    wasmMemory: WebAssembly.Memory;
    ringOffset: number;
    doorbellOffset: number;
  };
  const readCarrier = (m: ChannelCarrier): ChannelCarrier => ({
    wasmMemory: m.wasmMemory,
    ringOffset: m.ringOffset,
    doorbellOffset: m.doorbellOffset,
  });
  // Reset both rings in place for a reconnect (buffer/offset unchanged; C++ owns the malloc).
  const resetCarrier = (c: ChannelCarrier): void =>
    resetChannel(c.wasmMemory.buffer, undefined, c.ringOffset);

  // Byte-pump (vsock) reconnect params per catalog so reconnect() can respawn the pump (external stores wsId).
  type PumpParams =
    | ({ external: false; wsUrl: string } & ChannelCarrier)
    | ({ external: true; wsId: string } & ChannelCarrier);
  const pumpParams = new Map<string, PumpParams>();
  const carrierOf = (pp: PumpParams): ChannelCarrier => ({
    wasmMemory: pp.wasmMemory,
    ringOffset: pp.ringOffset,
    doorbellOffset: pp.doorbellOffset,
  });

  // Pump ws-worker → wasm-heap ring offset pending a free. Set on detach; the free fires only
  // when that pump acks it has released the ring ("pump-ring-released"), so we never free a
  // region still in use. Keyed by pump, not catalog: a re-ATTACH of the same name gets a new pump.
  const pendingRingFree = new Map<Worker, number>();

  const LIST_TIMEOUT_MS = 10_000;
  type ConnectionRequest = {
    catalogs: string[];
    finish: (error?: Error) => void;
  };

  // App-registered sockets by wsId; inbound listener attached at registration so the HELLO_ACK isn't missed.
  type ExternalSocket = {
    socket: WebSocket;
    pendingHello?: (header: FrameHeader) => void;
    // HELLO_ACK `capabilities`: negotiated here, read by the wasm WsClient via n6k.helloCapabilities.
    capabilities: string[];
    // Frames buffered before a pump was bound to route them; replayed on bind.
    inbound: ArrayBuffer[];
    // Pumps on this one socket keyed by session ns (multiplex: one socket, N sessions).
    targets: Map<number, Worker>;
    // Connection-scoped requests (no ns) issued from here, keyed by id; never reach a pump.
    requests: Map<number, ConnectionRequest>;
    nextReqId: number;
    onMessage: (ev: MessageEvent) => void;
    onClose: () => void;
  };
  const externalSockets = new Map<string, ExternalSocket>();
  // Per wsId until close(); outlives replaceWebsocket and keeps the socket registered past the last DETACH.
  const rpcClients = new Map<string, JsonRpcEndpoint>();
  const registrations = new Map<string, RegisteredWebsocket>();
  let nextWsId = 1;
  // catalog -> wsId, for detaching/reconnecting external catalogs.
  const externalBindings = new Map<string, string>();
  // catalog -> its session ns on the shared socket.
  const catalogNs = new Map<string, number>();
  // wsId -> catalogs attached on it; refcount so DETACH tears down only on the last catalog.
  const wsIdToCatalogs = new Map<string, Set<string>>();

  // These helper workers do their work over SAB/Atomics, not the event loop. Under Bun a
  // classic worker that parks on worker_threads' parentPort never releases the parent loop on
  // terminate(), so unref them at spawn; the duckdb worker stays ref'd and db.terminate() frees it.
  const unref = (w: Worker) =>
    (w as unknown as { unref?: () => void }).unref?.();

  // Private channel for n6k control frames; keeps them off duckdb-wasm's RPC postMessage binding.
  const n6kChannel = new MessageChannel();

  const worker = new Worker(duckdbWorkerUrl, { type: "classic" });
  trackWorker(worker, `#${instanceId} duckdb-worker`);
  worker.addEventListener("error", function (e) {
    log.error("duckdb-worker error:", e.message || e);
    log.error(
      "duckdb-worker error detail — filename:",
      e.filename,
      "lineno:",
      e.lineno,
    );
  });
  worker.postMessage(
    {
      type: "n6k-init",
      mainWorkerUrl,
      n6kPort: n6kChannel.port2,
      debug,
      instanceId,
    },
    [n6kChannel.port2],
  );

  // ws-workers torn down on purpose; their "closed" message must not report as "disconnected".
  const intentionalClose = new WeakSet<Worker>();

  // One ws-worker per catalog; dispose = graceful WS close then terminate backstop (avoids half-open server conns).
  const wsWorkers = createCatalogRegistry<Worker>(undefined, function (w, c) {
    intentionalClose.add(w);
    untrackWorker(w, `#${instanceId} ws-worker catalog=${c}`);
    try {
      w.postMessage({ type: "close" });
    } catch {
      /* ignore */
    }
    setTimeout(function () {
      try {
        w.terminate();
      } catch {
        /* ignore */
      }
    }, 100);
  });

  // Byte-pump ws-worker: moves frames WS↔channel; no atomics handshake (protocol is in C++ WsClient).
  function spawnPumpWsWorker(catalog: string, carrier: ChannelCarrier): Worker {
    const prev = wsWorkers.get(catalog);
    if (prev) {
      intentionalClose.add(prev);
      untrackWorker(prev, `#${instanceId} ws-worker catalog=${catalog}`);
      prev.terminate();
    }
    const w = new Worker(wsWorkerUrl, { type: "classic" });
    trackWorker(w, `#${instanceId} ws-worker catalog=${catalog}`);
    unref(w);
    w.addEventListener("error", function (ev) {
      log.error("ws-worker (pump) error:", ev.message || ev);
    });
    w.postMessage({ type: "init-channel", catalog, debug, ...carrier });
    wsWorkers.set(catalog, w);

    let connected = false;
    let disconnectNotified = false;
    w.addEventListener("message", function (ev: MessageEvent) {
      if (!ev.data) return;
      if (ev.data.type === "pump-ring-released") {
        // The pump finished with its ring; free the heap region iff a detach is pending
        // for this catalog (a plain socket drop leaves it for the reconnect to reuse).
        const off = pendingRingFree.get(w);
        if (off !== undefined) {
          pendingRingFree.delete(w);
          try {
            worker.postMessage({
              type: "n6k-vsock-free-ring",
              catalog,
              ringOffset: off,
            });
          } catch {
            // duckdb worker already torn down; region goes with the whole heap.
          }
        }
        return;
      }
      if (ev.data.type === "invalidated") {
        onInvalidated?.(catalog, ev.data.schemas as string[]);
        return;
      }
      if (ev.data.type === "ready") {
        connected = true;
        notifyStatus(catalog, "connected");
      } else if (
        (ev.data.type === "closed" || ev.data.type === "error") &&
        connected &&
        !disconnectNotified &&
        !intentionalClose.has(w)
      ) {
        disconnectNotified = true;
        log.error(
          `ws-worker (pump) disconnected: catalog=${catalog} reason=${ev.data.reason || "(none)"}`,
        );
        notifyStatus(catalog, "disconnected");
      }
    } as EventListener);
    return w;
  }

  // Attach the durable inbound bridge to an app socket under wsId, replacing any prior binding.
  function createExternalSocket(
    socket: WebSocket,
    wsId: string,
  ): ExternalSocket {
    try {
      socket.binaryType = "arraybuffer";
    } catch {
      // Some environments lock binaryType; fine if already "arraybuffer".
    }
    const state: ExternalSocket = {
      socket,
      capabilities: [],
      inbound: [],
      targets: new Map(),
      requests: new Map(),
      nextReqId: 1,
      onMessage: () => {},
      onClose: () => {},
    };
    state.onMessage = function (ev: MessageEvent) {
      const d = ev.data;
      if (typeof d === "string") {
        rpcClients.get(wsId)?.receive(d);
        return;
      }
      if (!(d instanceof ArrayBuffer)) return;
      const header = tryHeader(d);
      if (header?.t === FT.PING) {
        // Untagged PING has no demux target once 2+ sessions share the socket.
        try {
          socket.send(
            packFrame(
              header.id === undefined
                ? { t: FT.PONG }
                : { t: FT.PONG, id: header.id },
            ),
          );
        } catch (error) {
          log.error("external ws pong failed:", error);
        }
        return;
      }
      if (state.pendingHello && header && header.t === FT.HELLO_PENDING) {
        return;
      }
      if (
        state.pendingHello &&
        header &&
        (header.t === FT.HELLO_ACK || header.t === FT.HELLO_ERR)
      ) {
        state.pendingHello(header);
        return;
      }
      if (
        header?.t === FT.PUSH &&
        header.op === OP.CATALOGS_CHANGED &&
        typeof header.ns !== "number"
      ) {
        const cats = Array.isArray(header.catalogs) ? header.catalogs : [];
        onCatalogsChanged?.(
          wsId,
          cats.filter((c): c is string => typeof c === "string"),
        );
        return;
      }
      const replyId = header && connectionReplyId(header);
      const request =
        replyId === undefined ? undefined : state.requests.get(replyId);
      if (header && request) {
        if (header.t === FT.RESP_CHUNK) {
          const cats = header.catalogs;
          if (Array.isArray(cats)) {
            for (const c of cats)
              if (typeof c === "string") request.catalogs.push(c);
          }
        } else if (header.t === FT.RESP_END) {
          request.finish();
        } else {
          request.finish(
            new Error(
              `n6k: ${String(header.exception_message ?? "connection request failed")}`,
            ),
          );
        }
        return;
      }
      deliverInbound(state, d);
      // After delivery, so the engine has the PUSH before anything refetches on this notice.
      const schemas = invalidatedSchemas(header);
      if (schemas && typeof header?.ns === "number") {
        for (const catalog of wsIdToCatalogs.get(wsId) ?? []) {
          if (catalogNs.get(catalog) === header.ns) {
            onInvalidated?.(catalog, schemas);
          }
        }
      }
    };
    // Socket drop -> notify every bound pump; the driver never closes the app's socket.
    state.onClose = function () {
      failConnectionRequests(state, "n6k: socket closed");
      rpcClients.get(wsId)?.fail("n6k: socket closed");
      for (const w of state.targets.values()) {
        try {
          w.postMessage({ type: "ws-closed", reason: "app socket closed" });
        } catch {
          // worker already terminated (detach) — nothing to notify.
        }
      }
    };
    socket.addEventListener("message", state.onMessage as EventListener);
    socket.addEventListener("close", state.onClose);
    socket.addEventListener("error", state.onClose);
    return state;
  }

  function commitExternalSocket(wsId: string, state: ExternalSocket): void {
    const prev = externalSockets.get(wsId);
    if (prev) teardownExternalSocket(prev);
    externalSockets.set(wsId, state);
    // Same port as duckdb-wasm's RPC, so it lands before any ATTACH the app sends after this.
    worker.postMessage({
      type: "n6k-hello-caps",
      wsId,
      capabilities: state.capabilities,
    });
  }

  async function handshakeExternalSocket(
    wsId: string,
    socket: WebSocket,
    token: string,
  ): Promise<ExternalSocket> {
    const state = createExternalSocket(socket, wsId);
    try {
      state.capabilities = await helloExternalSocket(state, token);
    } catch (error) {
      teardownExternalSocket(state);
      throw error;
    }
    return state;
  }

  // Remove our listeners from an app socket without closing it — the app owns it.
  function teardownExternalSocket(state: ExternalSocket): void {
    failConnectionRequests(state, "n6k: websocket was replaced or detached");
    state.targets.clear();
    state.socket.removeEventListener(
      "message",
      state.onMessage as EventListener,
    );
    state.socket.removeEventListener("close", state.onClose);
    state.socket.removeEventListener("error", state.onClose);
  }

  function failConnectionRequests(
    state: ExternalSocket,
    message: string,
  ): void {
    for (const request of state.requests.values()) {
      request.finish(new Error(message));
    }
  }

  function listCatalogs(wsId: string): Promise<string[]> {
    const state = externalSockets.get(wsId);
    if (!state) {
      return Promise.reject(new Error(`n6k: unknown wsId ${wsId}`));
    }
    if (state.socket.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error(`n6k: socket for ${wsId} is not open`));
    }
    return new Promise<string[]>((resolve, reject) => {
      const id = state.nextReqId++;
      const request: ConnectionRequest = {
        catalogs: [],
        finish(error) {
          clearTimeout(timer);
          state.requests.delete(id);
          if (error) reject(error);
          else resolve(request.catalogs);
        },
      };
      const timer = setTimeout(
        () =>
          request.finish(
            new Error(
              `n6k: listCatalogs got no reply within ${LIST_TIMEOUT_MS}ms`,
            ),
          ),
        LIST_TIMEOUT_MS,
      );
      state.requests.set(id, request);
      try {
        state.socket.send(packFrame({ t: FT.REQ, op: OP.CATALOG_LIST, id }));
      } catch (error) {
        request.finish(
          error instanceof Error ? error : new Error(String(error)),
        );
      }
    });
  }

  function detachExternalBinding(catalog: string): void {
    const wsId = externalBindings.get(catalog);
    if (wsId === undefined) return;
    externalBindings.delete(catalog);
    const state = externalSockets.get(wsId);
    const ns = catalogNs.get(catalog);
    if (state !== undefined && ns !== undefined) state.targets.delete(ns);
    catalogNs.delete(catalog);
    const cats = wsIdToCatalogs.get(wsId);
    if (cats) {
      cats.delete(catalog);
      if (cats.size > 0) {
        // Siblings still ride this socket; ask the server to close just THIS session.
        if (state !== undefined && ns !== undefined) {
          sendCloseSession(state.socket, ns);
        }
        return;
      }
      wsIdToCatalogs.delete(wsId);
    }
    if (state && !rpcClients.has(wsId)) {
      teardownExternalSocket(state);
      externalSockets.delete(wsId);
    }
  }

  // Close one mux session (CANCEL with reserved req id 0) on a socket that stays open for siblings.
  function sendCloseSession(socket: WebSocket, ns: number): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    try {
      socket.send(packFrame({ t: FT.CANCEL, id: 0, ns }));
    } catch (error) {
      log.error("failed to send close-session frame:", error);
    }
  }

  function helloExternalSocket(
    state: ExternalSocket,
    token: string,
  ): Promise<string[]> {
    const socket = state.socket;
    return new Promise<string[]>((resolve, reject) => {
      const finish = (error?: Error, capabilities: string[] = []) => {
        state.pendingHello = undefined;
        socket.removeEventListener("open", sendHello);
        socket.removeEventListener("close", onClose);
        socket.removeEventListener("error", onClose);
        if (error) reject(error);
        else resolve(capabilities);
      };
      const onClose = () =>
        finish(new Error("n6k: socket closed before FT_HELLO_ACK"));
      const sendHello = () => {
        try {
          socket.send(
            packFrame({
              t: FT.HELLO,
              token,
              protocol_version: N6K_PROTOCOL_VERSION,
              capabilities: [...HELLO_CAPS],
            }),
          );
        } catch (error) {
          finish(error instanceof Error ? error : new Error(String(error)));
        }
      };
      state.pendingHello = (header) => {
        if (header.t === FT.HELLO_ERR) {
          finish(
            new HelloRefusedError(
              `n6k: ${String(header.exception_message ?? "FT_HELLO refused")}`,
            ),
          );
        } else if (header.protocol_version === N6K_PROTOCOL_VERSION) {
          const caps = header.capabilities;
          finish(
            undefined,
            Array.isArray(caps)
              ? caps.filter((c): c is string => typeof c === "string")
              : [],
          );
        } else {
          finish(
            new Error(
              `n6k: server protocol_version=${String(header.protocol_version)}, expected ${N6K_PROTOCOL_VERSION}`,
            ),
          );
        }
      };
      socket.addEventListener("close", onClose);
      socket.addEventListener("error", onClose);
      if (socket.readyState === WebSocket.OPEN) sendHello();
      else if (socket.readyState === WebSocket.CONNECTING)
        socket.addEventListener("open", sendHello, { once: true });
      else onClose();
    });
  }

  function sendOpen(socket: WebSocket): (text: string) => void {
    // A browser WebSocket drops a send on a closed socket without throwing.
    return (text) => {
      if (socket.readyState !== WebSocket.OPEN) {
        throw new Error("n6k: websocket is not open");
      }
      socket.send(text);
    };
  }

  async function registerWebsocket(
    socket: WebSocket,
    token: string,
  ): Promise<RegisteredWebsocket> {
    const wsId = `ext_${nextWsId++}`;
    const state = await handshakeExternalSocket(wsId, socket, token);
    commitExternalSocket(wsId, state);
    const client = createJsonRpcClient(sendOpen(socket));
    rpcClients.set(wsId, client);
    const rpc: JsonRpcClient = {
      call: client.call,
      notify: client.notify,
      on: client.on,
    };
    const registration: RegisteredWebsocket = {
      wsId,
      capabilities: state.capabilities,
      get rpc() {
        if (!registration.capabilities.includes(CAP_JSONRPC)) {
          throw new Error(
            `n6k: server did not grant "${CAP_JSONRPC}" on ${wsId}`,
          );
        }
        return rpc;
      },
      close: () => closeRegistration(wsId, client),
    };
    registrations.set(wsId, registration);
    return registration;
  }

  function closeRegistration(wsId: string, client: JsonRpcEndpoint): void {
    if (rpcClients.get(wsId) !== client) return;
    rpcClients.delete(wsId);
    registrations.delete(wsId);
    client.fail("n6k: registration closed");
    if (wsIdToCatalogs.has(wsId)) return;
    const state = externalSockets.get(wsId);
    if (state) {
      teardownExternalSocket(state);
      externalSockets.delete(wsId);
    }
  }

  async function replaceWebsocket(
    wsId: string,
    socket: WebSocket,
    token: string,
  ): Promise<void> {
    const prev = externalSockets.get(wsId);
    prev?.socket.removeEventListener("close", prev.onClose);
    prev?.socket.removeEventListener("error", prev.onClose);
    let state: ExternalSocket;
    try {
      state = await handshakeExternalSocket(wsId, socket, token);
    } catch (error) {
      if (prev && prev.socket.readyState === WebSocket.OPEN) {
        prev.socket.addEventListener("close", prev.onClose);
        prev.socket.addEventListener("error", prev.onClose);
      } else {
        prev?.onClose();
      }
      throw error;
    }
    const cats = [...(wsIdToCatalogs.get(wsId) ?? [])];
    commitExternalSocket(wsId, state);
    const client = rpcClients.get(wsId);
    if (client) {
      client.fail("n6k: websocket was replaced");
      client.rebind(sendOpen(socket));
    }
    const registration = registrations.get(wsId);
    if (registration) registration.capabilities = state.capabilities;
    for (const catalog of cats) reconnect(catalog);
  }

  // Reset the catalog's channel rings and respawn its pump; WsClient re-runs its handshake on next request.
  function reconnect(catalog: string): void {
    const pp = pumpParams.get(catalog);
    if (!pp) {
      log.error("reconnect: unknown catalog (not attached):", catalog);
      return;
    }
    notifyStatus(catalog, "reconnecting");
    const carrier = carrierOf(pp);
    resetCarrier(carrier);
    const w = spawnPumpWsWorker(catalog, carrier);
    if (pp.external) {
      const state = externalSockets.get(pp.wsId);
      const ns = catalogNs.get(catalog);
      if (state && ns !== undefined) {
        bindPump(state, ns, w);
        w.addEventListener("message", function (ev: MessageEvent) {
          if (ev.data && ev.data.type === "ws-out") {
            try {
              state.socket.send(ev.data.data as ArrayBuffer);
            } catch (error) {
              log.error("external vsock send failed:", error);
            }
          }
        } as EventListener);
        const openTransport = () =>
          w.postMessage({ type: "open-pump-external" });
        if (state.socket.readyState === WebSocket.OPEN) openTransport();
        else
          state.socket.addEventListener("open", openTransport, {
            once: true,
          });
      }
    } else {
      w.postMessage({ type: "open-pump", url: pp.wsUrl });
    }
  }

  // Use onmessage (not addEventListener): it auto-starts the port; an unstarted port drops frames.
  // eslint-disable-next-line unicorn/prefer-add-event-listener
  n6kChannel.port1.onmessage = function (e) {
    if (!e.data) return;

    switch (e.data.type) {
      case "n6k-vsock-attach": {
        // Byte-pump path: spawn a pump ws-worker that dials wsUrl and moves frames WS↔channel.
        const catalog: string = e.data.catalog || "";
        const wsUrl: string = e.data.wsUrl || "";
        const carrier = readCarrier(e.data);
        pumpParams.set(catalog, { external: false, wsUrl, ...carrier });
        const w = spawnPumpWsWorker(catalog, carrier);
        w.postMessage({ type: "open-pump", url: wsUrl });
        break;
      }
      case "n6k-vsock-attach-ws": {
        // Byte-pump over an app-registered socket: main thread bridges binary frames to/from the pump.
        const catalog: string = e.data.catalog || "";
        const wsId: string = e.data.wsId || "";
        // Session ns (from C++ WsClient, echoed by server) for demux.
        const ns = e.data.ns;
        const carrier = readCarrier(e.data);
        const state = externalSockets.get(wsId);
        if (!state || typeof ns !== "number" || ns === 0) {
          log.error("n6k-vsock-attach-ws: unknown wsId or missing ns:", wsId);
          notifyStatus(catalog, "error");
          break;
        }
        const w = spawnPumpWsWorker(catalog, carrier);
        // Route this session's inbound frames (by ns) to its pump, replaying any buffered.
        bindPump(state, ns, w);
        externalBindings.set(catalog, wsId);
        catalogNs.set(catalog, ns);
        let cats = wsIdToCatalogs.get(wsId);
        if (!cats) {
          cats = new Set();
          wsIdToCatalogs.set(wsId, cats);
        }
        cats.add(catalog);
        pumpParams.set(catalog, { external: true, wsId, ...carrier });
        // pump worker → app socket: relay outbound frames over the shared socket.
        w.addEventListener("message", function (ev: MessageEvent) {
          if (ev.data && ev.data.type === "ws-out") {
            try {
              state.socket.send(ev.data.data as ArrayBuffer);
            } catch (error) {
              log.error("external vsock send failed:", error);
            }
          }
        } as EventListener);
        const openTransport = () =>
          w.postMessage({ type: "open-pump-external" });
        if (state.socket.readyState === WebSocket.OPEN) {
          openTransport();
        } else {
          state.socket.addEventListener("open", openTransport, { once: true });
        }
        break;
      }
      case "n6k-vsock-detach": {
        const catalog: string | undefined = e.data.catalog;
        if (catalog) {
          // Schedule the ring-region free, honored once the pump acks release
          // (pump-ring-released). Set BEFORE closing the pump so the ack isn't missed.
          const pp = pumpParams.get(catalog);
          const pump = wsWorkers.get(catalog);
          if (pp && pump) {
            pendingRingFree.set(pump, pp.ringOffset);
          }
          detachExternalBinding(catalog);
          wsWorkers.remove(catalog);
          pumpParams.delete(catalog);
        }
        break;
      }
      // No default
    }
  };

  function dispose(): void {
    wsWorkers.clear(); // graceful WS close + terminate backstop per ws-worker
    for (const [wsId, client] of rpcClients) closeRegistration(wsId, client);
    for (const state of externalSockets.values()) teardownExternalSocket(state);
    externalSockets.clear();
    externalBindings.clear();
    catalogNs.clear();
    wsIdToCatalogs.clear();
    pumpParams.clear();
    pendingRingFree.clear();
    try {
      n6kChannel.port1.close();
    } catch {
      /* ignore */
    }
    untrackWorker(worker, `#${instanceId} duckdb-worker`);
  }

  return {
    worker,
    reconnect,
    registerWebsocket,
    replaceWebsocket,
    listCatalogs,
    dispose,
  };
}
