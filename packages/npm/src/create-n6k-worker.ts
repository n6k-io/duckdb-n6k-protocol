/* eslint-disable unicorn/prefer-global-this */
import {
  packFrame,
  FT,
  OP,
  HELLO_CAPS,
  N6K_PROTOCOL_VERSION,
  tryHeader,
  type FrameHeader,
} from "./ws-framing";
import { connectionReplyId } from "./virtual-socket/ws-demux";
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
  // The connection id: ATTACH '' AS x (TYPE n6k, CONNECTION '<wsId>').
  wsId: string;
  // From the latest HELLO_ACK; replaceWebsocket updates it.
  capabilities: string[];
  // Throws if the server did not grant `jsonrpc`.
  readonly rpc: JsonRpcClient;
  // Fails pending calls; the socket stays bridged until its last catalog is detached. Idempotent.
  // Trap: a held `rpc` still sends after close() but its replies are dropped, so calls end only by timeout
  // or abort.
  close: () => void;
};

export type N6kWorkerHandle = {
  worker: Worker;
  // Close the socket under an attached catalog, and so under every catalog on it, failing their in-flight
  // requests. It redials on the next request from any of them. Throws for an app-registered socket, which only
  // replaceWebsocket can swap. Logs and does nothing for an unknown catalog; status via onStatus.
  reconnect: (catalog: string) => void;
  // Resolves on HELLO_ACK; rejects on HELLO_ERR (HelloRefusedError), a protocol_version mismatch, the HELLO
  // send throwing, or the socket closing or erroring first. No deadline. Binary frames are n6k, text frames are JSON-RPC.
  registerWebsocket: (
    socket: WebSocket,
    token: string,
  ) => Promise<RegisteredWebsocket>;
  // Swap the socket behind a wsId and FT_HELLO it; its catalogs reopen on the new socket on their next
  // request. On success, in-flight reqs and rpc calls fail and rpc moves to the new socket. On handshake failure it rethrows and keeps the
  // previous socket if still OPEN; otherwise the previous socket's close handling runs. A wsId not currently
  // registered does not throw: the socket is bridged under it, with no registration or rpc.
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

// These helper workers do their work over SAB/Atomics, not the event loop. Under Bun a
// classic worker that parks on worker_threads' parentPort never releases the parent loop on
// terminate(), so unref them at spawn; the duckdb worker stays ref'd and db.terminate() frees it.
const unref = (handle: unknown) => (handle as { unref?: () => void }).unref?.();

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

  // Pump ws-worker → its ring, pending a free. Set when C++ closes the transport; the free fires only
  // when that pump acks it has released the ring ("pump-ring-released"), so we never free a region
  // still in use.
  const pendingRingFree = new Map<
    Worker,
    { key: string; ringOffset: number }
  >();

  const PUMP_RETIRE_MS = 5000;
  const LIST_TIMEOUT_MS = 10_000;
  type ConnectionRequest = {
    catalogs: string[];
    finish: (error?: Error) => void;
  };

  // App-registered sockets by id; inbound listener attached at registration so the HELLO_ACK isn't missed.
  type ExternalSocket = {
    socket: WebSocket;
    token: string;
    pendingHello?: (header: FrameHeader) => void;
    // HELLO_ACK `capabilities`: negotiated here, read by the wasm hub via n6k.helloCapabilities.
    capabilities: string[];
    // The pump of the connection C++ holds over this socket, while it holds one.
    pump?: Worker;
    // Connection-scoped requests (no ns) issued from here, keyed by id; never reach a pump.
    requests: Map<number, ConnectionRequest>;
    nextReqId: number;
    onMessage: (ev: MessageEvent) => void;
    onClose: () => void;
  };
  const externalSockets = new Map<string, ExternalSocket>();
  // Per id until close(); outlives replaceWebsocket.
  const rpcClients = new Map<string, JsonRpcEndpoint>();
  const registrations = new Map<string, RegisteredWebsocket>();
  // Closed while C++ still held a connection over the socket; torn down once it lets go.
  const closing = new Set<string>();

  // One pump per connection transport (one C++ dial), by the transport's key.
  type Channel = {
    connectionId: string;
    pump: Worker;
    ringOffset: number;
  };
  const channels = new Map<string, Channel>();
  // The catalogs riding each connection, as C++ reports them, so status is reported per catalog.
  const catalogsOf = new Map<string, Set<string>>();
  const connectionOf = new Map<string, string>();
  const connectionStatus = new Map<string, WsStatus>();

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

  function setConnectionStatus(connectionId: string, status: WsStatus): void {
    connectionStatus.set(connectionId, status);
    for (const catalog of catalogsOf.get(connectionId) ?? []) {
      notifyStatus(catalog, status);
    }
  }

  // A ring still pending here is never freed: terminate() is not synchronous, so the pump may still be writing it.
  function retirePump(w: Worker, label: string): void {
    intentionalClose.add(w);
    untrackWorker(w, label);
    try {
      w.postMessage({ type: "close" });
    } catch {
      /* ignore */
    }
    unref(
      setTimeout(function () {
        if (pendingRingFree.delete(w)) {
          log.error(`pump never released its ring: ${label}`);
        }
        try {
          w.terminate();
        } catch {
          /* ignore */
        }
      }, PUMP_RETIRE_MS),
    );
  }

  // Byte-pump ws-worker: moves frames WS↔channel; the protocol is in the C++ ConnectionHub.
  function spawnPump(
    key: string,
    connectionId: string,
    carrier: ChannelCarrier,
  ): Worker {
    const label = `#${instanceId} ws-worker ${key} connection=${connectionId}`;
    const w = new Worker(wsWorkerUrl, { type: "classic" });
    trackWorker(w, label);
    unref(w);
    w.addEventListener("error", function (ev) {
      log.error("ws-worker (pump) error:", ev.message || ev);
    });
    w.postMessage({ type: "init-channel", label, debug, ...carrier });

    let connected = false;
    let disconnectNotified = false;
    w.addEventListener("message", function (ev: MessageEvent) {
      if (!ev.data) return;
      if (ev.data.type === "pump-ring-released") {
        const pending = pendingRingFree.get(w);
        if (pending !== undefined) {
          pendingRingFree.delete(w);
          try {
            worker.postMessage({ type: "n6k-vsock-free-ring", ...pending });
          } catch {
            // duckdb worker already torn down; region goes with the whole heap.
          }
        }
        return;
      }
      if (ev.data.type === "ready") {
        connected = true;
        setConnectionStatus(connectionId, "connected");
      } else if (
        (ev.data.type === "closed" || ev.data.type === "error") &&
        connected &&
        !disconnectNotified &&
        !intentionalClose.has(w)
      ) {
        disconnectNotified = true;
        log.error(
          `ws-worker (pump) disconnected: ${label} reason=${ev.data.reason || "(none)"}`,
        );
        setConnectionStatus(connectionId, "disconnected");
      }
    } as EventListener);
    return w;
  }

  // Attach the durable inbound bridge to an app socket under its id.
  function createExternalSocket(
    socket: WebSocket,
    id: string,
    token: string,
  ): ExternalSocket {
    try {
      socket.binaryType = "arraybuffer";
    } catch {
      // Some environments lock binaryType; fine if already "arraybuffer".
    }
    const state: ExternalSocket = {
      socket,
      token,
      capabilities: [],
      requests: new Map(),
      nextReqId: 1,
      onMessage: () => {},
      onClose: () => {},
    };
    state.onMessage = function (ev: MessageEvent) {
      const d = ev.data;
      if (typeof d === "string") {
        rpcClients.get(id)?.receive(d);
        return;
      }
      if (!(d instanceof ArrayBuffer)) return;
      const header = tryHeader(d);
      if (header?.t === FT.PING) {
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
          id,
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
      // With no connection over the socket nothing awaits these: every session opens after its pump binds.
      state.pump?.postMessage({ type: "ws-in", data: d }, [d]);
    };
    // Socket drop -> the pump over it; the driver never closes the app's socket.
    state.onClose = function () {
      failConnectionRequests(state, "n6k: socket closed");
      rpcClients.get(id)?.fail("n6k: socket closed");
      worker.postMessage({ type: "n6k-socket-down", id });
      try {
        state.pump?.postMessage({
          type: "ws-closed",
          reason: "app socket closed",
        });
      } catch {
        // worker already terminated — nothing to notify.
      }
    };
    socket.addEventListener("message", state.onMessage as EventListener);
    socket.addEventListener("close", state.onClose);
    socket.addEventListener("error", state.onClose);
    return state;
  }

  function commitExternalSocket(id: string, state: ExternalSocket): void {
    const prev = externalSockets.get(id);
    if (prev) {
      setConnectionStatus(id, "reconnecting");
      if (prev.pump) {
        // Reported below as a recovery, not a drop. The connection redials onto this socket on its next request.
        intentionalClose.add(prev.pump);
        try {
          prev.pump.postMessage({
            type: "ws-closed",
            reason: "websocket replaced",
          });
        } catch {
          /* ignore */
        }
      }
      teardownExternalSocket(prev);
    }
    externalSockets.set(id, state);
    // Same port as duckdb-wasm's RPC, so it lands before any ATTACH the app sends after this.
    worker.postMessage({
      type: "n6k-registered",
      id,
      capabilities: state.capabilities,
      url: state.socket.url,
      token: state.token,
    });
    // Already handshaken, so the socket is up now, whether or not a query has redialed onto it yet.
    if (prev) setConnectionStatus(id, "connected");
  }

  async function handshakeExternalSocket(
    id: string,
    socket: WebSocket,
    token: string,
  ): Promise<ExternalSocket> {
    const state = createExternalSocket(socket, id, token);
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
    state.pump = undefined;
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

  function listCatalogs(id: string): Promise<string[]> {
    const state = externalSockets.get(id);
    if (!state) {
      return Promise.reject(new Error(`n6k: unknown wsId ${id}`));
    }
    if (state.socket.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error(`n6k: socket for ${id} is not open`));
    }
    return new Promise<string[]>((resolve, reject) => {
      const reqId = state.nextReqId++;
      const request: ConnectionRequest = {
        catalogs: [],
        finish(error) {
          clearTimeout(timer);
          state.requests.delete(reqId);
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
      state.requests.set(reqId, request);
      try {
        state.socket.send(
          packFrame({ t: FT.REQ, op: OP.CATALOG_LIST, id: reqId }),
        );
      } catch (error) {
        request.finish(
          error instanceof Error ? error : new Error(String(error)),
        );
      }
    });
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
    // A connection id: ATTACH ... (CONNECTION '<id>') rides this socket.
    const id = crypto.randomUUID();
    const state = await handshakeExternalSocket(id, socket, token);
    commitExternalSocket(id, state);
    const client = createJsonRpcClient(sendOpen(socket));
    rpcClients.set(id, client);
    const rpc: JsonRpcClient = {
      call: client.call,
      notify: client.notify,
      on: client.on,
    };
    const registration: RegisteredWebsocket = {
      wsId: id,
      capabilities: state.capabilities,
      get rpc() {
        if (!registration.capabilities.includes(CAP_JSONRPC)) {
          throw new Error(
            `n6k: server did not grant "${CAP_JSONRPC}" on ${id}`,
          );
        }
        return rpc;
      },
      close: () => closeRegistration(id, client),
    };
    registrations.set(id, registration);
    return registration;
  }

  function closeRegistration(id: string, client: JsonRpcEndpoint): void {
    if (rpcClients.get(id) !== client) return;
    rpcClients.delete(id);
    registrations.delete(id);
    client.fail("n6k: registration closed");
    worker.postMessage({ type: "n6k-unregistered", id });
    const state = externalSockets.get(id);
    if (state?.pump) {
      closing.add(id);
      return;
    }
    finishClose(id);
  }

  function finishClose(id: string): void {
    closing.delete(id);
    const state = externalSockets.get(id);
    if (state) {
      teardownExternalSocket(state);
      externalSockets.delete(id);
    }
  }

  async function replaceWebsocket(
    id: string,
    socket: WebSocket,
    token: string,
  ): Promise<void> {
    const prev = externalSockets.get(id);
    prev?.socket.removeEventListener("close", prev.onClose);
    prev?.socket.removeEventListener("error", prev.onClose);
    let state: ExternalSocket;
    try {
      state = await handshakeExternalSocket(id, socket, token);
    } catch (error) {
      if (prev && prev.socket.readyState === WebSocket.OPEN) {
        prev.socket.addEventListener("close", prev.onClose);
        prev.socket.addEventListener("error", prev.onClose);
      } else {
        prev?.onClose();
      }
      throw error;
    }
    commitExternalSocket(id, state);
    const client = rpcClients.get(id);
    if (client) {
      client.fail("n6k: websocket was replaced");
      client.rebind(sendOpen(socket));
    }
    const registration = registrations.get(id);
    if (registration) registration.capabilities = state.capabilities;
  }

  function reconnect(catalog: string): void {
    const connectionId = connectionOf.get(catalog);
    if (connectionId === undefined) {
      log.error("reconnect: unknown catalog (not attached):", catalog);
      return;
    }
    if (externalSockets.has(connectionId)) {
      throw new Error(
        `n6k: catalog ${catalog} rides an app-registered socket; call replaceWebsocket`,
      );
    }
    setConnectionStatus(connectionId, "reconnecting");
    for (const ch of channels.values()) {
      if (ch.connectionId !== connectionId) continue;
      try {
        ch.pump.postMessage({ type: "close" });
      } catch {
        /* ignore */
      }
    }
  }

  // Use onmessage (not addEventListener): it auto-starts the port; an unstarted port drops frames.
  // eslint-disable-next-line unicorn/prefer-add-event-listener
  n6kChannel.port1.onmessage = function (e) {
    if (!e.data) return;

    switch (e.data.type) {
      case "n6k-vsock-dial": {
        const key: string = e.data.key;
        const connectionId: string = e.data.connectionId;
        const carrier = readCarrier(e.data);
        const w = spawnPump(key, connectionId, carrier);
        channels.set(key, {
          connectionId,
          pump: w,
          ringOffset: carrier.ringOffset,
        });
        w.postMessage({ type: "open-pump", url: e.data.wsUrl });
        break;
      }
      case "n6k-vsock-adopt": {
        // Byte-pump over an app-registered socket: main thread bridges binary frames to/from the pump.
        const key: string = e.data.key;
        const connectionId: string = e.data.connectionId;
        const carrier = readCarrier(e.data);
        const w = spawnPump(key, connectionId, carrier);
        channels.set(key, {
          connectionId,
          pump: w,
          ringOffset: carrier.ringOffset,
        });
        const state = externalSockets.get(connectionId);
        if (!state) {
          w.postMessage({ type: "ws-closed", reason: "registration closed" });
          break;
        }
        state.pump = w;
        w.addEventListener("message", function (ev: MessageEvent) {
          if (ev.data && ev.data.type === "ws-out") {
            try {
              state.socket.send(ev.data.data as ArrayBuffer);
            } catch (error) {
              log.error("external vsock send failed:", error);
            }
          }
        } as EventListener);
        // Closed since vsockAdopt checked: no ws-closed here, a drop during the reopen would stop the catalog for
        // good. The OPEN times out instead, and replaceWebsocket still recovers.
        if (state.socket.readyState === WebSocket.OPEN) {
          w.postMessage({ type: "open-pump-external" });
        }
        break;
      }
      case "n6k-vsock-detach": {
        const key: string = e.data.key;
        const ch = channels.get(key);
        if (!ch) break;
        channels.delete(key);
        // Set BEFORE closing the pump so its release ack isn't missed.
        pendingRingFree.set(ch.pump, { key, ringOffset: ch.ringOffset });
        retirePump(ch.pump, `#${instanceId} ws-worker ${key}`);
        const state = externalSockets.get(ch.connectionId);
        if (state?.pump === ch.pump) state.pump = undefined;
        if (closing.has(ch.connectionId) && !state?.pump) {
          finishClose(ch.connectionId);
        }
        break;
      }
      case "n6k-catalog-joined": {
        const connectionId: string = e.data.connectionId;
        const catalog: string = e.data.catalog;
        let cats = catalogsOf.get(connectionId);
        if (!cats) {
          cats = new Set();
          catalogsOf.set(connectionId, cats);
        }
        cats.add(catalog);
        connectionOf.set(catalog, connectionId);
        const status = connectionStatus.get(connectionId);
        if (status !== undefined) notifyStatus(catalog, status);
        break;
      }
      case "n6k-catalog-left": {
        const connectionId: string = e.data.connectionId;
        const catalog: string = e.data.catalog;
        const cats = catalogsOf.get(connectionId);
        cats?.delete(catalog);
        if (cats?.size === 0) {
          catalogsOf.delete(connectionId);
          connectionStatus.delete(connectionId);
        }
        if (connectionOf.get(catalog) === connectionId) {
          connectionOf.delete(catalog);
        }
        break;
      }
      case "n6k-invalidated": {
        onInvalidated?.(e.data.catalog as string, e.data.schemas as string[]);
        break;
      }
      // No default
    }
  };

  function dispose(): void {
    for (const [key, ch] of channels) {
      retirePump(ch.pump, `#${instanceId} ws-worker ${key}`);
    }
    channels.clear();
    for (const [id, client] of rpcClients) closeRegistration(id, client);
    for (const state of externalSockets.values()) teardownExternalSocket(state);
    externalSockets.clear();
    closing.clear();
    catalogsOf.clear();
    connectionOf.clear();
    connectionStatus.clear();
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
