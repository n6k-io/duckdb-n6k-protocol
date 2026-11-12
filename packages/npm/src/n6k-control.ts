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
import { doorbellPumpEndpoint } from "./virtual-socket/doorbell-endpoint";
import {
  acceptInbound,
  createInboundBuffer,
  runPump,
} from "./virtual-socket/pump";
import { logger as log, isDebugEnabled } from "./logger";
import { CAP_JSONRPC } from "./protocol-generated";
import {
  createJsonRpcClient,
  type JsonRpcClient,
  type JsonRpcEndpoint,
} from "./jsonrpc";
import type { WsStatus } from "./types";
import { N6K_VERSION } from "./version";

export type N6kControlOptions = {
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

export type N6kControl = {
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
  // Stop the pumps and close the control channel. Does NOT touch the duckdb worker (owned by
  // AsyncDuckDB.terminate()).
  dispose: () => void;
};

// Byte pump for one adopted transport: moves frames between an app-registered socket and its ring in duckdb's heap.
type Pump = {
  open: () => void;
  wsIn: (frame: ArrayBuffer) => void;
  wsClosed: (reason: string) => void;
  // Resolves once nothing touches the ring again; the ring may then be freed.
  retire: () => Promise<void>;
  // Torn down on purpose: its close is not reported as "disconnected".
  intentional: boolean;
};

// `worker` is the one passed to `new AsyncDuckDB(logger, worker)`; LOAD n6k_client must have run in it.
// Rejects if the extension's glue does not answer n6k-init (not loaded, or an extension predating it) or
// speaks another protocol version.
export async function openN6kControl(
  worker: Worker,
  {
    onStatus,
    onCatalogsChanged,
    onInvalidated,
    instanceId = 0,
  }: N6kControlOptions = {},
): Promise<N6kControl> {
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

  // Ring region of an adopted transport: duckdb's shared WebAssembly.Memory + the C++ malloc offsets.
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
    pump?: Pump;
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

  // One per connection transport (one C++ dial), by the transport's key. `pump`: the page's pump for an
  // adopted socket; a dialed socket's pump runs inside the extension.
  type Channel = {
    connectionId: string;
    pump?: Pump;
    ringOffset?: number;
    open: boolean;
  };
  const channels = new Map<string, Channel>();
  let disposed = false;
  // The catalogs riding each connection, as C++ reports them, so status is reported per catalog.
  const catalogsOf = new Map<string, Set<string>>();
  const connectionOf = new Map<string, string>();
  const connectionStatus = new Map<string, WsStatus>();

  // Private channel for n6k control frames; keeps them off duckdb-wasm's RPC postMessage binding.
  const n6kChannel = new MessageChannel();

  const ATTACH_TIMEOUT_MS = 10_000;
  function attach(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        initWaiter = null;
        reject(
          new Error(
            "n6k: no answer to n6k-init: LOAD n6k_client has not run, or the extension predates the embedded glue",
          ),
        );
      }, ATTACH_TIMEOUT_MS);
      initWaiter = (error) => {
        clearTimeout(timer);
        initWaiter = null;
        if (error) reject(error);
        else resolve();
      };
      worker.postMessage(
        {
          type: "n6k-init",
          n6kPort: n6kChannel.port2,
          protocolVersion: N6K_PROTOCOL_VERSION,
          npmVersion: N6K_VERSION,
          debug,
          instanceId,
        },
        [n6kChannel.port2],
      );
    });
  }
  let initWaiter: ((error?: Error) => void) | null = null;

  function setConnectionStatus(connectionId: string, status: WsStatus): void {
    connectionStatus.set(connectionId, status);
    for (const catalog of catalogsOf.get(connectionId) ?? []) {
      notifyStatus(catalog, status);
    }
  }

  // The protocol is in the C++ ConnectionHub; this only moves bytes. Frames queued before open() wait in the TX ring.
  function createPump(
    key: string,
    connectionId: string,
    carrier: ChannelCarrier,
    send: (frame: Uint8Array<ArrayBuffer>) => void,
  ): Pump {
    const label = `#${instanceId} pump ${key} connection=${connectionId}`;
    const endpoint = doorbellPumpEndpoint(
      carrier.wasmMemory.buffer,
      carrier.ringOffset,
      carrier.doorbellOffset,
    );
    const inbound = createInboundBuffer();
    let stopped = false;
    let opened = false;
    let connected = false;
    let disconnectNotified = false;
    let done: Promise<void> = Promise.resolve();

    function disconnected(reason: string): void {
      if (!connected || disconnectNotified || pump.intentional) return;
      disconnectNotified = true;
      log.error(`pump disconnected: ${label} reason=${reason}`);
      setConnectionStatus(connectionId, "disconnected");
    }

    function sendFrame(frame: Uint8Array<ArrayBuffer>): void {
      if (!opened) {
        disconnected("cannot send frame: socket not open");
        return;
      }
      try {
        send(frame);
      } catch (error) {
        log.error("external vsock send failed:", error);
      }
    }

    function stop(reason: string): void {
      opened = false;
      stopped = true;
      endpoint.close();
      disconnected(reason);
    }

    const pump: Pump = {
      intentional: false,
      open() {
        opened = true;
        done = runPump(
          endpoint,
          { send: sendFrame },
          inbound,
          () => stopped,
        ).catch((error: unknown) => {
          log.error("pump failed:", error);
        });
        connected = true;
        setConnectionStatus(connectionId, "connected");
      },
      wsIn(frame) {
        if (stopped) return;
        if (acceptInbound(endpoint, inbound, new Uint8Array(frame))) return;
        stop("inbound overflow");
      },
      wsClosed: stop,
      retire() {
        pump.intentional = true;
        stopped = true;
        // Wakes a pump parked on rings C++ already closed, whose own wake was spent before this.
        inbound.kick?.();
        // After the final drain, so queued frames still reach the socket. Without endpoint.close(), C++ never sees
        // reconnect()'s drop.
        return done.then(() => {
          endpoint.close();
          opened = false;
        });
      },
    };
    return pump;
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
      state.pump?.wsIn(d);
    };
    // Socket drop -> the pump over it; the driver never closes the app's socket.
    state.onClose = function () {
      failConnectionRequests(state, "n6k: socket closed");
      rpcClients.get(id)?.fail("n6k: socket closed");
      worker.postMessage({ type: "n6k-socket-down", id });
      state.pump?.wsClosed("app socket closed");
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
        prev.pump.intentional = true;
        prev.pump.wsClosed("websocket replaced");
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
    for (const [key, ch] of channels) {
      if (ch.connectionId !== connectionId) continue;
      // The glue closes the rings; C++ sees the drop and redials on the next request.
      ch.open = false;
      worker.postMessage({ type: "n6k-reconnect", key });
    }
  }

  // Use onmessage (not addEventListener): it auto-starts the port; an unstarted port drops frames.
  // eslint-disable-next-line unicorn/prefer-add-event-listener
  n6kChannel.port1.onmessage = function (e) {
    if (!e.data) return;

    switch (e.data.type) {
      case "n6k-init-ack": {
        initWaiter?.();
        break;
      }
      case "n6k-init-error": {
        initWaiter?.(new Error(e.data.message as string));
        break;
      }
      case "n6k-vsock-dial": {
        channels.set(e.data.key as string, {
          connectionId: e.data.connectionId as string,
          open: false,
        });
        break;
      }
      case "n6k-conn-status": {
        const ch = channels.get(e.data.key as string);
        if (!ch) break;
        if (e.data.open) {
          ch.open = true;
          setConnectionStatus(ch.connectionId, "connected");
        } else if (ch.open) {
          ch.open = false;
          log.error(`dialed socket disconnected: ${e.data.key}`);
          setConnectionStatus(ch.connectionId, "disconnected");
        }
        break;
      }
      case "n6k-vsock-adopt": {
        const key: string = e.data.key;
        const connectionId: string = e.data.connectionId;
        const carrier = readCarrier(e.data);
        const state = externalSockets.get(connectionId);
        const pump = createPump(key, connectionId, carrier, (frame) =>
          state?.socket.send(frame),
        );
        channels.set(key, {
          connectionId,
          pump,
          ringOffset: carrier.ringOffset,
          open: false,
        });
        if (!state) {
          pump.wsClosed("registration closed");
          break;
        }
        state.pump = pump;
        // Closed since vsockAdopt checked: no wsClosed here, a drop during the reopen would stop the catalog for
        // good. The OPEN times out instead, and replaceWebsocket still recovers.
        if (state.socket.readyState === WebSocket.OPEN) pump.open();
        break;
      }
      case "n6k-vsock-detach": {
        const key: string = e.data.key;
        const ch = channels.get(key);
        if (!ch) break;
        channels.delete(key);
        // A dialed ring was already freed by the extension's pump.
        if (!ch.pump || ch.ringOffset === undefined) break;
        const ringOffset = ch.ringOffset;
        void ch.pump.retire().then(() => {
          if (disposed) return;
          worker.postMessage({ type: "n6k-vsock-free-ring", key, ringOffset });
        });
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
    disposed = true;
    for (const ch of channels.values()) void ch.pump?.retire();
    channels.clear();
    for (const [id, client] of rpcClients) closeRegistration(id, client);
    for (const state of externalSockets.values()) teardownExternalSocket(state);
    externalSockets.clear();
    closing.clear();
    catalogsOf.clear();
    connectionOf.clear();
    connectionStatus.clear();
    try {
      n6kChannel.port1.close();
    } catch {
      /* ignore */
    }
  }

  try {
    await attach();
  } catch (error) {
    dispose();
    throw error;
  }
  return {
    reconnect,
    registerWebsocket,
    replaceWebsocket,
    listCatalogs,
    dispose,
  };
}
