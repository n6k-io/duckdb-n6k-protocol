// Installed by LOAD n6k_client on duckdb-wasm's main worker: the `globalThis.n6k` API the extension calls, and
// the control channel to the page. Works without a page: until `n6k-init` arrives, page-bound messages drop.

import { applyHostDebugFlag, createLogger, setLogInstance } from "../logger";
import { createVsockRegistry } from "../virtual-socket/duckdb-glue";
import { N6K_PROTOCOL_VERSION } from "../ws-framing";
import { PUMP_CLOSED, PUMP_OPENED, PUMP_RELEASED } from "./pump-status";

const log = createLogger("main-glue");

export type N6kApi = {
  version: number;
  // The page's npm version, from n6k-init; "" without a page.
  npmVersion: string;
  // A dialed transport: register its duckdb-side endpoint and report its status word to the page. The pump
  // thread owns the socket.
  vsockDial(
    key: string,
    connectionId: string,
    ringPtr: number,
    doorbellPtr: number,
    statusPtr: number,
  ): string;
  // Or bridge the socket the page registered under `connectionId`; the page's pump owns the ring.
  vsockAdopt(
    key: string,
    connectionId: string,
    ringPtr: number,
    doorbellPtr: number,
  ): string;
  vsockClose(key: string): void;
  helloCapabilities(id: string): string;
  isRegistered(id: string): string;
  registeredFor(wsUrl: string, token: string): string;
  resolveWsUrl(spec: string): string;
  catalogJoined(connectionId: string, catalog: string): void;
  catalogLeft(connectionId: string, catalog: string): void;
  invalidated(catalog: string, schemas: string): void;
};

declare global {
  var n6k: N6kApi | undefined;
}

export type N6kInitMessage = {
  type: "n6k-init";
  n6kPort: MessagePort;
  protocolVersion: number;
  npmVersion: string;
  debug?: boolean;
  instanceId?: number;
};

// `up`: its socket has not closed since; a closed one still answers to its id, awaiting a replacement.
type Registered = {
  capabilities: string[];
  url: string;
  token: string;
  up: boolean;
};

export function resolveWsUrl(spec: string): string {
  if (spec.startsWith("ws://") || spec.startsWith("wss://")) return spec;
  if (spec.startsWith("https://")) return "wss://" + spec.slice(8);
  if (spec.startsWith("http://")) return "ws://" + spec.slice(7);
  if (spec.startsWith("n6ks://")) return "wss://" + spec.slice(7);
  if (spec.startsWith("n6k://")) return "ws://" + spec.slice(6);
  const u = new URL(spec, globalThis.location.origin);
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  return u.toString();
}

export function installMainGlue(
  memory: WebAssembly.Memory,
  free: (ptr: number) => void,
): void {
  if (globalThis.n6k) return;

  let port: MessagePort | null = null;
  const post = (msg: unknown, transfer: Transferable[] = []) => {
    port?.postMessage(msg, transfer);
  };

  const vsock = createVsockRegistry();
  // Ring offset per open transport; tells whether a ring free belongs to the currently registered endpoint.
  const ringOf = new Map<string, number>();
  // Dialed transports' doorbells, for n6k-reconnect.
  const doorbellOf = new Map<string, number>();
  const registered = new Map<string, Registered>();

  const openChannel = (key: string, ringPtr: number, doorbellPtr: number) => {
    if (!(ringPtr > 0) || !(doorbellPtr > 0)) {
      throw new Error(
        `n6k: invalid vsock ring/doorbell pointer (ring=${ringPtr}, doorbell=${doorbellPtr})`,
      );
    }
    vsock.register(key, memory.buffer, undefined, ringPtr);
    ringOf.set(key, ringPtr);
  };

  const ringDoorbell = (doorbellPtr: number) => {
    const doorbell = new Int32Array(memory.buffer, doorbellPtr, 1);
    Atomics.add(doorbell, 0, 1);
    Atomics.notify(doorbell, 0);
  };

  // Reports each newly set bit; frees the word once the pump has released.
  const watchStatus = async (key: string, statusPtr: number) => {
    const word = new Int32Array(memory.buffer, statusPtr, 1);
    let seen = 0;
    for (;;) {
      const wait = Atomics.waitAsync(word, 0, seen);
      if (wait.async) await wait.value;
      const now = Atomics.load(word, 0);
      const fresh = now & ~seen;
      seen = now;
      if (fresh & PUMP_OPENED)
        post({ type: "n6k-conn-status", key, open: true });
      if (fresh & PUMP_CLOSED)
        post({ type: "n6k-conn-status", key, open: false });
      if (fresh & PUMP_RELEASED) {
        free(statusPtr);
        post({ type: "n6k-vsock-detach", key });
        return;
      }
    }
  };

  const api: N6kApi = {
    version: N6K_PROTOCOL_VERSION,
    npmVersion: "",

    vsockDial(key, connectionId, ringPtr, doorbellPtr, statusPtr) {
      openChannel(key, ringPtr, doorbellPtr);
      doorbellOf.set(key, doorbellPtr);
      post({ type: "n6k-vsock-dial", key, connectionId });
      void watchStatus(key, statusPtr);
      return "OK";
    },

    vsockAdopt(key, connectionId, ringPtr, doorbellPtr) {
      const registration = registered.get(connectionId);
      if (!registration) {
        return `ERROR: no registered socket ${connectionId}`;
      }
      if (!registration.up) {
        return `ERROR: socket ${connectionId} is closed; call replaceWebsocket`;
      }
      openChannel(key, ringPtr, doorbellPtr);
      post({
        type: "n6k-vsock-adopt",
        key,
        connectionId,
        wasmMemory: memory,
        ringOffset: ringPtr,
        doorbellOffset: doorbellPtr,
      });
      return "OK";
    },

    vsockClose(key) {
      if (doorbellOf.delete(key)) {
        // The pump thread frees a dialed ring next; drop every view of it first.
        vsock.unregister(key);
        ringOf.delete(key);
        return;
      }
      vsock.close(key);
      post({ type: "n6k-vsock-detach", key });
    },

    helloCapabilities: (id) =>
      (registered.get(id)?.capabilities ?? []).join(","),

    isRegistered: (id) => (registered.has(id) ? "1" : ""),

    registeredFor(wsUrl, token) {
      const href = new URL(wsUrl).href;
      for (const [id, r] of registered) {
        if (r.up && r.token === token && new URL(r.url).href === href) {
          return id;
        }
      }
      return "";
    },

    resolveWsUrl,

    catalogJoined: (connectionId, catalog) =>
      post({ type: "n6k-catalog-joined", connectionId, catalog }),

    catalogLeft: (connectionId, catalog) =>
      post({ type: "n6k-catalog-left", connectionId, catalog }),

    invalidated: (catalog, schemas) =>
      post({
        type: "n6k-invalidated",
        catalog,
        schemas: schemas.split(",").filter(Boolean),
      }),
  };
  globalThis.n6k = api;

  // duckdb-wasm's own handler also sees these; once instantiated it ignores unknown types.
  globalThis.addEventListener("message", (ev: MessageEvent) => {
    const m = ev.data;
    if (!m || typeof m !== "object") return;
    switch (m.type) {
      case "n6k-init": {
        const init = m as N6kInitMessage;
        if (init.protocolVersion !== N6K_PROTOCOL_VERSION) {
          init.n6kPort.postMessage({
            type: "n6k-init-error",
            message:
              `n6k protocol mismatch: extension v${N6K_PROTOCOL_VERSION}, ` +
              `npm package v${init.protocolVersion}`,
          });
          return;
        }
        applyHostDebugFlag(init.debug);
        if (init.instanceId !== undefined) setLogInstance(init.instanceId);
        api.npmVersion = init.npmVersion;
        port = init.n6kPort;
        post({ type: "n6k-init-ack" });
        log.debug("page attached");
        return;
      }
      case "n6k-registered": {
        registered.set(m.id as string, {
          capabilities: m.capabilities as string[],
          url: m.url as string,
          token: m.token as string,
          up: true,
        });
        return;
      }
      case "n6k-socket-down": {
        const r = registered.get(m.id as string);
        if (r) r.up = false;
        return;
      }
      case "n6k-unregistered": {
        registered.delete(m.id as string);
        return;
      }
      case "n6k-reconnect": {
        // Closing the rings is a drop to C++, which stops the transport and redials on the next request.
        const key = m.key as string;
        const doorbellPtr = doorbellOf.get(key);
        if (doorbellPtr === undefined) return;
        vsock.close(key);
        ringDoorbell(doorbellPtr);
        return;
      }
      // Sent only after the page's pump released an adopted ring.
      case "n6k-vsock-free-ring": {
        const off = m.ringOffset as number;
        if (typeof m.key === "string" && ringOf.get(m.key) === off) {
          vsock.unregister(m.key);
          ringOf.delete(m.key);
        }
        if (typeof off === "number") free(off);
        return;
      }
      case "n6k-probe-memory": {
        const buffer = memory.buffer;
        (m.replyPort as MessagePort | undefined)?.postMessage({
          type: "n6k-probe-memory-result",
          found: true,
          isSharedArrayBuffer:
            typeof SharedArrayBuffer !== "undefined" &&
            buffer instanceof SharedArrayBuffer,
          byteLength: buffer.byteLength,
          source: "wasmMemory",
        });
        return;
      }
      case "n6k-vsock-selftest": {
        const ringOffset = [...ringOf.values()].at(-1);
        (m.replyPort as MessagePort | undefined)?.postMessage({
          type: "n6k-vsock-selftest-result",
          attached: ringOffset != null,
          heap: ringOffset != null,
          ringOffset: ringOffset ?? null,
          canFree: true,
          openRings: ringOf.size,
        });
        return;
      }
      // No default: everything else is duckdb-wasm's.
    }
  });
}
