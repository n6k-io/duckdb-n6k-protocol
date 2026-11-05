/// <reference lib="webworker" />

declare function importScripts(...urls: string[]): void;

import { applyHostDebugFlag, createLogger, setLogInstance } from "../logger";
import { N6K_VERSION } from "../version";
import { createVsockRegistry } from "../virtual-socket/duckdb-glue";

const log = createLogger("duckdb-worker");
import { N6K_PROTOCOL_VERSION } from "../ws-framing";

interface N6kInitMessage {
  type: "n6k-init";
  mainWorkerUrl: string;
  // Private port for n6k control frames, kept off duckdb-wasm's shared RPC channel.
  n6kPort: MessagePort;
  // Page debug snapshot; workers can't read localStorage.
  debug?: boolean;
  instanceId?: number;
}

// One vsock channel per connection transport (a C++ dial), keyed by the transport's key.
const vsock = createVsockRegistry();

// Resolve a ws spec into a dialable ws(s):// URL; relative specs resolve against the worker origin.
function resolveWsUrl(spec: string): string {
  if (spec.startsWith("ws://") || spec.startsWith("wss://")) return spec;
  if (spec.startsWith("https://")) return "wss://" + spec.slice(8);
  if (spec.startsWith("http://")) return "ws://" + spec.slice(7);
  if (spec.startsWith("n6ks://")) return "wss://" + spec.slice(7);
  if (spec.startsWith("n6k://")) return "ws://" + spec.slice(6);
  const u = new URL(spec, globalThis.location.origin);
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  return u.toString();
}

interface N6kApi {
  // A connection's transport: a pump moves its opaque n6k frames between the ring C++ allocated at
  // `ringPtr` in duckdb's shared wasm heap and a WebSocket. `key` names this one transport.
  // Dial `wsUrl`:
  vsockDial(
    key: string,
    connectionId: string,
    ringPtr: number,
    doorbellPtr: number,
    wsUrl: string,
  ): string;
  // Or bridge the socket registered under `connectionId`:
  vsockAdopt(
    key: string,
    connectionId: string,
    ringPtr: number,
    doorbellPtr: number,
  ): string;
  vsockClose(key: string): void;
  // The HELLO_ACK capabilities negotiated on registered socket `id`, comma-joined.
  helloCapabilities(id: string): string;
  // "1" while `id` names a registered socket.
  isRegistered(id: string): string;
  // The registered socket dialed to `wsUrl` with `token`, or "".
  registeredFor(wsUrl: string, token: string): string;
  resolveWsUrl(spec: string): string;
  // Which catalogs ride which connection, so status is reported per catalog.
  catalogJoined(connectionId: string, catalog: string): void;
  catalogLeft(connectionId: string, catalog: string): void;
  // The server invalidated `schemas` (comma-joined) of `catalog`.
  invalidated(catalog: string, schemas: string): void;
}

interface N6kApiWithVersion extends N6kApi {
  version: number;
  npmVersion: string;
}

declare global {
  var n6k: N6kApiWithVersion;
}

globalThis.addEventListener("error", (e: ErrorEvent) => {
  log.error("uncaught error:", e.message || e);
  if (e.filename) log.error("at", e.filename + ":" + e.lineno + ":" + e.colno);
});

if ((globalThis as unknown as { name: string }).name === "em-pthread") {
  importScripts(decodeURIComponent(globalThis.location.hash.slice(1)));
}

// The shared-memory data path needs the exact WebAssembly.Memory duckdb-wasm runs
// on. In the coi build that Memory is created inside the DuckDBModule factory
// closure (not on any global) and IMPORTED into the wasm instance, so we grab it
// off the imports object of the standard WebAssembly.instantiate(Streaming) call
// duckdb makes. Installed before importScripts so it's in place when duckdb boots.
// (The production forwarder can instead export it from C++ via EM_JS, where
// `wasmMemory` is a visible closure var.)
let capturedWasmMemory: WebAssembly.Memory | undefined;
// duckdb-wasm's `free` (and `malloc`) export, grabbed off the instantiated instance.
// JS owns freeing the heap-backed ring region on detach (the pump-teardown timing is
// only known here), so C++ must NOT also free it — that would double-free.
let capturedWasmFree: ((ptr: number) => void) | undefined;
function memoryFromImports(imports: unknown): WebAssembly.Memory | undefined {
  if (!imports || typeof imports !== "object") return undefined;
  for (const ns of Object.values(imports as Record<string, unknown>)) {
    if (ns instanceof WebAssembly.Memory) return ns;
    if (ns && typeof ns === "object") {
      for (const v of Object.values(ns as Record<string, unknown>)) {
        if (v instanceof WebAssembly.Memory) return v;
      }
    }
  }
  return undefined;
}
// Pull the wasm `free` export off the resolved instantiate result (instance.exports.free
// or ._free). emscripten exports these because C++ uses them everywhere.
function captureExports<T>(result: T): T {
  const r = result as unknown as
    | { instance?: WebAssembly.Instance }
    | WebAssembly.Instance;
  const inst = (r as { instance?: WebAssembly.Instance }).instance ?? r;
  const exports = (inst as WebAssembly.Instance).exports as
    | Record<string, unknown>
    | undefined;
  const free = exports?.free ?? exports?._free;
  if (typeof free === "function") {
    capturedWasmFree = free as (ptr: number) => void;
  }
  return result;
}
{
  const origInstantiate = WebAssembly.instantiate;
  WebAssembly.instantiate = function (
    this: unknown,
    ...args: Parameters<typeof WebAssembly.instantiate>
  ) {
    const mem = memoryFromImports(args[1]);
    if (mem) capturedWasmMemory = mem;
    return origInstantiate.apply(this, args).then(captureExports);
  } as typeof WebAssembly.instantiate;
  if (WebAssembly.instantiateStreaming) {
    const origStreaming = WebAssembly.instantiateStreaming;
    WebAssembly.instantiateStreaming = function (
      this: unknown,
      ...args: Parameters<typeof WebAssembly.instantiateStreaming>
    ) {
      const mem = memoryFromImports(args[1]);
      if (mem) capturedWasmMemory = mem;
      return origStreaming.apply(this, args).then(captureExports);
    } as typeof WebAssembly.instantiateStreaming;
  }
}

// Open a vsock channel for a catalog over the ring region C++ malloc'd in duckdb's shared
// heap and register the duckdb-side endpoint. Returns the carrier fields to forward to the
// pump ws-worker so it rebuilds the same rings on its side.
// doorbellOffset: wasm-heap byte offset of the shared n6k I/O thread's doorbell int32; the
// pump bumps + notifies it after publishing an inbound frame so the one I/O thread wakes.
type VsockCarrier = {
  wasmMemory: WebAssembly.Memory;
  ringOffset: number;
  doorbellOffset: number;
};
// Ring offset per open transport: read by the n6k-vsock-selftest probe, and tells whether a
// ring free belongs to the currently registered endpoint.
const vsockAttachInfo = new Map<string, number>();
// Registered sockets, posted by the host after it handshakes each one.
// `up`: its socket has not closed since. A closed one still answers to its id, awaiting a replacement, but
// no URL ATTACH should ride it.
type Registered = {
  capabilities: string[];
  url: string;
  token: string;
  up: boolean;
};
const registered = new Map<string, Registered>();
function openVsockChannel(
  key: string,
  ringPtr: number,
  doorbellPtr: number,
): VsockCarrier {
  if (!capturedWasmMemory) {
    throw new Error(
      "n6k: duckdb's shared WebAssembly.Memory was not captured; cannot attach vsock channel",
    );
  }
  if (!(ringPtr > 0) || !(doorbellPtr > 0)) {
    throw new Error(
      `n6k: invalid vsock ring/doorbell pointer (ring=${ringPtr}, doorbell=${doorbellPtr})`,
    );
  }
  vsock.register(key, capturedWasmMemory.buffer, undefined, ringPtr);
  vsockAttachInfo.set(key, ringPtr);
  return {
    wasmMemory: capturedWasmMemory,
    ringOffset: ringPtr,
    doorbellOffset: doorbellPtr,
  };
}

globalThis.addEventListener(
  "message",
  function _n6kInitHandler(e: MessageEvent) {
    if (!(e.data && e.data.type === "n6k-init")) return;
    const { mainWorkerUrl, n6kPort, debug, instanceId } =
      e.data as N6kInitMessage;
    applyHostDebugFlag(debug);
    if (instanceId !== undefined) setLogInstance(instanceId);

    const api: N6kApiWithVersion = {
      version: N6K_PROTOCOL_VERSION,
      npmVersion: N6K_VERSION,

      // Returns at once: frames C++ sends before the socket opens wait in the TX ring.
      vsockDial(
        key: string,
        connectionId: string,
        ringPtr: number,
        doorbellPtr: number,
        wsUrl: string,
      ): string {
        const carrier = openVsockChannel(key, ringPtr, doorbellPtr);
        n6kPort.postMessage({
          type: "n6k-vsock-dial",
          key,
          connectionId,
          wsUrl,
          ...carrier,
        });
        return "OK";
      },

      vsockAdopt(
        key: string,
        connectionId: string,
        ringPtr: number,
        doorbellPtr: number,
      ): string {
        const registration = registered.get(connectionId);
        if (!registration) {
          return `ERROR: no registered socket ${connectionId}`;
        }
        if (!registration.up) {
          return `ERROR: socket ${connectionId} is closed; call replaceWebsocket`;
        }
        const carrier = openVsockChannel(key, ringPtr, doorbellPtr);
        n6kPort.postMessage({
          type: "n6k-vsock-adopt",
          key,
          connectionId,
          ...carrier,
        });
        return "OK";
      },

      vsockClose(key: string): void {
        vsock.close(key);
        n6kPort.postMessage({ type: "n6k-vsock-detach", key });
      },

      helloCapabilities(id: string): string {
        return (registered.get(id)?.capabilities ?? []).join(",");
      },

      isRegistered(id: string): string {
        return registered.has(id) ? "1" : "";
      },

      registeredFor(wsUrl: string, token: string): string {
        const href = new URL(wsUrl).href;
        for (const [id, r] of registered) {
          if (r.up && r.token === token && new URL(r.url).href === href) {
            return id;
          }
        }
        return "";
      },

      resolveWsUrl,

      catalogJoined(connectionId: string, catalog: string): void {
        n6kPort.postMessage({
          type: "n6k-catalog-joined",
          connectionId,
          catalog,
        });
      },

      catalogLeft(connectionId: string, catalog: string): void {
        n6kPort.postMessage({
          type: "n6k-catalog-left",
          connectionId,
          catalog,
        });
      },

      invalidated(catalog: string, schemas: string): void {
        n6kPort.postMessage({
          type: "n6k-invalidated",
          catalog,
          schemas: schemas.split(",").filter(Boolean),
        });
      },
    };

    globalThis.n6k = new Proxy(api, {
      get(target, prop, receiver) {
        if (prop in target) return Reflect.get(target, prop, receiver);
        if (typeof prop === "string") {
          throw new TypeError(
            `n6k API mismatch: "${prop}" is not a method on the JS worker ` +
              `(protocol v${N6K_PROTOCOL_VERSION}). ` +
              `WASM extension and JS worker are out of sync — rebuild both.`,
          );
        }
        return;
      },
    });

    globalThis.removeEventListener("message", _n6kInitHandler);

    // Persistent listener (survives init) for host→worker control messages.
    globalThis.addEventListener("message", function (ev: MessageEvent) {
      const m = ev.data;
      if (!m) return;
      if (m.type === "n6k-registered") {
        registered.set(m.id as string, {
          capabilities: m.capabilities as string[],
          url: m.url as string,
          token: m.token as string,
          up: true,
        });
        return;
      }
      if (m.type === "n6k-socket-down") {
        const r = registered.get(m.id as string);
        if (r) r.up = false;
        return;
      }
      if (m.type === "n6k-unregistered") {
        registered.delete(m.id as string);
        return;
      }
      // Reclaim a heap-backed ring region. The driver sends this only AFTER the pump
      // ws-worker has released the ring (its runPump exited), so no one still reads/writes
      // the region and freeing it (returning it to duckdb's allocator) is safe.
      if (m.type === "n6k-vsock-free-ring") {
        // Drop the (now-closed) endpoint first so its stale ring views can't be reused,
        // then return the region to duckdb's allocator.
        const off = m.ringOffset;
        if (typeof m.key === "string" && vsockAttachInfo.get(m.key) === off) {
          vsock.unregister(m.key);
          vsockAttachInfo.delete(m.key);
        }
        if (capturedWasmFree && typeof off === "number") capturedWasmFree(off);
        return;
      }
      // Diagnostic for the shared-memory data path: report duckdb-wasm's wasm heap
      // so callers can verify the coi build exposes a forwardable SharedArrayBuffer
      // (the substrate the RX/TX rings + pthread mailboxes live in). Reads
      // capturedWasmMemory, not Module.wasmMemory — the Memory is created inside the
      // DuckDBModule factory closure and never lands on a global, which is why the
      // WebAssembly.instantiate patch above exists. Reply on the transferred port so
      // duckdb-wasm's own message handler never observes the response.
      if (m.type === "n6k-probe-memory") {
        const port = m.replyPort as MessagePort | undefined;
        const buffer = capturedWasmMemory?.buffer;
        port?.postMessage({
          type: "n6k-probe-memory-result",
          found: capturedWasmMemory != null,
          isSharedArrayBuffer:
            typeof SharedArrayBuffer !== "undefined" &&
            buffer instanceof SharedArrayBuffer,
          byteLength: buffer?.byteLength ?? 0,
          source: capturedWasmMemory
            ? "WebAssembly.instantiate imports"
            : "none",
        });
        return;
      }
      // Selftest for the heap-backed data path: report the most recent transport's vsock ring offset
      // into duckdb's Memory. Reply on the transferred port so duckdb-wasm's own handler never sees it.
      if (m.type === "n6k-vsock-selftest") {
        const port = m.replyPort as MessagePort | undefined;
        const ringOffset = [...vsockAttachInfo.values()].at(-1);
        port?.postMessage({
          type: "n6k-vsock-selftest-result",
          attached: ringOffset != null,
          heap: ringOffset != null,
          ringOffset: ringOffset ?? null,
          canFree: capturedWasmFree != null,
          openRings: vsockAttachInfo.size,
        });
        return;
      }
    });

    // Route duckdb-wasm's console through our debug-gated logger to silence duplicate RPC-error noise.
    console.log = (...args: unknown[]) => log.log(...args);
    console.info = (...args: unknown[]) => log.log(...args);
    console.debug = (...args: unknown[]) => log.debug(...args);
    console.warn = (...args: unknown[]) => log.warn(...args);
    console.error = (...args: unknown[]) => log.error(...args);

    log.debug("importScripts:", mainWorkerUrl);
    try {
      importScripts(mainWorkerUrl);
    } catch (error) {
      log.error("importScripts failed:", (error as Error).message || error);
      throw error;
    }
  },
);
