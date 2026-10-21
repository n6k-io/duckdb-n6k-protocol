import type { ConnectionLike } from "../../connection-shape";
import type { WsStatus } from "../../types";

export type Capability =
  | "reconnect"
  | "status"
  | "forceDrop"
  | "protocolCounts";

export type BackendCapabilities = Record<Capability, boolean>;

// coi-only: the wasm extension is built for shared memory (wasm_threads), which
// only the browser coi bundle can load. The old node-eh (`wasm-node`) and
// browser-eh (`browser`) backends can't load it and were retired.
export type BackendName = "native" | "browser-threads";

export type BackendSetupOptions = {
  onStatus?: (catalog: string, status: WsStatus) => void;
};

export type MemoryProbe = {
  found: boolean;
  isSharedArrayBuffer: boolean;
  byteLength: number;
  source: string;
};

export type VsockSelftest = {
  attached: boolean;
  // true == rings live in duckdb's shared wasm heap (Phase 1); false == SAB bootstrap.
  heap: boolean;
  ringOffset: number | null;
  // Whether the duckdb worker captured the wasm `free` export (needed to reclaim the
  // heap ring region on detach).
  canFree: boolean;
};

export type BackendHandle = {
  readonly name: BackendName;
  readonly caps: BackendCapabilities;
  readonly conn: ConnectionLike;
  reconnect(catalog: string): void;
  cleanup(): Promise<void>;
  // Inspect the duckdb-wasm heap for the shared-memory data path. Only the
  // browser backends (which own a page) implement it; undefined elsewhere.
  probeMemory?(): Promise<MemoryProbe>;
  // Report whether an attached catalog's vsock channel took the heap-backed path.
  // Browser backends only; undefined elsewhere.
  vsockSelftest?(catalog?: string): Promise<VsockSelftest>;
};

export type Backend = {
  readonly name: BackendName;
  readonly caps: BackendCapabilities;
  available(): boolean;
  // Human-readable reason available() returned false — used by the fail-fast
  // guard so an explicitly-selected-but-unavailable backend errors loudly
  // instead of silently skipping every test.
  unavailableReason?(): string;
  setup(opts?: BackendSetupOptions): Promise<BackendHandle>;
};
