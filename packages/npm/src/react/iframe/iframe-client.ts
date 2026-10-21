import type { ConnectionLike, ArrowLikeResult } from "../../connection-shape";
import type { AttachConfig } from "../attachment";
import {
  INITIAL_MIRROR,
  type ClientMsg,
  type HostMsg,
  type Mirror,
} from "./protocol";

const DEFAULT_QUERY_TIMEOUT_MS = 30_000;

type TimerHandle = ReturnType<typeof setTimeout>;

export type IframeClientOptions = {
  // Decode Arrow IPC bytes; injected so this core imports no arrow.
  decodeIPC: (bytes: Uint8Array) => ArrowLikeResult;
  queryTimeoutMs?: number;
  setTimeoutFn?: (fn: () => void, ms: number) => TimerHandle;
  clearTimeoutFn?: (handle: TimerHandle) => void;
};

export type IframeClient = {
  conn: ConnectionLike;
  getMirror: () => Mirror;
  subscribe: (onChange: () => void) => () => void;
  // (Re)bind the port; a fresh port at any time lets a reloaded parent re-link.
  attachPort: (port: MessagePort) => void;
  setDesired: (catalog: string, config: AttachConfig) => void;
  removeDesired: (catalog: string) => void;
  reconnect: (catalog: string) => void;
  teardown: (reason?: string) => void;
};

type Pending = {
  resolve: (r: ArrowLikeResult) => void;
  reject: (e: Error) => void;
  timer: TimerHandle;
};

export function createIframeClient(opts: IframeClientOptions): IframeClient {
  const queryTimeoutMs = opts.queryTimeoutMs ?? DEFAULT_QUERY_TIMEOUT_MS;
  const setTimer =
    opts.setTimeoutFn ?? ((fn: () => void, ms: number) => setTimeout(fn, ms));
  const clearTimer =
    opts.clearTimeoutFn ?? ((handle: TimerHandle) => clearTimeout(handle));

  let port: MessagePort | null = null;
  let mirror: Mirror = INITIAL_MIRROR;
  let nextId = 1;
  let torn = false;
  const pending = new Map<number, Pending>();
  const listeners = new Set<() => void>();

  function onMessage(ev: MessageEvent): void {
    const d = ev.data as HostMsg | undefined;
    if (!d || typeof d !== "object" || !("__n6k" in d)) return;
    switch (d.__n6k) {
      case "state": {
        mirror = {
          status: d.status,
          error: d.error,
          connStatus: d.connStatus,
          // Absent from a parent on an older build.
          invalidations: d.invalidations ?? {},
          desired: d.desired,
          attached: d.attached,
          errors: d.errors,
        };
        for (const l of listeners) l();
        break;
      }
      case "queryOk": {
        const p = pending.get(d.id);
        if (!p) return;
        pending.delete(d.id);
        clearTimer(p.timer);
        p.resolve(opts.decodeIPC(new Uint8Array(d.ipc)));
        break;
      }
      case "queryErr": {
        const p = pending.get(d.id);
        if (!p) return;
        pending.delete(d.id);
        clearTimer(p.timer);
        p.reject(new Error(d.message));
        break;
      }
    }
  }

  function send(msg: ClientMsg): void {
    port?.postMessage(msg);
  }

  const conn: ConnectionLike = {
    query(sql: string): Promise<ArrowLikeResult> {
      return new Promise<ArrowLikeResult>((resolve, reject) => {
        if (torn) {
          reject(new Error("iframe DuckDB client torn down"));
          return;
        }
        if (!port) {
          reject(new Error("iframe not connected to parent DuckDB"));
          return;
        }
        const id = nextId++;
        const timer = setTimer(() => {
          if (pending.delete(id)) {
            reject(
              new Error(`DuckDB query timed out after ${queryTimeoutMs}ms`),
            );
          }
        }, queryTimeoutMs);
        pending.set(id, { resolve, reject, timer });
        port.postMessage({ __n6k: "query", id, sql });
      });
    },
    // Connections live in the parent; teardown() is the real dispose, not close().
    close: async () => {},
  };

  function attachPort(p: MessagePort): void {
    port = p;
    p.addEventListener("message", onMessage);
    p.start();
  }

  function teardown(reason = "iframe DuckDB client torn down"): void {
    torn = true;
    for (const p of pending.values()) {
      clearTimer(p.timer);
      p.reject(new Error(reason));
    }
    pending.clear();
    port?.close();
    port = null;
  }

  return {
    conn,
    getMirror: () => mirror,
    subscribe(onChange) {
      listeners.add(onChange);
      return () => listeners.delete(onChange);
    },
    attachPort,
    setDesired: (catalog, config) =>
      send({ __n6k: "setDesired", catalog, config }),
    removeDesired: (catalog) => send({ __n6k: "removeDesired", catalog }),
    reconnect: (catalog) => send({ __n6k: "reconnect", catalog }),
    teardown,
  };
}
