import {
  HelloRefusedError,
  type RegisteredWebsocket,
} from "../create-n6k-worker";
import { logger as log } from "../logger";

export type SocketToken = string | (() => string | Promise<string>);

export type SocketSpec =
  | { connect: () => WebSocket; token: SocketToken }
  | { socket: WebSocket | null; token: SocketToken };

export type Register = (
  socket: WebSocket,
  token: string,
) => Promise<RegisteredWebsocket>;
export type Replace = (
  wsId: string,
  socket: WebSocket,
  token: string,
) => Promise<void>;

const MIN_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 30_000;

function resolveToken(token: SocketToken): Promise<string> {
  return Promise.resolve(typeof token === "function" ? token() : token);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function validateSockets(
  sockets: Record<string, SocketSpec> | undefined,
): void {
  for (const [name, spec] of Object.entries(sockets ?? {})) {
    const hasConnect = "connect" in spec;
    const hasSocket = "socket" in spec;
    if (hasConnect === hasSocket) {
      throw new Error(
        `n6k: sockets.${name} needs exactly one of 'connect' or 'socket'`,
      );
    }
  }
}

export type Callbacks = {
  registered: (name: string, reg: RegisteredWebsocket) => void;
  failed: (name: string, message: string) => void;
  recovered: (name: string) => void;
  released: (name: string) => void;
};

export type AppSocketState = {
  ws: WebSocket | null;
  reg: RegisteredWebsocket | null;
  chain: Promise<void>;
  disposed: boolean;
};

export async function bindAppSocket(
  name: string,
  st: AppSocketState,
  ws: WebSocket,
  token: SocketToken,
  register: Register,
  replace: Replace,
  cb: Callbacks,
): Promise<void> {
  if (st.disposed || st.ws !== ws) return;
  try {
    const t = await resolveToken(token);
    if (st.reg) {
      await replace(st.reg.wsId, ws, t);
      cb.recovered(name);
      return;
    }
    const reg = await register(ws, t);
    if (st.disposed) {
      reg.close();
      return;
    }
    st.reg = reg;
    cb.registered(name, reg);
  } catch (error) {
    log.error(`socket ${name}: register/replace failed:`, error);
    if (!st.reg || error instanceof HelloRefusedError) {
      cb.failed(name, messageOf(error));
    }
  }
}

export function runConnectSocket(
  name: string,
  spec: { connect: () => WebSocket; token: SocketToken },
  register: Register,
  replace: Replace,
  cb: Callbacks,
): () => void {
  let disposed = false;
  let ws: WebSocket | null = null;
  let reg: RegisteredWebsocket | null = null;
  let delay = MIN_BACKOFF_MS;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const retry = () => {
    if (disposed) return;
    timer = setTimeout(open, delay);
    delay = Math.min(delay * 2, MAX_BACKOFF_MS);
  };

  const onDrop = (ev: Event) => {
    if (ev.target !== ws) return;
    log.debug(`socket ${name} closed; reconnecting`);
    retry();
  };

  async function open(): Promise<void> {
    if (disposed) return;
    let next: WebSocket;
    try {
      next = spec.connect();
    } catch (error) {
      log.error(`socket ${name}: connect() threw:`, error);
      if (!reg) cb.failed(name, messageOf(error));
      retry();
      return;
    }
    ws = next;
    try {
      const t = await resolveToken(spec.token);
      if (reg) {
        await replace(reg.wsId, next, t);
        cb.recovered(name);
      } else {
        const r = await register(next, t);
        if (disposed) {
          r.close();
          return;
        }
        reg = r;
        cb.registered(name, r);
      }
    } catch (error) {
      if (disposed) return;
      log.error(`socket ${name}: register/replace failed:`, error);
      if (!reg || error instanceof HelloRefusedError) {
        cb.failed(name, messageOf(error));
      }
      next.close();
      retry();
      return;
    }
    if (disposed) return;
    delay = MIN_BACKOFF_MS;
    if (next.readyState >= WebSocket.CLOSING) {
      retry();
      return;
    }
    next.addEventListener("close", onDrop, { once: true });
  }

  void open();

  return () => {
    disposed = true;
    clearTimeout(timer);
    ws?.removeEventListener("close", onDrop);
    reg?.close();
    ws?.close();
    cb.released(name);
  };
}

export type SocketSetCallbacks = Callbacks;

export type SocketSet = {
  // Diffs against the previous call. Torn down: a removed name, a kind change, or a `connect` spec
  // that is a different object. A `socket` spec keeps its registration; a different `socket` value
  // goes through replaceWebsocket.
  apply(sockets: Record<string, SocketSpec>): void;
  dispose(): void;
};

type Live =
  | { kind: "connect"; spec: SocketSpec; dispose: () => void }
  | { kind: "socket"; spec: SocketSpec; state: AppSocketState };

export function createSocketSet(
  register: Register,
  replace: Replace,
  cb: Callbacks,
): SocketSet {
  const live = new Map<string, Live>();

  const stop = (name: string, entry: Live) => {
    if (entry.kind === "connect") {
      entry.dispose();
    } else {
      entry.state.disposed = true;
      entry.state.reg?.close();
      cb.released(name);
    }
    live.delete(name);
  };

  return {
    apply(sockets) {
      validateSockets(sockets);
      for (const [name, entry] of live) {
        const next = sockets[name];
        if (next === entry.spec) continue;
        if (next && entry.kind === "socket" && "socket" in next) continue;
        stop(name, entry);
      }
      for (const [name, spec] of Object.entries(sockets)) {
        const entry = live.get(name);
        if ("connect" in spec) {
          if (!entry) {
            live.set(name, {
              kind: "connect",
              spec,
              dispose: runConnectSocket(name, spec, register, replace, cb),
            });
          }
          continue;
        }
        const state =
          entry?.kind === "socket"
            ? entry.state
            : {
                ws: null,
                reg: null,
                chain: Promise.resolve(),
                disposed: false,
              };
        live.set(name, { kind: "socket", spec, state });
        if (spec.socket === state.ws) continue;
        state.ws = spec.socket;
        const ws = spec.socket;
        if (ws === null) continue;
        state.chain = state.chain.then(() =>
          bindAppSocket(name, state, ws, spec.token, register, replace, cb),
        );
      }
    },
    dispose() {
      for (const [name, entry] of live) stop(name, entry);
    },
  };
}
