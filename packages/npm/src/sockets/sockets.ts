import {
  HelloRefusedError,
  type RegisteredWebsocket,
} from "../create-n6k-worker";
import { logger as log } from "../logger";

export type SocketToken = string | (() => string | Promise<string>);

export type SocketSpec = { connect: () => WebSocket; token: SocketToken };

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

export type Callbacks = {
  registered: (name: string, reg: RegisteredWebsocket) => void;
  failed: (name: string, message: string) => void;
  recovered: (name: string) => void;
  released: (name: string) => void;
};

export function runConnectSocket(
  name: string,
  spec: SocketSpec,
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

// One reconnecting socket per spec; the returned function tears them all down.
export function startSockets(
  sockets: Record<string, SocketSpec>,
  register: Register,
  replace: Replace,
  cb: Callbacks,
): () => void {
  const stops = Object.entries(sockets).map(([name, spec]) =>
    runConnectSocket(name, spec, register, replace, cb),
  );
  return () => {
    for (const stop of stops) stop();
  };
}
