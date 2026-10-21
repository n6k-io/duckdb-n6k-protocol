import { useEffect, useRef, useState } from "react";
import {
  HelloRefusedError,
  type RegisteredWebsocket,
} from "../create-n6k-worker";
import { logger as log } from "../logger";

export type SocketToken = string | (() => string | Promise<string>);

export type SocketSpec =
  | { connect: () => WebSocket; token: SocketToken }
  | { socket: WebSocket | null; token: SocketToken };

type Register = (
  socket: WebSocket,
  token: string,
) => Promise<RegisteredWebsocket>;
type Replace = (
  wsId: string,
  socket: WebSocket,
  token: string,
) => Promise<void>;

export type ProviderSockets = {
  registrations: Record<string, RegisteredWebsocket>;
  // Per socket name, the current failure: any register/connect failure before the first registration, or the
  // server's refusal (HelloRefusedError) of a reconnect's FT_HELLO. Cleared on register, replace or release.
  socketErrors: Record<string, string>;
};

const MIN_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 30_000;

function resolveToken(token: SocketToken): Promise<string> {
  return Promise.resolve(typeof token === "function" ? token() : token);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function omitKey<V>(
  obj: Record<string, V>,
  key: string,
): Record<string, V> {
  if (!(key in obj)) return obj;
  const next = { ...obj };
  delete next[key];
  return next;
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

export function useProviderSockets(
  sockets: Record<string, SocketSpec> | undefined,
  ready: boolean,
  register: Register,
  replace: Replace,
): ProviderSockets {
  const [registrations, setRegistrations] = useState<
    Record<string, RegisteredWebsocket>
  >({});
  const [socketErrors, setSocketErrors] = useState<Record<string, string>>({});

  const [callbacks] = useState<Callbacks>(() => ({
    registered: (name: string, reg: RegisteredWebsocket) => {
      setRegistrations((prev) => ({ ...prev, [name]: reg }));
      setSocketErrors((prev) => omitKey(prev, name));
    },
    failed: (name: string, message: string) =>
      setSocketErrors((prev) => ({ ...prev, [name]: message })),
    recovered: (name: string) => setSocketErrors((prev) => omitKey(prev, name)),
    released: (name: string) => {
      setRegistrations((prev) => omitKey(prev, name));
      setSocketErrors((prev) => omitKey(prev, name));
    },
  }));

  const [connectSpecs] = useState(() =>
    Object.entries(sockets ?? {}).filter(
      (e): e is [string, Extract<SocketSpec, { connect: unknown }>] =>
        "connect" in e[1],
    ),
  );

  useEffect(() => {
    if (!ready) return;
    const disposers = connectSpecs.map(([name, spec]) =>
      runConnectSocket(name, spec, register, replace, callbacks),
    );
    return () => {
      for (const dispose of disposers) dispose();
    };
  }, [ready, connectSpecs, register, replace, callbacks]);

  const appSockets = useRef<Record<string, AppSocketState>>({});
  useEffect(() => {
    if (!ready) return;
    for (const [name, spec] of Object.entries(sockets ?? {})) {
      if (!("socket" in spec)) continue;
      const st = (appSockets.current[name] ??= {
        ws: null,
        reg: null,
        chain: Promise.resolve(),
        disposed: false,
      });
      if (spec.socket === st.ws) continue;
      st.ws = spec.socket;
      const ws = spec.socket;
      if (ws === null) continue;
      const token = spec.token;
      st.chain = st.chain.then(() =>
        bindAppSocket(name, st, ws, token, register, replace, callbacks),
      );
    }
  });

  useEffect(() => {
    const states = appSockets.current;
    return () => {
      for (const [name, st] of Object.entries(states)) {
        st.disposed = true;
        st.reg?.close();
        callbacks.released(name);
      }
      appSockets.current = {};
    };
  }, [callbacks]);

  return { registrations, socketErrors };
}

type Callbacks = {
  registered: (name: string, reg: RegisteredWebsocket) => void;
  failed: (name: string, message: string) => void;
  recovered: (name: string) => void;
  released: (name: string) => void;
};

type AppSocketState = {
  ws: WebSocket | null;
  reg: RegisteredWebsocket | null;
  chain: Promise<void>;
  disposed: boolean;
};

async function bindAppSocket(
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

function runConnectSocket(
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
