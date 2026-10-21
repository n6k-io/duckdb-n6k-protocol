import { nanoid } from "nanoid";
import { logger as log } from "./logger";

const METHOD_NOT_FOUND = -32_601;
const DEFAULT_TIMEOUT_MS = 60_000;

export class JsonRpcError extends Error {
  readonly code: number;
  readonly data: unknown;

  constructor(code: number, message: string, data?: unknown) {
    super(message);
    this.name = "JsonRpcError";
    this.code = code;
    this.data = data;
  }
}

export type JsonRpcCallOptions = {
  signal?: AbortSignal;
  timeoutMs?: number;
};

export type JsonRpcClient = {
  // Rejects with JsonRpcError for a server error reply; a plain Error on timeout (DEFAULT_TIMEOUT_MS), abort
  // (the signal's reason if an Error), malformed reply, send failure, socket close/replace, or registration close().
  call: (
    method: string,
    params?: unknown,
    opts?: JsonRpcCallOptions,
  ) => Promise<unknown>;
  notify: (method: string, params?: unknown) => void;
  // Server notifications only; a server request (with `id`) is auto-answered METHOD_NOT_FOUND. Returns `off`.
  on: (method: string, handler: (params: unknown) => void) => () => void;
};

export type JsonRpcEndpoint = JsonRpcClient & {
  receive: (text: string) => void;
  // Reject every pending call; the client stays usable.
  fail: (reason: string) => void;
  rebind: (send: (text: string) => void) => void;
};

type Pending = {
  resolve: (result: unknown) => void;
  reject: (error: Error) => void;
};

export function createJsonRpcClient(
  send: (text: string) => void,
): JsonRpcEndpoint {
  let sendText = send;
  const pending = new Map<string, Pending>();
  const handlers = new Map<string, Set<(params: unknown) => void>>();

  function call(
    method: string,
    params?: unknown,
    opts: JsonRpcCallOptions = {},
  ): Promise<unknown> {
    const { signal, timeoutMs = DEFAULT_TIMEOUT_MS } = opts;
    if (signal?.aborted) return Promise.reject(abortReason(signal));
    const id = nanoid();
    return new Promise<unknown>((resolve, reject) => {
      const onAbort = () => settle.reject(abortReason(signal!));
      const timer = setTimeout(
        () =>
          settle.reject(
            new Error(`n6k: ${method} got no reply within ${timeoutMs}ms`),
          ),
        timeoutMs,
      );
      const done = () => {
        pending.delete(id);
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
      };
      const settle: Pending = {
        resolve(result) {
          done();
          resolve(result);
        },
        reject(error) {
          done();
          reject(error);
        },
      };
      pending.set(id, settle);
      signal?.addEventListener("abort", onAbort, { once: true });
      try {
        sendText(JSON.stringify(message(method, params, id)));
      } catch (error) {
        settle.reject(
          error instanceof Error ? error : new Error(String(error)),
        );
      }
    });
  }

  function notify(method: string, params?: unknown): void {
    sendText(JSON.stringify(message(method, params)));
  }

  function on(method: string, handler: (params: unknown) => void): () => void {
    let set = handlers.get(method);
    if (!set) {
      set = new Set();
      handlers.set(method, set);
    }
    set.add(handler);
    return () => {
      set.delete(handler);
      if (set.size === 0 && handlers.get(method) === set) {
        handlers.delete(method);
      }
    };
  }

  function receive(text: string): void {
    let msg: unknown;
    try {
      msg = JSON.parse(text);
    } catch {
      log.debug("jsonrpc: dropping non-JSON text frame");
      return;
    }
    if (typeof msg !== "object" || msg === null || Array.isArray(msg)) {
      log.debug("jsonrpc: dropping non-object message");
      return;
    }
    const m = msg as Record<string, unknown>;
    if (typeof m.method === "string") {
      if ("id" in m) {
        sendQuietly({
          jsonrpc: "2.0",
          id: m.id,
          error: {
            code: METHOD_NOT_FOUND,
            message: `method not found: ${m.method}`,
          },
        });
        return;
      }
      const set = handlers.get(m.method);
      if (!set) {
        log.debug("jsonrpc: no handler for notification", m.method);
        return;
      }
      for (const handler of set) {
        try {
          handler(m.params);
        } catch (error) {
          log.error(`jsonrpc: ${m.method} handler threw:`, error);
        }
      }
      return;
    }
    const request = typeof m.id === "string" ? pending.get(m.id) : undefined;
    if (!request) {
      if (m.error) log.warn("jsonrpc: error with no matching call:", m.error);
      else log.debug("jsonrpc: dropping reply for unknown id", m.id);
      return;
    }
    if ("error" in m) {
      const e = m.error;
      if (
        typeof e === "object" &&
        e !== null &&
        typeof (e as Record<string, unknown>).code === "number" &&
        typeof (e as Record<string, unknown>).message === "string"
      ) {
        const { code, message, data } = e as {
          code: number;
          message: string;
          data?: unknown;
        };
        request.reject(new JsonRpcError(code, message, data));
      } else {
        request.reject(
          new Error(`n6k: malformed JSON-RPC error: ${JSON.stringify(e)}`),
        );
      }
    } else if ("result" in m) {
      request.resolve(m.result);
    } else {
      request.reject(
        new Error("n6k: JSON-RPC reply has neither result nor error"),
      );
    }
  }

  function fail(reason: string): void {
    for (const request of pending.values()) {
      request.reject(new Error(reason));
    }
  }

  function rebind(next: (text: string) => void): void {
    sendText = next;
  }

  function sendQuietly(msg: unknown): void {
    try {
      sendText(JSON.stringify(msg));
    } catch (error) {
      log.error("jsonrpc: send failed:", error);
    }
  }

  return { call, notify, on, receive, fail, rebind };
}

function message(method: string, params: unknown, id?: string) {
  return {
    jsonrpc: "2.0",
    method,
    ...(params === undefined ? {} : { params }),
    ...(id === undefined ? {} : { id }),
  };
}

function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return reason instanceof Error ? reason : new Error("n6k: call aborted");
}
