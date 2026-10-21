// Runtime-toggleable debug logger; enable via localStorage 'n6k:debug' or globalThis.N6K_DEBUG. warn/error always print.
// Exported so the main thread can snapshot debug state and forward it into spawned workers.
export function isDebugEnabled(): boolean {
  try {
    const g = globalThis as { N6K_DEBUG?: unknown };
    if (g.N6K_DEBUG) return true;
  } catch {
    // some sandboxed contexts throw on globalThis access
  }
  try {
    if (
      typeof localStorage !== "undefined" &&
      localStorage.getItem("n6k:debug")
    ) {
      return true;
    }
  } catch {
    // workers / privacy modes throw on localStorage access
  }
  return false;
}

// Workers apply the host's snapshotted debug flag; only turns debug on, never off.
export function applyHostDebugFlag(enabled: unknown): void {
  if (enabled) {
    (globalThis as { N6K_DEBUG?: unknown }).N6K_DEBUG = true;
  }
}

export type Logger = {
  debug: (...args: unknown[]) => void;
  log: (...args: unknown[]) => void;
  warn: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
};

// Capture native console at load so a thread that wraps `console` can't recurse back into us.
const nativeLog = console.log.bind(console);
const nativeWarn = console.warn.bind(console);
const nativeError = console.error.bind(console);

export function createLogger(scope?: string): Logger {
  const prefix = scope ? `[n6k:${scope}]` : "[n6k]";
  return {
    debug: (...args) => {
      if (isDebugEnabled()) nativeLog(prefix, ...args);
    },
    log: (...args) => {
      if (isDebugEnabled()) nativeLog(prefix, ...args);
    },
    warn: (...args) => {
      nativeWarn(prefix, ...args);
    },
    error: (...args) => {
      nativeError(prefix, ...args);
    },
  };
}

export const logger = createLogger();
