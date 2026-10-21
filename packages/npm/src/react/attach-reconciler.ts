import { configEqual, renderAttachSql, type AttachConfig } from "./attachment";
import { omitKey } from "./use-provider-sockets";
import { logger as log } from "../logger";

type QueryConn = { query(sql: string): Promise<unknown> };

export type ReconcileInputs = {
  desired: Record<string, AttachConfig>;
  socketOf: Record<string, string>;
  registrations: Record<string, { wsId: string } | undefined>;
};

export type ReconcileHooks = {
  setup(c: string): string[];
  onAttached(c: string, config: AttachConfig): void;
  onDetached(c: string): void;
  onError(c: string, config: AttachConfig, message: string): void;
};

export type AttachReconciler = {
  run(inputs: ReconcileInputs): Promise<void>;
  stop(): void;
};

export function createAttachReconciler(
  conn: QueryConn,
  hooks: ReconcileHooks,
): AttachReconciler {
  let attached: Record<string, AttachConfig> = {};
  let latest: ReconcileInputs | null = null;
  let inFlight: Promise<void> | null = null;
  let dirty = false;
  let stopped = false;

  const detach = async (c: string) => {
    log.debug(`detaching ${c}...`);
    try {
      await conn.query(`DETACH ${c};`);
    } catch (error_) {
      log.error(`detach ${c} failed:`, error_);
    }
    attached = omitKey(attached, c);
    hooks.onDetached(c);
  };

  const attach = async (c: string, config: AttachConfig, wsId?: string) => {
    const sql = renderAttachSql(
      c,
      wsId === undefined
        ? config
        : { ...config, options: { ...config.options, wsId } },
    );
    try {
      // Pre-attach setup runs on the same connection immediately before the ATTACH it authorizes, with no
      // transaction; not logged since it may embed a token.
      for (const stmt of hooks.setup(c)) {
        await conn.query(stmt);
      }
      log.debug(sql);
      await conn.query(sql);
      attached = { ...attached, [c]: config };
      hooks.onAttached(c, config);
      log.debug(`attached ${c}`);
    } catch (error_) {
      const message = (error_ as Error).message;
      hooks.onError(c, config, message);
      log.error(`attach ${c} failed:`, error_);
    }
  };

  const pass = async ({
    desired,
    socketOf,
    registrations,
  }: ReconcileInputs) => {
    for (const c of Object.keys(attached)) {
      if (stopped || dirty) return;
      if (!(c in desired)) await detach(c);
    }
    for (const [c, config] of Object.entries(desired)) {
      if (stopped || dirty) return;
      const socket = socketOf[c];
      const wsId =
        socket === undefined ? undefined : registrations[socket]?.wsId;
      if (socket !== undefined && wsId === undefined) continue;
      const have = attached[c];
      if (have && configEqual(have, config)) continue;
      if (have !== undefined) await detach(c);
      await attach(c, config, wsId);
    }
  };

  return {
    run(inputs) {
      latest = inputs;
      if (inFlight) {
        dirty = true;
        return inFlight;
      }
      inFlight = (async () => {
        try {
          do {
            dirty = false;
            await pass(latest!);
          } while (dirty && !stopped);
        } finally {
          inFlight = null;
        }
      })();
      return inFlight;
    },
    stop() {
      stopped = true;
    },
  };
}
