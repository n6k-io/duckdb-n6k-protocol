import { renderAttachSql, type AttachConfig } from "./attach/attachment";
import { createCatalogStore, type CatalogStore } from "./attach/catalog-states";
import {
  socketsOf,
  specsToConfigs,
  type DatabaseSpec,
} from "./attach/database-spec";
import type { ConnectionLike } from "./connection-shape";
import { parseSqlTables, type TableRef } from "./metadata/parse-sql-tables";
import {
  describeN6kTable,
  type TableIdent,
  type TableMeta,
} from "./metadata/table-describe";
import { startSockets, type SocketSpec } from "./sockets/sockets";
import { omitKey } from "./util/omit-key";
import {
  createDuckDB,
  type CreateDuckDBOptions,
  type DuckDBInstance,
} from "./create-duckdb";
import type { RegisteredWebsocket } from "./create-n6k-worker";
import { logger as log, stopwatch } from "./logger";
import type { WsStatus } from "./types";
import { createWasmConnect, type WasmDatabase } from "./wasm-lease";

// maximumThreads above 1 can deadlock boot(): it LOADs n6k_client after open() (see CreateDuckDBOptions).
export type N6kDuckDBOptions = Omit<
  CreateDuckDBOptions,
  "onStatus" | "onCatalogsChanged" | "onInvalidated"
> & {
  sockets?: Record<string, SocketSpec>;
  databases?: Record<string, DatabaseSpec>;
};

export type N6kDuckDBStatus =
  | "initializing"
  | "loading-extensions"
  | "attaching"
  | "ready"
  | "error";

export type N6kDuckDBSnapshot = {
  status: N6kDuckDBStatus;
  error: string | null;
  connStatus: Record<string, WsStatus>;
  // Per catalog: server invalidations plus reconnects (pushes are lost while disconnected).
  invalidations: Record<string, number>;
  // Per alias: its ATTACH failure, or the failure of the socket it rides.
  errors: Record<string, string>;
  // Registered sockets by name; a name is present from its first registration until dispose().
  registrations: Record<string, RegisteredWebsocket>;
};

export type N6kMetadataFns = {
  tablesOf(conn: ConnectionLike, sql: string): Promise<TableRef[]>;
  tableMeta(conn: ConnectionLike, ref: TableIdent): Promise<TableMeta>;
  catalogs: CatalogStore;
};

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

// Sockets and databases are fixed at construction: connect() resolves once every database is ATTACHed (or failed).
export class N6kDuckDB {
  private snapshot: N6kDuckDBSnapshot = {
    status: "initializing",
    error: null,
    connStatus: {},
    invalidations: {},
    errors: {},
    registrations: {},
  };
  private readonly listeners = new Set<() => void>();
  private booted: Promise<DuckDBInstance> | null = null;
  private stopSockets: (() => void) | null = null;
  private readonly duckdbOptions: CreateDuckDBOptions;
  private readonly sockets: Record<string, SocketSpec>;
  private readonly configs: Record<string, AttachConfig>;
  private readonly socketOf: Record<string, string>;
  private attachErrors: Record<string, string> = {};
  private socketErrors: Record<string, string> = {};

  readonly metadataFns: N6kMetadataFns;

  // Boot is deferred to the first subscribe()/ready()/connect(), so constructing one at module scope is safe under SSR.
  constructor(opts: N6kDuckDBOptions = {}) {
    const { sockets = {}, databases = {}, ...duckdbOptions } = opts;
    this.duckdbOptions = duckdbOptions;
    this.sockets = sockets;
    this.socketOf = socketsOf(databases, sockets);
    this.configs = specsToConfigs(databases);
    this.metadataFns = {
      tablesOf: parseSqlTables,
      tableMeta: describeN6kTable,
      catalogs: createCatalogStore(
        (fn) => this.subscribe(fn),
        Object.keys(this.configs),
        () => this.snapshot,
      ),
    };
  }

  private start(): Promise<DuckDBInstance> {
    if (!this.booted) {
      this.booted = this.boot();
      // Failures surface through status/error and ready(); don't also raise an unhandled rejection.
      this.booted.catch(() => {});
    }
    return this.booted;
  }

  private async boot(): Promise<DuckDBInstance> {
    const sinceBoot = stopwatch();
    try {
      const instance = await createDuckDB({
        ...this.duckdbOptions,
        onStatus: (catalog, status) => this.onConnStatus(catalog, status),
        onInvalidated: (catalog) => this.bumpInvalidation(catalog),
      });
      this.update({ status: "loading-extensions" });
      const sinceLoad = stopwatch();
      await instance.conn.query("LOAD n6k_client;");
      log.debug(`#${instance.id} n6k extension loaded (${sinceLoad()})`);
      await this.openSockets(instance);
      this.update({ status: "attaching" });
      for (const [alias, config] of Object.entries(this.configs)) {
        await this.attach(instance, alias, config);
      }
      this.update({ status: "ready" });
      log.debug(`#${instance.id} ready (boot ${sinceBoot()})`);
      return instance;
    } catch (error) {
      log.error("N6kDuckDB boot failed:", error);
      this.update({ status: "error", error: messageOf(error) });
      throw error;
    }
  }

  // Resolves once every socket a database rides has registered or failed once.
  private openSockets(instance: DuckDBInstance): Promise<void> {
    const waiting = new Map<string, () => void>();
    const needed = new Set(Object.values(this.socketOf));
    const settled = [...needed].map(
      (name) => new Promise<void>((resolve) => waiting.set(name, resolve)),
    );
    const settle = (name: string) => {
      waiting.get(name)?.();
      waiting.delete(name);
    };
    this.stopSockets = startSockets(
      this.sockets,
      (socket, token) => instance.registerWebsocket(socket, token),
      (wsId, socket, token) => instance.replaceWebsocket(wsId, socket, token),
      {
        registered: (name, reg) => {
          this.socketErrors = omitKey(this.socketErrors, name);
          this.update({
            registrations: { ...this.snapshot.registrations, [name]: reg },
          });
          settle(name);
        },
        failed: (name, message) => {
          this.socketErrors = { ...this.socketErrors, [name]: message };
          this.update({});
          settle(name);
        },
        recovered: (name) => {
          this.socketErrors = omitKey(this.socketErrors, name);
          this.update({});
        },
        released: (name) => {
          this.socketErrors = omitKey(this.socketErrors, name);
          this.update({
            registrations: omitKey(this.snapshot.registrations, name),
          });
        },
      },
    );
    return Promise.all(settled).then(() => {});
  }

  private async attach(
    instance: DuckDBInstance,
    alias: string,
    config: AttachConfig,
  ): Promise<void> {
    const socket = this.socketOf[alias];
    let withWs = config;
    if (socket !== undefined) {
      const reg = this.snapshot.registrations[socket];
      // The socket's own failure is already this alias's error.
      if (!reg) return;
      withWs = {
        ...config,
        options: { ...config.options, connection: reg.wsId },
      };
    }
    const sinceAttach = stopwatch();
    try {
      await instance.conn.query(renderAttachSql(alias, withWs));
      log.debug(`attached ${alias} (${sinceAttach()})`);
    } catch (error) {
      log.error(`attach ${alias} failed:`, error);
      this.attachErrors = { ...this.attachErrors, [alias]: messageOf(error) };
      this.update({});
    }
  }

  private onConnStatus(catalog: string, status: WsStatus): void {
    const previous = this.snapshot.connStatus[catalog];
    this.update({
      connStatus: { ...this.snapshot.connStatus, [catalog]: status },
    });
    if (
      status === "connected" &&
      (previous === "disconnected" || previous === "reconnecting")
    ) {
      this.bumpInvalidation(catalog);
    }
  }

  private bumpInvalidation(catalog: string): void {
    const { invalidations } = this.snapshot;
    this.update({
      invalidations: {
        ...invalidations,
        [catalog]: (invalidations[catalog] ?? 0) + 1,
      },
    });
  }

  private update(patch: Partial<N6kDuckDBSnapshot>): void {
    this.snapshot = {
      ...this.snapshot,
      ...patch,
      errors: this.visibleErrors(),
    };
    for (const fn of this.listeners) fn();
  }

  private visibleErrors(): Record<string, string> {
    let out = this.attachErrors;
    for (const [alias, name] of Object.entries(this.socketOf)) {
      const message = this.socketErrors[name];
      if (message !== undefined) out = { ...out, [alias]: message };
    }
    return out;
  }

  subscribe = (fn: () => void): (() => void) => {
    void this.start();
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  };

  getSnapshot = (): N6kDuckDBSnapshot => this.snapshot;

  async ready(): Promise<void> {
    await this.start();
  }

  connect = async (): Promise<ConnectionLike> => {
    const instance = await this.start();
    return createWasmConnect(instance.db as unknown as WasmDatabase)();
  };

  async dispose(): Promise<void> {
    const instance = this.booted ? await this.booted.catch(() => null) : null;
    this.stopSockets?.();
    await instance?.dispose();
    log.debug(`#${instance?.id ?? "?"} disposed`);
  }
}
