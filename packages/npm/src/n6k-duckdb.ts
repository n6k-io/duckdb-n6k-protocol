import {
  configEqual,
  type AttachConfig,
  type AttachError,
} from "./attach/attachment";
import { createCatalogStore, type CatalogStore } from "./attach/catalog-states";
import {
  socketsOf,
  specsToConfigs,
  type DatabaseSpec,
} from "./attach/database-spec";
import {
  createAttachReconciler,
  type AttachReconciler,
} from "./attach/reconciler";
import type { ConnectionLike } from "./connection-shape";
import { parseSqlTables, type TableRef } from "./metadata/parse-sql-tables";
import {
  describeN6kTable,
  type TableIdent,
  type TableMeta,
} from "./metadata/table-describe";
import {
  createSocketSet,
  validateSockets,
  type SocketSet,
  type SocketSpec,
} from "./sockets/sockets";
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

export type N6kDuckDBOptions = Omit<
  CreateDuckDBOptions,
  "onStatus" | "onCatalogsChanged" | "onInvalidated"
>;

export type N6kDuckDBStatus =
  | "initializing"
  | "loading-extensions"
  | "ready"
  | "error";

export type N6kDuckDBSnapshot = {
  status: N6kDuckDBStatus;
  error: string | null;
  connStatus: Record<string, WsStatus>;
  // Per catalog, how many times the server invalidated it.
  invalidations: Record<string, number>;
  desired: Record<string, AttachConfig>;
  attached: Record<string, AttachConfig>;
  // Attach failures, plus the failure of the socket an alias rides.
  errors: Record<string, AttachError>;
  // Registered sockets by name; a name is absent until it registers.
  registrations: Record<string, RegisteredWebsocket>;
};

export type N6kMetadataFns = {
  tablesOf(conn: ConnectionLike, sql: string): Promise<TableRef[]>;
  tableMeta(conn: ConnectionLike, ref: TableIdent): Promise<TableMeta>;
  catalogs: CatalogStore;
};

type InvalidatedListener = (catalog: string, schemas: string[]) => void;
type CatalogsChangedListener = (wsId: string, catalogs: string[]) => void;

export class N6kDuckDB {
  private snapshot: N6kDuckDBSnapshot = {
    status: "initializing",
    error: null,
    connStatus: {},
    invalidations: {},
    desired: {},
    attached: {},
    errors: {},
    registrations: {},
  };
  private readonly listeners = new Set<() => void>();
  private readonly invalidatedListeners = new Set<InvalidatedListener>();
  private readonly catalogsChangedListeners =
    new Set<CatalogsChangedListener>();
  private instance: DuckDBInstance | null = null;
  private booted: Promise<DuckDBInstance> | null = null;
  private sockets: Record<string, SocketSpec> = {};
  private socketSet: SocketSet | null = null;
  private socketErrors: Record<string, string> = {};
  private socketOf: Record<string, string> = {};
  private attachErrors: Record<string, AttachError> = {};
  // From setDatabases; setDesired/removeDesired own the remaining keys.
  private specAliases = new Set<string>();
  // Kept out of the snapshot: it can carry a secret token.
  private readonly setup: Record<string, string[]> = {};
  private reconciler: AttachReconciler | null = null;

  readonly metadataFns: N6kMetadataFns = {
    tablesOf: parseSqlTables,
    tableMeta: describeN6kTable,
    catalogs: createCatalogStore(
      (fn) => this.subscribe(fn),
      () => this.snapshot,
    ),
  };

  // Boot is deferred to the first subscribe()/ready()/connect(), so constructing one at module scope is safe under SSR.
  constructor(private readonly opts: N6kDuckDBOptions = {}) {}

  private start(): Promise<DuckDBInstance> {
    if (!this.booted) {
      this.booted = this.boot(this.opts);
      // Failures surface through status/error and ready(); don't also raise an unhandled rejection.
      this.booted.catch(() => {});
    }
    return this.booted;
  }

  private async boot(opts: N6kDuckDBOptions): Promise<DuckDBInstance> {
    const sinceBoot = stopwatch();
    try {
      const instance = await createDuckDB({
        ...opts,
        onStatus: (catalog, status) =>
          this.update({
            connStatus: { ...this.snapshot.connStatus, [catalog]: status },
          }),
        onInvalidated: (catalog, schemas) => {
          this.update({
            invalidations: {
              ...this.snapshot.invalidations,
              [catalog]: (this.snapshot.invalidations[catalog] ?? 0) + 1,
            },
          });
          for (const fn of this.invalidatedListeners) fn(catalog, schemas);
        },
        onCatalogsChanged: (wsId, catalogs) => {
          for (const fn of this.catalogsChangedListeners) fn(wsId, catalogs);
        },
      });
      this.instance = instance;
      this.update({ status: "loading-extensions" });
      const sinceLoad = stopwatch();
      await instance.conn.query("LOAD n6k_client;");
      log.debug(`#${instance.id} n6k extension loaded (${sinceLoad()})`);
      this.reconciler = createAttachReconciler(await instance.db.connect(), {
        setup: (alias) => this.setup[alias] ?? [],
        onAttached: (alias, config) => {
          this.attachErrors = omitKey(this.attachErrors, alias);
          this.update({
            attached: { ...this.snapshot.attached, [alias]: config },
          });
        },
        onDetached: (alias) =>
          this.update({
            attached: omitKey(this.snapshot.attached, alias),
            connStatus: omitKey(this.snapshot.connStatus, alias),
          }),
        onError: (alias, config, message) => {
          this.attachErrors = {
            ...this.attachErrors,
            [alias]: { config, message },
          };
          this.update({});
        },
      });
      this.socketSet = createSocketSet(
        this.registerWebsocket,
        this.replaceWebsocket,
        {
          registered: (name, reg) => {
            this.socketErrors = omitKey(this.socketErrors, name);
            this.update({
              registrations: { ...this.snapshot.registrations, [name]: reg },
            });
          },
          failed: (name, message) => {
            this.socketErrors = { ...this.socketErrors, [name]: message };
            this.update({});
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
      this.update({ status: "ready" });
      this.socketSet.apply(this.sockets);
      this.reconcile();
      log.debug(`#${instance.id} ready (boot ${sinceBoot()})`);
      return instance;
    } catch (error) {
      log.error("N6kDuckDB boot failed:", error);
      this.update({
        status: "error",
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  private update(patch: Partial<N6kDuckDBSnapshot>): void {
    const next = { ...this.snapshot, ...patch };
    next.errors = this.visibleErrors(next.desired);
    const reconcile =
      next.desired !== this.snapshot.desired ||
      next.registrations !== this.snapshot.registrations;
    this.snapshot = next;
    for (const fn of this.listeners) fn();
    if (reconcile) this.reconcile();
  }

  private visibleErrors(
    desired: Record<string, AttachConfig>,
  ): Record<string, AttachError> {
    let out = this.attachErrors;
    for (const [alias, name] of Object.entries(this.socketOf)) {
      const message = this.socketErrors[name];
      const config = desired[alias];
      if (message === undefined || config === undefined) continue;
      out = { ...out, [alias]: { config, message } };
    }
    return out;
  }

  private reconcile(): void {
    const { desired, registrations } = this.snapshot;
    void this.reconciler?.run({
      desired,
      socketOf: this.socketOf,
      registrations,
    });
  }

  // Replaces the previous set; see SocketSet.apply for which sockets keep their registration.
  setSockets(sockets: Record<string, SocketSpec>): void {
    validateSockets(sockets);
    this.sockets = sockets;
    this.socketSet?.apply(sockets);
  }

  // Replaces the aliases from the previous call; aliases set through setDesired are untouched.
  setDatabases(
    databases: Record<string, DatabaseSpec>,
    sockets: Record<string, SocketSpec> = this.sockets,
  ): void {
    this.socketOf = socketsOf(databases, sockets);
    const configs = specsToConfigs(databases);
    let desired = this.snapshot.desired;
    for (const alias of this.specAliases) {
      if (!(alias in configs)) desired = omitKey(desired, alias);
    }
    for (const [alias, config] of Object.entries(configs)) {
      const existing = desired[alias];
      if (!existing || !configEqual(existing, config)) {
        desired = { ...desired, [alias]: config };
      }
    }
    this.specAliases = new Set(Object.keys(configs));
    this.update({ desired });
  }

  // `setup` runs before this catalog's next ATTACH (reconciler `attach`); a setup-only change does not re-attach.
  setDesired = (
    catalog: string,
    config: AttachConfig,
    setup?: string[],
  ): void => {
    if (setup && setup.length > 0) {
      this.setup[catalog] = setup;
    } else {
      delete this.setup[catalog];
    }
    const existing = this.snapshot.desired[catalog];
    if (existing && configEqual(existing, config)) return;
    this.update({ desired: { ...this.snapshot.desired, [catalog]: config } });
  };

  removeDesired = (catalog: string): void => {
    delete this.setup[catalog];
    if (!(catalog in this.snapshot.desired)) return;
    this.update({ desired: omitKey(this.snapshot.desired, catalog) });
  };

  subscribe = (fn: () => void): (() => void) => {
    void this.start();
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  };

  getSnapshot = (): N6kDuckDBSnapshot => this.snapshot;

  async ready(): Promise<void> {
    await this.start();
  }

  onInvalidated(fn: InvalidatedListener): () => void {
    this.invalidatedListeners.add(fn);
    return () => this.invalidatedListeners.delete(fn);
  }

  onCatalogsChanged(fn: CatalogsChangedListener): () => void {
    this.catalogsChangedListeners.add(fn);
    return () => this.catalogsChangedListeners.delete(fn);
  }

  private get live(): DuckDBInstance {
    if (!this.instance || this.snapshot.status !== "ready") {
      throw new Error(
        "n6k: N6kDuckDB is not ready. Gate on its status === 'ready'.",
      );
    }
    return this.instance;
  }

  connect = async (): Promise<ConnectionLike> => {
    const instance = await this.start();
    return createWasmConnect(instance.db as unknown as WasmDatabase)();
  };

  reconnect = (catalog: string): void => this.live.reconnect(catalog);

  registerWebsocket = (
    socket: WebSocket,
    token: string,
  ): Promise<RegisteredWebsocket> => this.live.registerWebsocket(socket, token);

  replaceWebsocket = (
    wsId: string,
    socket: WebSocket,
    token: string,
  ): Promise<void> => this.live.replaceWebsocket(wsId, socket, token);

  listCatalogs = (wsId: string): Promise<string[]> =>
    this.live.listCatalogs(wsId);

  async dispose(): Promise<void> {
    const instance = this.booted ? await this.booted.catch(() => null) : null;
    this.socketSet?.dispose();
    this.reconciler?.stop();
    await this.reconciler?.settled();
    await instance?.dispose();
    log.debug(`#${instance?.id ?? "?"} disposed`);
  }
}
