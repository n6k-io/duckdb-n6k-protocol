import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DuckDBInstance } from "@duckdb/node-api";
import type { DuckDBConnection } from "@duckdb/node-api";
import {
  makeConfig,
  renderAttachSql,
  type AttachOptions,
} from "../react/attachment";
import { logger as log } from "../logger";

export type NativeDatabaseSpec =
  | string
  | { path: string; options?: AttachOptions };

export type CreateNativeDuckDBOptions = {
  path?: string;
  n6kExtensionPath?: string;
  databases?: Record<string, NativeDatabaseSpec>;
  duckdbConfig?: Record<string, string>;
};

export type CreatedNativeDuckDB = {
  conn: DuckDBConnection;
  dispose: () => void;
};

function specToConfig(spec: NativeDatabaseSpec) {
  if (typeof spec === "string") return makeConfig(spec);
  return makeConfig(spec.path, spec.options);
}

const undisposed = new FinalizationRegistry<string>((label) => {
  log.warn(
    `[n6k] native DuckDB conn ${label} was garbage-collected without dispose() — likely a leak`,
  );
});

let nextId = 0;

export async function createNativeDuckDB(
  opts: CreateNativeDuckDBOptions = {},
): Promise<CreatedNativeDuckDB> {
  const config: Record<string, string> = {
    allow_unsigned_extensions: "true",
    ...opts.duckdbConfig,
  };
  // Isolate secrets per-instance: the machine-global ~/.duckdb store can inject a stray secret into ATTACH.
  let secretDir: string | undefined;
  if (!("secret_directory" in config)) {
    secretDir = fs.mkdtempSync(path.join(os.tmpdir(), "n6k-secrets-"));
    config.secret_directory = secretDir;
  }
  const instance = await DuckDBInstance.create(opts.path, config);
  const conn = await instance.connect();

  // httpfs supplies POST/PUT/DELETE (HTTPUtil's base only does GET); load before n6k so catalog ops reach the server.
  await conn.run("INSTALL httpfs");
  await conn.run("LOAD httpfs");

  if (opts.n6kExtensionPath) {
    await conn.run(`LOAD '${opts.n6kExtensionPath.replaceAll("'", "''")}'`);
  }

  if (opts.databases) {
    for (const [catalog, spec] of Object.entries(opts.databases)) {
      const sql = renderAttachSql(catalog, specToConfig(spec));
      await conn.run(sql);
    }
  }

  const label = `#${++nextId}`;
  const token = { label };
  undisposed.register(token, label, token);

  let disposed = false;
  return {
    conn,
    dispose() {
      if (disposed) return;
      disposed = true;
      undisposed.unregister(token);
      conn.disconnectSync();
      instance.closeSync();
      if (secretDir) {
        try {
          fs.rmSync(secretDir, { recursive: true, force: true });
        } catch {
          /* best-effort cleanup of the per-instance secret dir */
        }
      }
    },
  };
}
