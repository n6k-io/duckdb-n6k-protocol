import fs from "node:fs";
import path from "node:path";
import { createNativeDuckDB } from "../../native/create-native-duckdb";
import { toWasmShape } from "../../native-wasm-adapter/adapter";
import { N6K_URL } from "../_server-gate";
import type { Backend, BackendHandle } from "./types";

const N6K_EXT = path.resolve(
  import.meta.dir,
  "../../../../../build/release/extension/n6k_client/n6k_client.duckdb_extension",
);

const AVAILABLE = fs.existsSync(N6K_EXT);

const caps = {
  reconnect: false,
  status: false,
  forceDrop: false,
  protocolCounts: false,
};

export const nativeBackend: Backend = {
  name: "native",
  caps,
  available: () => AVAILABLE,
  unavailableReason: () =>
    `native n6k extension not found at ${N6K_EXT} — run \`make release\` to build it.`,
  async setup(): Promise<BackendHandle> {
    const { conn, dispose } = await createNativeDuckDB({
      n6kExtensionPath: N6K_EXT,
      databases: { db: N6K_URL },
    });
    return {
      name: "native",
      caps,
      conn: toWasmShape(conn),
      reconnect() {
        throw new Error("native backend does not support reconnect");
      },
      cleanup: async () => dispose(),
    };
  },
};
