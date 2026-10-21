import fs from "node:fs";
import path from "node:path";

export const WASM_DIR = path.resolve(import.meta.dir, "../../wasm");

export function findN6kWasmExtension(): string | null {
  if (!fs.existsSync(WASM_DIR)) return null;
  for (const version of fs.readdirSync(WASM_DIR)) {
    const ext = path.join(
      WASM_DIR,
      version,
      "wasm_threads",
      "n6k_client.duckdb_extension.wasm",
    );
    if (fs.existsSync(ext)) return ext;
  }
  return null;
}

export const WASM_AVAILABLE = findN6kWasmExtension() !== null;
