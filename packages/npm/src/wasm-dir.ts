import { fileURLToPath } from "node:url";
import path from "node:path";

export const wasmDir = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
  "wasm",
);
