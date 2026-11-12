// Bundled into the extension and eval'd inside duckdb-wasm's module scope, where these are in scope.
import { installMainGlue } from "./main-glue";

declare const wasmMemory: WebAssembly.Memory;
declare function _free(ptr: number): void;

installMainGlue(wasmMemory, _free);
