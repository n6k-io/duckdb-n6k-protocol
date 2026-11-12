// Bundled into the extension and eval'd on its pump pthread, inside duckdb-wasm's module scope.
import { installPumpGlue } from "./pump-glue";

declare const wasmMemory: WebAssembly.Memory;
declare function _free(ptr: number): void;

installPumpGlue(wasmMemory, _free);
