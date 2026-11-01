// Dedicated iframe entry, kept off the root barrel so the iframe graph stays duckdb-wasm-free.

export { createIframeN6k } from "./n6k-iframe";
export type { IframeN6k, IframeN6kOptions } from "./n6k-iframe";
export { bindIframeHost } from "./host";
export type { IframeHostOptions, IframeHostTarget } from "./host";
export type { N6kMsg } from "./protocol";
