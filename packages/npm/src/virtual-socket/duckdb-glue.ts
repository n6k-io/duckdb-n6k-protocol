// duckdb-worker half of the vsock: tracks each catalog's duckdb-side endpoint so detach can close it.

import { duckdbEndpoint, type ChannelLayout, type Endpoint } from "./channel";

export type VsockRegistry = {
  // Register the duckdb-side endpoint for a catalog's channel (on attach). `buf` is duckdb's
  // shared wasm heap; `baseOffset` locates the channel within it (the C++ malloc pointer).
  register(
    catalog: string,
    buf: ArrayBufferLike,
    layout?: ChannelLayout,
    baseOffset?: number,
  ): void;
  // Drop and close a catalog's channel (on detach).
  unregister(catalog: string): void;
  // n6k.vsockClose: close the channel (from the reactor's Stop).
  close(catalog: string): void;
};

export function createVsockRegistry(): VsockRegistry {
  const channels = new Map<string, Endpoint>();

  return {
    register(catalog, buf, layout, baseOffset = 0) {
      channels.set(catalog, duckdbEndpoint(buf, layout, baseOffset));
    },

    unregister(catalog) {
      const ep = channels.get(catalog);
      if (ep) ep.close();
      channels.delete(catalog);
    },

    close(catalog) {
      const ep = channels.get(catalog);
      if (ep) ep.close();
    },
  };
}
