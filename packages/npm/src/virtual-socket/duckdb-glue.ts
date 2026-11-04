// duckdb-worker half of the vsock: tracks each transport's duckdb-side endpoint so close can close it.

import { duckdbEndpoint, type ChannelLayout, type Endpoint } from "./channel";

export type VsockRegistry = {
  // Register the duckdb-side endpoint for a transport's channel. `buf` is duckdb's
  // shared wasm heap; `baseOffset` locates the channel within it (the C++ malloc pointer).
  register(
    key: string,
    buf: ArrayBufferLike,
    layout?: ChannelLayout,
    baseOffset?: number,
  ): void;
  // Close and drop a transport's channel. Before its ring is freed: closing writes to the ring.
  unregister(key: string): void;
  // n6k.vsockClose: close the channel (from the transport's Stop).
  close(key: string): void;
};

export function createVsockRegistry(): VsockRegistry {
  const channels = new Map<string, Endpoint>();

  return {
    register(key, buf, layout, baseOffset = 0) {
      channels.set(key, duckdbEndpoint(buf, layout, baseOffset));
    },

    unregister(key) {
      const ep = channels.get(key);
      if (ep) ep.close();
      channels.delete(key);
    },

    close(key) {
      const ep = channels.get(key);
      if (ep) ep.close();
    },
  };
}
