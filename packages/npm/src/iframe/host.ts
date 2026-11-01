// Parent side: answers iframe query RPCs from leased connections and pushes attach state.

import type { N6kDuckDB, N6kDuckDBSnapshot } from "../n6k-duckdb";
import { createHostCore, type HostCore } from "./iframe-host-core";
import { makeRunQueryIPC } from "./host-lease";
import type { Mirror, N6kHandshake } from "./protocol";

export type IframeHostTarget = Pick<
  N6kDuckDB,
  | "subscribe"
  | "getSnapshot"
  | "connect"
  | "setDesired"
  | "removeDesired"
  | "reconnect"
>;

export type IframeHostOptions = {
  // Exact origins permitted to connect (second gate; the handshake also binds the iframe element).
  // "*" is matched literally, so it admits no iframe.
  allowedOrigins: string[];
};

function mirrorOf(s: N6kDuckDBSnapshot): Mirror {
  return {
    status: s.status,
    error: s.error,
    connStatus: s.connStatus,
    invalidations: s.invalidations,
    desired: s.desired,
    attached: s.attached,
    errors: s.errors,
  };
}

export function bindIframeHost(
  iframe: () => HTMLIFrameElement | null,
  duckdb: IframeHostTarget,
  options: IframeHostOptions,
): () => void {
  let core: HostCore | null = null;
  let boundClientId: string | null = null;

  const push = () => core?.pushState(mirrorOf(duckdb.getSnapshot()));

  function onHello(e: MessageEvent): void {
    const data = e.data as N6kHandshake | undefined;
    if (!data || data.__n6k !== "hello") return;

    const el = iframe();
    const source = e.source as Window | null;
    if (!el || source === null || source !== el.contentWindow) return;
    if (!options.allowedOrigins.includes(e.origin)) return;

    // Duplicate hello from same mount → re-snapshot; keyed on clientId, not window identity.
    if (core && boundClientId === data.clientId) {
      push();
      return;
    }

    // New/reloaded frame → dispose stale core, re-handshake with a fresh channel.
    core?.dispose();
    const channel = new MessageChannel();
    core = createHostCore({
      runQueryIPC: makeRunQueryIPC(duckdb.connect),
      onSetDesired: duckdb.setDesired,
      onRemoveDesired: duckdb.removeDesired,
      onReconnect: duckdb.reconnect,
    });
    core.bind(channel.port1);
    boundClientId = data.clientId;
    // Opaque-origin ("null") can't target; "*" is safe — frame authenticated, port unforgeable.
    const targetOrigin = e.origin === "null" ? "*" : e.origin;
    source.postMessage({ __n6k: "port" }, targetOrigin, [channel.port2]);
    push();
  }

  window.addEventListener("message", onHello);
  const unsubscribe = duckdb.subscribe(push);
  return () => {
    unsubscribe();
    window.removeEventListener("message", onHello);
    core?.dispose();
    core = null;
    boundClientId = null;
  };
}
