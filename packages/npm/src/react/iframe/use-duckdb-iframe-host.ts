// Parent-side hook: answers iframe query RPCs from the live connection and pushes attach state.

import { useEffect, useRef, type RefObject } from "react";
import { useDuckDB } from "../use-duckdb";
import { createHostCore, type HostCore } from "./iframe-host-core";
import { makeRunQueryIPC } from "./host-lease";
import type { Mirror, N6kHandshake } from "./protocol";

export type UseDuckDBIframeHostOptions = {
  // Origins permitted to connect (second gate; the handshake also binds the iframe element). Never "*".
  allowedOrigins: string[];
};

export function useDuckDBIframeHost(
  iframeRef: RefObject<HTMLIFrameElement | null>,
  options: UseDuckDBIframeHostOptions,
): void {
  const ctx = useDuckDB();
  // Always-fresh refs so the one-time listener reads latest ctx/options at message time.
  const ctxRef = useRef(ctx);
  const optsRef = useRef(options);
  useEffect(() => {
    ctxRef.current = ctx;
    optsRef.current = options;
  });

  const coreRef = useRef<HostCore | null>(null);
  const boundClientIdRef = useRef<string | null>(null);

  // Mount the listener once; read allowedOrigins from optsRef so re-renders can't tear the port down.
  useEffect(() => {
    function onHello(e: MessageEvent): void {
      const data = e.data as N6kHandshake | undefined;
      if (!data || data.__n6k !== "hello") return;

      const el = iframeRef.current;
      const source = e.source as Window | null;
      if (!el || source === null || source !== el.contentWindow) return;
      if (!optsRef.current.allowedOrigins.includes(e.origin)) return;

      // Duplicate hello from same mount → re-snapshot; keyed on clientId, not window identity.
      if (coreRef.current && boundClientIdRef.current === data.clientId) {
        coreRef.current.pushState(mirrorOf(ctxRef.current));
        return;
      }

      // New/reloaded frame → dispose stale core, re-handshake with a fresh channel.
      coreRef.current?.dispose();
      const channel = new MessageChannel();
      const core = createHostCore({
        runQueryIPC: makeRunQueryIPC(() => ctxRef.current.connect()),
        onSetDesired: (catalog, config) =>
          ctxRef.current.setDesired(catalog, config),
        onRemoveDesired: (catalog) => ctxRef.current.removeDesired(catalog),
        onReconnect: (catalog) => ctxRef.current.reconnect(catalog),
      });
      core.bind(channel.port1);
      coreRef.current = core;
      boundClientIdRef.current = data.clientId;
      // Opaque-origin ("null") can't target; "*" is safe — frame authenticated, port unforgeable.
      const targetOrigin = e.origin === "null" ? "*" : e.origin;
      source.postMessage({ __n6k: "port" }, targetOrigin, [channel.port2]);
      core.pushState(mirrorOf(ctxRef.current));
    }

    window.addEventListener("message", onHello);
    return () => {
      window.removeEventListener("message", onHello);
      coreRef.current?.dispose();
      coreRef.current = null;
      boundClientIdRef.current = null;
    };
  }, [iframeRef]);

  // Re-push on any provider state change; pushing is idempotent.
  useEffect(() => {
    coreRef.current?.pushState({
      status: ctx.status,
      error: ctx.error,
      connStatus: ctx.connStatus,
      invalidations: ctx.invalidations,
      desired: ctx.desired,
      attached: ctx.attached,
      errors: ctx.errors,
    });
  }, [
    ctx.status,
    ctx.error,
    ctx.connStatus,
    ctx.invalidations,
    ctx.desired,
    ctx.attached,
    ctx.errors,
  ]);
}

function mirrorOf(ctx: ReturnType<typeof useDuckDB>): Mirror {
  return {
    status: ctx.status,
    error: ctx.error,
    connStatus: ctx.connStatus,
    invalidations: ctx.invalidations,
    desired: ctx.desired,
    attached: ctx.attached,
    errors: ctx.errors,
  };
}
