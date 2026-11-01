import { useEffect, type RefObject } from "react";
import {
  bindIframeHost,
  type IframeHostOptions,
  type IframeHostTarget,
} from "../iframe/host";

export function useDuckDBIframeHost(
  iframeRef: RefObject<HTMLIFrameElement | null>,
  duckdb: IframeHostTarget,
  { allowedOrigins }: IframeHostOptions,
): void {
  const origins = allowedOrigins.join("\n");
  useEffect(
    () =>
      bindIframeHost(() => iframeRef.current, duckdb, {
        allowedOrigins: origins.split("\n"),
      }),
    [iframeRef, duckdb, origins],
  );
}
