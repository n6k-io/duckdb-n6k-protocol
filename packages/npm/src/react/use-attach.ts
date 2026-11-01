import { useCallback, useEffect } from "react";
import {
  configFingerprint,
  makeConfig,
  type AttachConfig,
  type AttachOptions,
} from "../attach/attachment";
import type { WsStatus } from "../types";
import { useN6kSnapshot, type Subscribable } from "./use-n6k-snapshot";

export type AttachTarget = Subscribable<{
  connStatus: Record<string, WsStatus>;
}> & {
  setDesired: (catalog: string, config: AttachConfig) => void;
  removeDesired: (catalog: string) => void;
  reconnect: (catalog: string) => void;
};

export type Attach = {
  status: WsStatus | undefined;
  reconnect: () => void;
};

export function useAttach(
  duckdb: AttachTarget,
  catalog: string,
  path: string,
  options?: AttachOptions,
): Attach {
  const { connStatus } = useN6kSnapshot(duckdb);
  const config = makeConfig(path, options);
  const fingerprint = configFingerprint(config);
  useEffect(() => {
    duckdb.setDesired(catalog, config);
    return () => duckdb.removeDesired(catalog);
    // `config` is recreated each render; `fingerprint` is its stable identity — depending on `config` would oscillate desired state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [duckdb, catalog, fingerprint]);

  return {
    status: connStatus[catalog],
    reconnect: useCallback(() => duckdb.reconnect(catalog), [duckdb, catalog]),
  };
}
