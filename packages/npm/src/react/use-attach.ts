import { useCallback, useEffect } from "react";
import { useDuckDB } from "./use-duckdb";
import {
  configFingerprint,
  makeConfig,
  type AttachOptions,
} from "./attachment";
import type { WsStatus } from "../types";

export type Attach = {
  status: WsStatus | undefined;
  reconnect: () => void;
};

export function useAttach(
  catalog: string,
  path: string,
  options?: AttachOptions,
): Attach {
  const { setDesired, removeDesired, connStatus, reconnect } = useDuckDB();
  const config = makeConfig(path, options);
  const fingerprint = configFingerprint(config);
  useEffect(() => {
    setDesired(catalog, config);
    return () => removeDesired(catalog);
    // `config` is recreated each render; `fingerprint` is its stable identity — depending on `config` would oscillate desired state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [catalog, fingerprint, setDesired, removeDesired]);

  return {
    status: connStatus[catalog],
    reconnect: useCallback(() => reconnect(catalog), [reconnect, catalog]),
  };
}
