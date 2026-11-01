import {
  makeConfig,
  type AttachConfig,
  type AttachOptions,
} from "./attachment";
import type { SocketSpec } from "../sockets/sockets";

export type DatabaseSpec =
  | string
  | { path: string; options?: AttachOptions }
  | { socket: string; catalog?: string; options?: AttachOptions };

// Socket-backed entries carry no wsId here; the reconciler adds it at ATTACH time.
export function specToConfig(spec: DatabaseSpec): AttachConfig {
  if (typeof spec === "string") return makeConfig(spec);
  if ("socket" in spec) {
    return makeConfig("", {
      ...spec.options,
      ...(spec.catalog === undefined ? {} : { catalog: spec.catalog }),
    });
  }
  return makeConfig(spec.path, spec.options);
}

export function socketsOf(
  databases: Record<string, DatabaseSpec> | undefined,
  sockets: Record<string, SocketSpec> | undefined,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [alias, spec] of Object.entries(databases ?? {})) {
    if (typeof spec === "string" || !("socket" in spec)) continue;
    if (!sockets || !(spec.socket in sockets)) {
      throw new Error(
        `n6k: databases.${alias} references unknown socket '${spec.socket}'`,
      );
    }
    const keys = new Set(
      Object.keys(spec.options ?? {}).map((k) => k.toLowerCase()),
    );
    if (keys.has("token") || keys.has("wsid")) {
      throw new Error(
        `n6k: databases.${alias} rides socket '${spec.socket}'; set the token on the socket, not 'token'/'wsId' options`,
      );
    }
    out[alias] = spec.socket;
  }
  return out;
}

export function specsToConfigs(
  specs: Record<string, DatabaseSpec> | undefined,
): Record<string, AttachConfig> {
  if (!specs) return {};
  return Object.fromEntries(
    Object.entries(specs).map(([k, v]) => [k, specToConfig(v)]),
  );
}
