export type AttachStatus = "pending" | "ready" | "error";

export type AttachOptions = Record<string, string>;

export type AttachConfig = { path: string; options: AttachOptions };

export type AttachError = { config: AttachConfig; message: string };

export type AttachState = {
  desired: Record<string, AttachConfig>;
  attached: Record<string, AttachConfig>;
  errors: Record<string, AttachError>;
};

export const DEFAULT_OPTIONS: AttachOptions = { TYPE: "n6k" };

export function makeConfig(
  path: string,
  options?: AttachOptions,
): AttachConfig {
  return { path, options: { ...DEFAULT_OPTIONS, ...options } };
}

export function configFingerprint(c: AttachConfig): string {
  const keys = Object.keys(c.options).toSorted();
  const opts = keys.map((k) => `${k}=${c.options[k]}`).join("|");
  return `${c.path}::${opts}`;
}

export function configEqual(a: AttachConfig, b: AttachConfig): boolean {
  return configFingerprint(a) === configFingerprint(b);
}

function sqlEscape(v: string): string {
  return v.replaceAll("'", "''");
}

export function renderAttachSql(catalog: string, c: AttachConfig): string {
  const parts = Object.entries(c.options).map(([k, v]) =>
    k.toUpperCase() === "TYPE" ? `TYPE ${v}` : `${k} '${sqlEscape(v)}'`,
  );
  return `ATTACH '${sqlEscape(c.path)}' AS ${catalog} (${parts.join(", ")});`;
}

export function statusOf(
  s: AttachState,
  catalog: string,
): AttachStatus | undefined {
  const want = s.desired[catalog];
  const have = s.attached[catalog];
  const err = s.errors[catalog];
  if (err && want && configEqual(err.config, want)) return "error";
  if (want === undefined && have === undefined) return undefined;
  if (want && have && configEqual(want, have)) return "ready";
  return "pending";
}

export function errorOf(s: AttachState, catalog: string): string | undefined {
  const err = s.errors[catalog];
  const want = s.desired[catalog];
  if (!err || !want) return undefined;
  if (!configEqual(err.config, want)) return undefined;
  return err.message;
}
