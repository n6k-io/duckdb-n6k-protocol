export type AttachOptions = Record<string, string>;

export type AttachConfig = { path: string; options: AttachOptions };

export const DEFAULT_OPTIONS: AttachOptions = { TYPE: "n6k" };

export function makeConfig(
  path: string,
  options?: AttachOptions,
): AttachConfig {
  return { path, options: { ...DEFAULT_OPTIONS, ...options } };
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
