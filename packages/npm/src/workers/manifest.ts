import manifest from "./manifest.json" with { type: "json" };

// Single source of truth for the n6k worker set; everyone else derives from this file.

export type N6kWorkerExport = "duckdbWorkerUrl" | "wsWorkerUrl";

export interface N6kWorkerEntry {
  readonly exportName: N6kWorkerExport;
  readonly file: string;
  readonly src: string;
}

export const N6K_WORKERS_URL_PREFIX: string = manifest.urlPrefix;
export const N6K_WORKER_ENTRIES: readonly N6kWorkerEntry[] =
  manifest.workers as readonly N6kWorkerEntry[];

export function n6kWorkerUrl(file: string): string {
  return `${N6K_WORKERS_URL_PREFIX}/${file}`;
}

export const N6K_DEFAULT_WORKER_URLS: Record<N6kWorkerExport, string> =
  Object.fromEntries(
    N6K_WORKER_ENTRIES.map((e) => [e.exportName, n6kWorkerUrl(e.file)]),
  ) as Record<N6kWorkerExport, string>;
