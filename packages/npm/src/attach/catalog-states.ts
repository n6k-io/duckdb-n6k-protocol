// `version` moves on a server invalidation or a reconnect, so reads keyed on it refetch.
export type CatalogState = { version: string; error?: string };

export type CatalogStore = {
  subscribe(onChange: () => void): () => void;
  getSnapshot(): Readonly<Record<string, CatalogState>>;
};

export type CatalogInputs = {
  invalidations: Record<string, number>;
  errors: Record<string, string>;
};

export function catalogStatesOf(
  aliases: readonly string[],
  s: CatalogInputs,
): Record<string, CatalogState> {
  const out: Record<string, CatalogState> = {};
  for (const c of aliases) {
    const version = String(s.invalidations[c] ?? 0);
    const error = s.errors[c];
    out[c] = error === undefined ? { version } : { version, error };
  }
  return out;
}

// Memoized on input identity so getSnapshot is stable between changes, as useSyncExternalStore requires.
export function createCatalogStore(
  subscribe: (fn: () => void) => () => void,
  aliases: readonly string[],
  inputs: () => CatalogInputs,
): CatalogStore {
  let last: CatalogInputs | null = null;
  let states: Record<string, CatalogState> = {};
  return {
    subscribe,
    getSnapshot() {
      const now = inputs();
      if (now !== last) {
        last = now;
        states = catalogStatesOf(aliases, now);
      }
      return states;
    },
  };
}
