import { useSyncExternalStore } from "react";

export type Subscribable<S> = {
  subscribe: (onChange: () => void) => () => void;
  getSnapshot: () => S;
};

export function useN6kSnapshot<S>(store: Subscribable<S>): S {
  return useSyncExternalStore(
    store.subscribe,
    store.getSnapshot,
    store.getSnapshot,
  );
}
