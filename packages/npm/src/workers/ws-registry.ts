// Per-catalog resource registry: each catalog owns an isolated resource so concurrent catalogs never share.

export interface CatalogRegistry<T> {
  ensure(catalog: string): T;
  set(catalog: string, value: T): void;
  get(catalog: string): T | undefined;
  has(catalog: string): boolean;
  remove(catalog: string): boolean;
  clear(): void;
  size(): number;
  catalogs(): string[];
}

export function createCatalogRegistry<T>(
  make: ((catalog: string) => T) | undefined = undefined,
  dispose: (value: T, catalog: string) => void = () => {},
): CatalogRegistry<T> {
  const items = new Map<string, T>();
  return {
    ensure(catalog: string): T {
      if (items.has(catalog)) return items.get(catalog) as T;
      if (!make) throw new Error("CatalogRegistry.ensure: no make() provided");
      const created = make(catalog);
      items.set(catalog, created);
      return created;
    },
    set(catalog: string, value: T): void {
      if (items.has(catalog)) dispose(items.get(catalog) as T, catalog);
      items.set(catalog, value);
    },
    get(catalog: string): T | undefined {
      return items.get(catalog);
    },
    has(catalog: string): boolean {
      return items.has(catalog);
    },
    remove(catalog: string): boolean {
      if (!items.has(catalog)) return false;
      const value = items.get(catalog) as T;
      items.delete(catalog);
      dispose(value, catalog);
      return true;
    },
    clear(): void {
      for (const [catalog, value] of items) dispose(value, catalog);
      items.clear();
    },
    size(): number {
      return items.size;
    },
    catalogs(): string[] {
      return [...items.keys()];
    },
  };
}
