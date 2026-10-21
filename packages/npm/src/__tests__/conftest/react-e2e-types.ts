export type ReactProbe = {
  status: string;
  rows: Array<Record<string, unknown>>;
  error: string | null;
};

declare global {
  // Renders DuckDBProvider (attaching `catalog` from the test server) around a useQuery of `sql`.
  var __n6kMountReact: ((catalog: string, sql: string) => void) | undefined;
  var __n6kReactProbe: ReactProbe | undefined;
}
