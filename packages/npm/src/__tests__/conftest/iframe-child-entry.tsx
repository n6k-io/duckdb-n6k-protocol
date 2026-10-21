import { useEffect } from "react";
import { createRoot } from "react-dom/client";
import { QueryProvider } from "../../react/query-provider";
import { IFrameDuckDBProvider } from "../../react/iframe/iframe-duckdb-provider";
import { useQuery } from "../../react/use-sql-query";
import { useDuckDB } from "../../react/use-duckdb";
import "./iframe-e2e-types";

function Probe() {
  const r = useQuery("SELECT id, name, amt FROM t ORDER BY id", {
    tables: ["t"],
  });
  const { conn } = useDuckDB();

  useEffect(() => {
    globalThis.__child = {
      status: r.status,
      rows: r.rows,
      error: r.error,
      query: async (sql: string) => {
        const res = await conn!.query(sql);
        return res.toArray().map((row) => row.toJSON());
      },
    };
  });

  return null;
}

function boot(): void {
  const el = document.querySelector("#root");
  if (!el) throw new Error("missing #root");
  createRoot(el).render(
    <QueryProvider>
      <IFrameDuckDBProvider parentOrigin={globalThis.location.origin}>
        <Probe />
      </IFrameDuckDBProvider>
    </QueryProvider>,
  );
  globalThis.__childReady = true;
}

try {
  boot();
} catch (error) {
  globalThis.__childError =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
}
