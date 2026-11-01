import { createIframeN6k } from "../../iframe/n6k-iframe";
import type { ChildApi } from "./iframe-e2e-types";

const SQL = "SELECT id, name, CAST(amt AS DOUBLE) AS amt FROM t ORDER BY id";

// Reads `t` from the parent once the mirrored status is ready; publishes progress through `publish`.
export function startChild(
  parentOrigin: string,
  publish: (api: ChildApi) => void,
): void {
  const n6k = createIframeN6k({ parentOrigin });
  const read = async (sql: string) => {
    const c = await n6k.connect();
    const r = await c.query(sql);
    return r.toArray().map((row) => row.toJSON());
  };
  const api: ChildApi = {
    status: "loading",
    rows: [],
    error: null,
    query: read,
  };
  publish(api);
  let started = false;
  n6k.subscribe(() => {
    if (started || n6k.getSnapshot().status !== "ready") return;
    started = true;
    read(SQL).then(
      (rows) => publish({ ...api, status: "ready", rows }),
      (error: unknown) =>
        publish({ ...api, status: "error", error: String(error) }),
    );
  });
}
