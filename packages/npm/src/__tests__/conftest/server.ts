import type { ConnectionLike } from "../../connection-shape";
import { SERVER } from "../_server-gate";

export { SERVER } from "../_server-gate";

export async function serverUp(): Promise<boolean> {
  try {
    const r = await fetch(`${SERVER}/debug/counts`, {
      signal: AbortSignal.timeout(2000),
    });
    return r.ok;
  } catch {
    return false;
  }
}

export async function resetCounts(): Promise<void> {
  await fetch(`${SERVER}/debug/counts/reset`, { method: "POST" });
}

export async function getCounts(): Promise<{
  http: Record<string, number>;
  ws: Record<string, number>;
}> {
  const r = await fetch(`${SERVER}/debug/counts`);
  return (await r.json()) as {
    http: Record<string, number>;
    ws: Record<string, number>;
  };
}

export async function wsCloseAll(): Promise<void> {
  await fetch(`${SERVER}/debug/ws/close_all`, { method: "POST" });
}

// Run DDL server-side; the remote CREATE TABLE path drops constraints like PRIMARY KEY.
export async function serverExec(sql: string, catalog = "db"): Promise<void> {
  const url = `${SERVER}/debug/server_exec?catalog=${catalog}&sql=${encodeURIComponent(sql)}`;
  const r = await fetch(url, { method: "POST" });
  if (!r.ok) {
    throw new Error(`server_exec failed: ${r.status} ${await r.text()}`);
  }
}

export async function pushInvalidate(
  catalog: string,
  schemas: string,
): Promise<number> {
  const r = await fetch(
    `${SERVER}/debug/push_invalidate?catalog=${catalog}&schemas=${schemas}`,
    { method: "POST" },
  );
  if (!r.ok) throw new Error(`push_invalidate failed: ${r.status}`);
  const body = (await r.json()) as { handlers: number };
  return body.handlers;
}

// Serve `catalog` on every live /tuned socket serving `on`; returns how many sockets.
export async function addCatalog(on: string, catalog: string): Promise<number> {
  const r = await fetch(
    `${SERVER}/debug/catalog/add?on=${on}&catalog=${catalog}`,
    { method: "POST" },
  );
  if (!r.ok) throw new Error(`catalog/add failed: ${r.status}`);
  return ((await r.json()) as { added: number }).added;
}

// Stop serving `catalog` on every live /tuned socket serving it; returns how many sockets.
export async function removeCatalog(catalog: string): Promise<number> {
  const r = await fetch(`${SERVER}/debug/catalog/remove?catalog=${catalog}`, {
    method: "POST",
  });
  if (!r.ok) throw new Error(`catalog/remove failed: ${r.status}`);
  return ((await r.json()) as { removed: number }).removed;
}

export async function waitFor(
  pred: () => boolean,
  timeoutMs = 2000,
  stepMs = 10,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pred()) return true;
    await new Promise((r) => setTimeout(r, stepMs));
  }
  return pred();
}

// Deep-normalize for toEqual: BigInt→string, Date→ISO, Uint8Array→hex. Idempotent.
export function normalize(v: unknown): unknown {
  if (v === null || v === undefined) return v;
  if (typeof v === "bigint") return `bigint:${v.toString()}`;
  if (v instanceof Date) return `date:${v.toISOString()}`;
  if (v instanceof Uint8Array) {
    return `bytes:${[...v].map((b) => b.toString(16).padStart(2, "0")).join("")}`;
  }
  if (Array.isArray(v)) return v.map((x) => normalize(x));
  if (typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      out[k] = normalize(val);
    }
    return out;
  }
  return v;
}

export type Snapshot = {
  fields: { name: string; typeId: number; scale?: number }[];
  rows: unknown[];
};

// Decimal cells differ in shape per backend; canonicalize to decimal:<digits> (scale via field).
const ARROW_DECIMAL = 7;

function canonicalDecimal(v: unknown): string {
  return `decimal:${String(v).replaceAll(/[^\d-]/g, "")}`;
}

export async function snap(
  conn: ConnectionLike,
  sql: string,
): Promise<Snapshot> {
  const r = await conn.query(sql);
  const fields = r.schema.fields.map((f) => ({
    name: f.name,
    typeId: f.type.typeId,
    ...(f.type.scale === undefined ? {} : { scale: f.type.scale }),
  }));
  const decimalCols = new Set(
    fields.filter((f) => f.typeId === ARROW_DECIMAL).map((f) => f.name),
  );
  const rows = r.toArray().map((row) => {
    const obj = row.toJSON();
    if (decimalCols.size === 0) return normalize(obj);
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = decimalCols.has(k) && v != null ? canonicalDecimal(v) : v;
    }
    return normalize(out);
  });
  return { fields, rows };
}
