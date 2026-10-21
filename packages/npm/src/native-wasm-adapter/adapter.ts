// Adapts a @duckdb/node-api connection to the ArrowLikeResult shape the wasm React hooks consume (Arrow type ids, Decimal as raw mantissa).

import {
  DuckDBTypeId,
  DuckDBDecimalType,
  DuckDBDecimalValue,
  type DuckDBConnection,
} from "@duckdb/node-api";
import { Type } from "apache-arrow";
import type {
  ArrowLikeField,
  ArrowLikeResult,
  ConnectionLike,
} from "../connection-shape";

export type { ArrowLikeField, ArrowLikeResult } from "../connection-shape";
export type WasmShapeConnection = ConnectionLike;

const DUCKDB_TO_ARROW: Partial<Record<DuckDBTypeId, number>> = {
  [DuckDBTypeId.BOOLEAN]: Type.Bool,
  [DuckDBTypeId.TINYINT]: Type.Int,
  [DuckDBTypeId.SMALLINT]: Type.Int,
  [DuckDBTypeId.INTEGER]: Type.Int,
  [DuckDBTypeId.BIGINT]: Type.Int,
  [DuckDBTypeId.UTINYINT]: Type.Int,
  [DuckDBTypeId.USMALLINT]: Type.Int,
  [DuckDBTypeId.UINTEGER]: Type.Int,
  [DuckDBTypeId.UBIGINT]: Type.Int,
  [DuckDBTypeId.HUGEINT]: Type.Int,
  [DuckDBTypeId.UHUGEINT]: Type.Int,
  [DuckDBTypeId.FLOAT]: Type.Float,
  [DuckDBTypeId.DOUBLE]: Type.Float,
  [DuckDBTypeId.VARCHAR]: Type.Utf8,
  [DuckDBTypeId.BLOB]: Type.Binary,
  [DuckDBTypeId.DECIMAL]: Type.Decimal,
  [DuckDBTypeId.DATE]: Type.Date,
  [DuckDBTypeId.TIME]: Type.Time,
  [DuckDBTypeId.TIMESTAMP]: Type.Timestamp,
  [DuckDBTypeId.TIMESTAMP_S]: Type.Timestamp,
  [DuckDBTypeId.TIMESTAMP_MS]: Type.Timestamp,
  [DuckDBTypeId.TIMESTAMP_NS]: Type.Timestamp,
  [DuckDBTypeId.TIMESTAMP_TZ]: Type.Timestamp,
  [DuckDBTypeId.LIST]: Type.List,
  [DuckDBTypeId.STRUCT]: Type.Struct,
  [DuckDBTypeId.MAP]: Type.Map,
  [DuckDBTypeId.UUID]: Type.Utf8,
};

function arrowTypeIdFor(typeId: DuckDBTypeId): number {
  return DUCKDB_TO_ARROW[typeId] ?? Type.Utf8;
}

// Convert a getRowObjects() value to the shape hooks expect: Decimal→mantissa, nested DuckDB*Value→plain arrays/objects.
function jsonValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;
  if (value instanceof DuckDBDecimalValue) return value.value;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  const o = value as { items?: unknown; entries?: unknown };
  if (Array.isArray(o.items)) {
    return o.items.map((x) => jsonValue(x));
  }
  if (Array.isArray(o.entries)) {
    // DuckDBMapValue: entries is [{ key, value }].
    return (o.entries as Array<{ key: unknown; value: unknown }>).map((e) => ({
      key: jsonValue(e.key),
      value: jsonValue(e.value),
    }));
  }
  if (o.entries && typeof o.entries === "object") {
    // DuckDBStructValue: entries is a plain { field: value } object.
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(o.entries as Record<string, unknown>)) {
      out[k] = jsonValue(v);
    }
    return out;
  }
  return (value as { toString(): string }).toString();
}

export function toWasmShape(conn: DuckDBConnection): ConnectionLike {
  return {
    async query(sql: string): Promise<ArrowLikeResult> {
      const reader = await conn.runAndReadAll(sql);
      const names = reader.columnNames();
      const types = reader.columnTypes();
      const fields: ArrowLikeField[] = names.map((name, i) => {
        const t = types[i]!;
        const typeId = arrowTypeIdFor(t.typeId);
        if (t instanceof DuckDBDecimalType) {
          return { name, type: { typeId, scale: t.scale } };
        }
        return { name, type: { typeId } };
      });
      const rawRows = reader.getRowObjects();
      const rows: Record<string, unknown>[] = rawRows.map((row) => {
        const out: Record<string, unknown> = {};
        for (const name of names) {
          out[name] = jsonValue(row[name]);
        }
        return out;
      });
      return {
        schema: { fields },
        toArray() {
          return rows.map((row) => ({ toJSON: () => row }));
        },
      };
    },
    // Caller owns conn (disposed by createNativeDuckDB); nothing to lease here.
    close: async () => {},
  };
}
