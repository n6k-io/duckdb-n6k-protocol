import { describe, it, expect } from "bun:test";
import fs from "node:fs";
import path from "node:path";

const IFRAME_INDEX = path.resolve(import.meta.dir, "../index.ts");
const CORE_INDEX = path.resolve(import.meta.dir, "../../core.ts");

const DENY_BARE = ["@duckdb/duckdb-wasm"];
const DENY_PATH = [
  "create-duckdb",
  "create-n6k-worker",
  "create-native-duckdb",
  `${path.sep}workers${path.sep}`,
];

const FROM_IMPORT =
  /\b(?:import|export)\b(\s+type\b)?[^;]*?\bfrom\s*["']([^"']+)["']/g;
const SIDE_EFFECT_IMPORT = /\bimport\s+["']([^"']+)["']/g;

function resolveRelative(fromFile: string, spec: string): string | null {
  const base = path.resolve(path.dirname(fromFile), spec);
  const candidates = [
    `${base}.ts`,
    `${base}.tsx`,
    path.join(base, "index.ts"),
    path.join(base, "index.tsx"),
  ];
  return candidates.find((c) => fs.existsSync(c)) ?? null;
}

type Scan = { files: Set<string>; bareImports: Set<string> };

function collectSpecifiers(content: string): string[] {
  const specs: string[] = [];
  for (const m of content.matchAll(FROM_IMPORT)) {
    const isTypeOnly = m[1] !== undefined;
    if (!isTypeOnly) specs.push(m[2]);
  }
  for (const m of content.matchAll(SIDE_EFFECT_IMPORT)) specs.push(m[1]);
  return specs;
}

function walk(entry: string): Scan {
  const files = new Set<string>();
  const bareImports = new Set<string>();
  const queue = [entry];
  while (queue.length > 0) {
    const file = queue.pop() as string;
    if (files.has(file)) continue;
    files.add(file);
    const content = fs.readFileSync(file, "utf8");
    for (const spec of collectSpecifiers(content)) {
      if (spec.startsWith(".")) {
        const resolved = resolveRelative(file, spec);
        if (resolved && !files.has(resolved)) queue.push(resolved);
      } else {
        bareImports.add(spec);
      }
    }
  }
  return { files, bareImports };
}

describe("iframe entry has no duckdb-wasm in its runtime import graph", () => {
  const scan = walk(IFRAME_INDEX);

  it("traverses past the barrel (sanity)", () => {
    expect(scan.files.size).toBeGreaterThanOrEqual(5);
  });

  it("imports no forbidden bare module (e.g. @duckdb/duckdb-wasm)", () => {
    const offenders = [...scan.bareImports].filter((s) =>
      DENY_BARE.some((d) => s === d || s.startsWith(`${d}/`)),
    );
    expect(offenders).toEqual([]);
  });

  it("reaches no wasm/worker/createDuckDB source file", () => {
    const offenders = [...scan.files].filter((f) =>
      DENY_PATH.some((d) => f.includes(d)),
    );
    expect(offenders).toEqual([]);
  });

  it("only pulls the expected externals (react, apache-arrow)", () => {
    expect([...scan.bareImports].toSorted()).toEqual(["apache-arrow", "react"]);
  });
});

describe("react/core entry has no duckdb-wasm in its runtime import graph", () => {
  // core is the wasm-free react subset but legitimately uses react-query, so assert deny-lists.
  const scan = walk(CORE_INDEX);

  it("traverses past the barrel (sanity)", () => {
    expect(scan.files.size).toBeGreaterThanOrEqual(5);
  });

  it("imports no forbidden bare module (e.g. @duckdb/duckdb-wasm)", () => {
    const offenders = [...scan.bareImports].filter((s) =>
      DENY_BARE.some((d) => s === d || s.startsWith(`${d}/`)),
    );
    expect(offenders).toEqual([]);
  });

  it("reaches no wasm/worker/createDuckDB source file", () => {
    const offenders = [...scan.files].filter((f) =>
      DENY_PATH.some((d) => f.includes(d)),
    );
    expect(offenders).toEqual([]);
  });
});
