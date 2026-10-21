import { cpSync, mkdirSync, existsSync, readdirSync } from "node:fs";
import path from "node:path";
import { wasmDir } from "./wasm-dir";

/** Copy wasm extensions; each version/variant lands at `<dest>/<version>/<variant>/`. */
export function copyWasmExtensions(
  dest: string,
  opts: { versions?: string[]; variants?: string[] } = {},
) {
  const versions = opts.versions ?? readdirSync(wasmDir);

  for (const version of versions) {
    const versionDir = path.join(wasmDir, version);
    if (!existsSync(versionDir)) {
      console.warn(`Skipping wasm copy: ${versionDir} not found`);
      continue;
    }

    const variants = opts.variants ?? readdirSync(versionDir);

    for (const variant of variants) {
      const src = path.join(versionDir, variant);
      if (!existsSync(src)) {
        console.warn(`Skipping wasm copy: ${src} not found`);
        continue;
      }

      const target = path.join(dest, version, variant);
      mkdirSync(target, { recursive: true });
      cpSync(src, target, { recursive: true });
      console.log(`Copied wasm extensions to ${target}`);
    }
  }
}
