import { startChild } from "./iframe-child";
import "./iframe-e2e-types";

try {
  const parentOrigin = globalThis.__PARENT_ORIGIN;
  if (!parentOrigin) throw new Error("missing injected __PARENT_ORIGIN");
  startChild(parentOrigin, (api) => {
    globalThis.__childOpaque = api;
  });
} catch (error) {
  globalThis.__childError =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
}
