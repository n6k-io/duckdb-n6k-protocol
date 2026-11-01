import { startChild } from "./iframe-child";
import "./iframe-e2e-types";

try {
  startChild(globalThis.location.origin, (api) => {
    globalThis.__child = api;
  });
  globalThis.__childReady = true;
} catch (error) {
  globalThis.__childError =
    error instanceof Error ? (error.stack ?? error.message) : String(error);
}
