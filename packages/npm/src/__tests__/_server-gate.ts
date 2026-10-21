import { describe } from "bun:test";

const TEST_SERVER = process.env.TEST_SERVER;
if (!TEST_SERVER) {
  throw new Error(
    "TEST_SERVER env var is required. Set TEST_SERVER=<base-url> " +
      "(e.g. http://localhost:8099) to run server tests, or TEST_SERVER=skip " +
      "to skip every server-requiring test (CI without a server).",
  );
}

export const SKIP_SERVER_TESTS = TEST_SERVER === "skip";

function deriveUrls(base: string): {
  SERVER: string;
  HOST: string;
  WS_URL: string;
  N6K_URL: string;
} {
  let u: URL;
  try {
    u = new URL(base);
  } catch {
    throw new Error(
      `TEST_SERVER="${base}" is not a valid URL. Use a base URL like ` +
        "http://localhost:8099, or TEST_SERVER=skip.",
    );
  }
  const wsProto = u.protocol === "https:" ? "wss:" : "ws:";
  return {
    SERVER: u.origin,
    HOST: u.host,
    WS_URL: `${wsProto}//${u.host}/ws`,
    N6K_URL: `n6k://${u.host}`,
  };
}

const urls = SKIP_SERVER_TESTS
  ? { SERVER: "", HOST: "", WS_URL: "", N6K_URL: "" }
  : deriveUrls(TEST_SERVER);

export const SERVER = urls.SERVER;
export const HOST = urls.HOST;
export const WS_URL = urls.WS_URL;
export const N6K_URL = urls.N6K_URL;

export const describeServer = SKIP_SERVER_TESTS ? describe.skip : describe;
