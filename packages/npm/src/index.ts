export {
  createN6kWorker,
  HelloRefusedError,
  type RegisteredWebsocket,
} from "./create-n6k-worker";
export {
  JsonRpcError,
  type JsonRpcCallOptions,
  type JsonRpcClient,
} from "./jsonrpc";
export { createDuckDB } from "./create-duckdb";
export type { DuckDBInstance } from "./create-duckdb";
export { N6kDuckDB } from "./n6k-duckdb";
export type {
  N6kDuckDBOptions,
  N6kDuckDBSnapshot,
  N6kDuckDBStatus,
} from "./n6k-duckdb";
export { N6K_VERSION } from "./version";
export type { N6kMetadataFns } from "./n6k-duckdb";
export type { WsStatus } from "./types";
export { serverN6k } from "./server-n6k";
export type { SocketSpec, SocketToken } from "./sockets/sockets";
export type { DatabaseSpec } from "./attach/database-spec";
export {
  configEqual,
  configFingerprint,
  errorOf,
  makeConfig,
  renderAttachSql,
  statusOf,
} from "./attach/attachment";
export type {
  AttachConfig,
  AttachError,
  AttachOptions,
  AttachState,
  AttachStatus,
} from "./attach/attachment";
export type { CatalogState, CatalogStore } from "./attach/catalog-states";
export { parseSqlTables } from "./metadata/parse-sql-tables";
export type {
  PrivilegeType,
  TableRef,
  TableRefType,
} from "./metadata/parse-sql-tables";
export { describeN6kTable } from "./metadata/table-describe";
export type {
  ColumnMeta,
  TableIdent,
  TableMeta,
} from "./metadata/table-describe";
export type {
  ArrowLikeBatch,
  ArrowLikeResult,
  ConnectionLike,
  StreamReader,
} from "./connection-shape";
