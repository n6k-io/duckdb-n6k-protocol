export type ChildApi = {
  status: string;
  rows: Array<Record<string, unknown>>;
  error: string | null;
  query: (sql: string) => Promise<Array<Record<string, unknown>>>;
};

declare global {
  var __parentReady: boolean | undefined;
  var __parentError: string | undefined;
  var __bumpParent: (() => void) | undefined;
  var __child: ChildApi | undefined;
  var __childReady: boolean | undefined;
  var __childError: string | undefined;
  var __childOpaque: ChildApi | undefined;
  var __PARENT_ORIGIN: string | undefined;
}
