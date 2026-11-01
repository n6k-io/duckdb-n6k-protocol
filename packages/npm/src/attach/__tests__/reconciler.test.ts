import { describe, it, expect } from "bun:test";
import { createAttachReconciler } from "../reconciler";
import { makeConfig } from "../attachment";

describe("createAttachReconciler", () => {
  it("does not re-attach a catalog whose ATTACH is still in flight", async () => {
    const sent: string[] = [];
    const { promise: mydbGate, resolve: releaseMydb } =
      Promise.withResolvers<void>();
    const conn = {
      query: async (sql: string) => {
        sent.push(sql);
        if (sql.includes(" AS mydb ")) await mydbGate;
      },
    };
    const errors: string[] = [];
    const attached: string[] = [];
    const reconciler = createAttachReconciler(conn, {
      setup: () => [],
      onAttached: (c) => attached.push(c),
      onDetached: () => {},
      onError: (c, _config, message) => errors.push(`${c}: ${message}`),
    });

    const mydb = makeConfig("mydb");
    const kernel1 = makeConfig("kernel1");
    const first = reconciler.run({
      desired: { mydb },
      socketOf: {},
      registrations: {},
    });
    const second = reconciler.run({
      desired: { mydb, kernel1 },
      socketOf: {},
      registrations: {},
    });
    releaseMydb();
    await Promise.all([first, second]);

    expect(sent.filter((s) => s.includes(" AS mydb "))).toHaveLength(1);
    expect(attached).toContain("kernel1");
    expect(errors).toEqual([]);
  });
});
