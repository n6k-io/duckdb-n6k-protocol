import { describe, it, expect } from "bun:test";
import { createJsonRpcClient, JsonRpcError } from "../jsonrpc";

function harness() {
  const sent: Record<string, unknown>[] = [];
  const client = createJsonRpcClient((text) => sent.push(JSON.parse(text)));
  const reply = (body: Record<string, unknown>) =>
    client.receive(JSON.stringify({ jsonrpc: "2.0", ...body }));
  return { sent, client, reply };
}

describe("createJsonRpcClient", () => {
  it("resolves a call with its result", async () => {
    const { sent, client, reply } = harness();
    const p = client.call("echo", { x: 1 });
    const id = sent[0].id;
    expect(id).toBeString();
    expect(sent).toEqual([
      { jsonrpc: "2.0", method: "echo", params: { x: 1 }, id },
    ]);
    reply({ id, result: 1 });
    expect(await p).toBe(1);
  });

  it("rejects a call with JsonRpcError", async () => {
    const { sent, client, reply } = harness();
    const p = client.call("fail");
    reply({
      id: sent[0].id,
      error: { code: 1234, message: "boom", data: { k: 1 } },
    });
    const error = await p.catch((error_: unknown) => error_);
    expect(error).toBeInstanceOf(JsonRpcError);
    expect(error).toMatchObject({
      code: 1234,
      message: "boom",
      data: { k: 1 },
    });
  });

  it("rejects a malformed error reply", async () => {
    const { sent, client, reply } = harness();
    const a = client.call("a");
    const b = client.call("b");
    reply({ id: sent[0].id, error: "boom" });
    reply({ id: sent[1].id, error: { message: "no code" } });
    for (const p of [a, b]) {
      const error = await p.catch((error_: unknown) => error_);
      expect(error).not.toBeInstanceOf(JsonRpcError);
      expect((error as Error).message).toContain("malformed JSON-RPC error");
    }
  });

  it("rejects a reply with neither result nor error", async () => {
    const { sent, client, reply } = harness();
    const p = client.call("a");
    reply({ id: sent[0].id });
    const error = await p.catch((error_: Error) => error_.message);
    expect(error).toContain("neither result nor error");
  });

  it("matches replies by id out of order", async () => {
    const { sent, client, reply } = harness();
    const a = client.call("a");
    const b = client.call("b");
    expect(sent[0].id).not.toBe(sent[1].id);
    reply({ id: sent[1].id, result: "b" });
    reply({ id: sent[0].id, result: "a" });
    expect(await Promise.all([a, b])).toEqual(["a", "b"]);
  });

  it("times out", async () => {
    const { client } = harness();
    const error = await client
      .call("slow", undefined, { timeoutMs: 10 })
      .catch((error_: Error) => error_.message);
    expect(error).toContain("no reply within 10ms");
  });

  it("aborts, and ignores a late reply", async () => {
    const { sent, client, reply } = harness();
    const ac = new AbortController();
    const p = client.call("slow", undefined, { signal: ac.signal });
    ac.abort(new Error("stop"));
    expect(await p.catch((error: Error) => error.message)).toBe("stop");
    reply({ id: sent[0].id, result: "late" });
  });

  it("rejects at once on an already-aborted signal without sending", async () => {
    const { sent, client } = harness();
    const ac = new AbortController();
    ac.abort();
    await expect(
      client.call("x", undefined, { signal: ac.signal }),
    ).rejects.toThrow();
    expect(sent).toEqual([]);
  });

  it("rejects when send throws", async () => {
    const client = createJsonRpcClient(() => {
      throw new Error("not open");
    });
    await expect(client.call("x")).rejects.toThrow("not open");
  });

  it("sends a notification without an id", () => {
    const { sent, client } = harness();
    client.notify("ping");
    expect(sent).toEqual([{ jsonrpc: "2.0", method: "ping" }]);
  });

  it("delivers server notifications to on() until off", () => {
    const { client, reply } = harness();
    const seen: unknown[] = [];
    const off = client.on("tick", (params) => seen.push(params));
    reply({ method: "tick", params: 1 });
    off();
    reply({ method: "tick", params: 2 });
    reply({ method: "unknown" });
    expect(seen).toEqual([1]);
  });

  it("answers a server request with method-not-found", () => {
    const { sent, client, reply } = harness();
    client.on("ask", () => {});
    reply({ method: "ask", id: 7 });
    expect(sent).toEqual([
      {
        jsonrpc: "2.0",
        id: 7,
        error: { code: -32_601, message: "method not found: ask" },
      },
    ]);
  });

  it("drops non-JSON and unknown replies", () => {
    const { client, reply } = harness();
    client.receive("not json");
    client.receive("[1]");
    reply({ id: "unknown", result: 1 });
    reply({ id: 1, result: 1 });
  });

  it("fail() rejects pending calls and the client keeps working", async () => {
    const { sent, client, reply } = harness();
    const p = client.call("a");
    client.fail("socket closed");
    expect(await p.catch((error: Error) => error.message)).toBe(
      "socket closed",
    );
    const q = client.call("b");
    reply({ id: sent[1].id, result: "b" });
    expect(await q).toBe("b");
  });

  it("rebind() sends on the new function", () => {
    const { sent, client } = harness();
    const next: string[] = [];
    client.rebind((text) => next.push(text));
    client.notify("x");
    expect(sent).toEqual([]);
    expect(next).toHaveLength(1);
  });
});
