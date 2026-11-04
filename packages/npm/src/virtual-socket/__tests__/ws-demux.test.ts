import { test, expect, describe } from "bun:test";
import { FT } from "../../ws-framing";
import { connectionReplyId } from "../ws-demux";

describe("ws-demux", () => {
  test("connectionReplyId matches only untagged RESP_CHUNK/RESP_END/ERR", () => {
    expect(connectionReplyId({ t: FT.RESP_CHUNK, id: 3 })).toBe(3);
    expect(connectionReplyId({ t: FT.RESP_END, id: 3 })).toBe(3);
    expect(connectionReplyId({ t: FT.ERR, id: 3 })).toBe(3);
    expect(connectionReplyId({ t: FT.RESP_END, id: 3, ns: 1 })).toBeUndefined();
    expect(connectionReplyId({ t: FT.PONG, id: 3 })).toBeUndefined();
    expect(connectionReplyId({ t: FT.RESP_END })).toBeUndefined();
  });
});
