// Untagged replies on a registered socket: answers to the connection-scoped requests JS issues itself.

import { FT, type FrameHeader } from "../ws-framing";

// Id of a connection-scoped reply (untagged RESP_CHUNK/RESP_END/ERR); PONG also carries an untagged id.
export function connectionReplyId(header: FrameHeader): number | undefined {
  if (typeof header.ns === "number" || typeof header.id !== "number") {
    return undefined;
  }
  return header.t === FT.RESP_CHUNK ||
    header.t === FT.RESP_END ||
    header.t === FT.ERR
    ? header.id
    : undefined;
}
