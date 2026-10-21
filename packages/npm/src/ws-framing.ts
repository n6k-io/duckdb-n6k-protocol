// Wire framing helpers: a frame is one self-delimiting msgpack header map optionally followed by a raw body.

import { encode, Decoder } from "@msgpack/msgpack";
import { FT, OP } from "./protocol-generated";

export {
  FT,
  OP,
  N6K_PROTOCOL_VERSION,
  HELLO_CAPS,
  OPEN_CAPS,
} from "./protocol-generated";

// msgpack header map; `t` always present, `id`/`op` on frames that carry them.
export type FrameHeader = {
  t: number;
  id?: number;
  op?: number;
  [key: string]: unknown;
};

export type Frame = {
  header: FrameHeader;
  body: Uint8Array;
};

// Encode one frame: msgpack header then raw body appended (not msgpack-wrapped, so body stays zero-copy).
export function packFrame(
  header: FrameHeader,
  body?: Uint8Array,
): Uint8Array<ArrayBuffer> {
  const head = encode(header);
  if (!body || body.byteLength === 0) {
    // encode may return a pooled subarray; copy to an exact-fit buffer so callers can transfer it.
    return new Uint8Array(head);
  }
  const out = new Uint8Array(head.byteLength + body.byteLength);
  out.set(head, 0);
  out.set(body, head.byteLength);
  return out;
}

// Recover the body offset via decodeMulti's advancing `pos` (decode() throws on trailing bytes).
class OffsetDecoder extends Decoder {
  get bytePos(): number {
    return (this as unknown as { pos: number }).pos;
  }
}

export function unpackFrame(buf: ArrayBuffer | Uint8Array): Frame {
  const bytes = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  const decoder = new OffsetDecoder();
  const header = decoder.decodeMulti(bytes).next().value as FrameHeader;
  if (
    header == null ||
    typeof header !== "object" ||
    typeof header.t !== "number"
  ) {
    throw new Error("n6k frame: header is not a map with a numeric `t`");
  }
  const body = bytes.subarray(decoder.bytePos);
  return { header, body };
}

export function tryHeader(
  buf: ArrayBuffer | Uint8Array,
): FrameHeader | undefined {
  try {
    return unpackFrame(buf).header;
  } catch {
    return undefined;
  }
}

// The schemas of a PUSH CATALOG_INVALIDATED, else undefined.
export function invalidatedSchemas(
  header: FrameHeader | undefined,
): string[] | undefined {
  if (header?.t !== FT.PUSH || header.op !== OP.CATALOG_INVALIDATED) {
    return undefined;
  }
  const schemas = Array.isArray(header.schemas) ? header.schemas : [];
  return schemas.filter((s): s is string => typeof s === "string");
}
