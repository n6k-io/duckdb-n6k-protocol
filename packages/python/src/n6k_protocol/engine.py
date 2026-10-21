"""The frame codec: msgpack header + optional Arrow IPC body, and back.

This is all that is left of what used to be a Python implementation of the
server. The protocol is served by `src/n6k_server` (a DuckDB extension); Python
reaches it by handing over a socket and pumping bytes (`n6k_server.pump`). What
survives here is what a *client* or a test needs to speak the wire directly:
`pack_frame` / `unpack_frame` and header validation against `schema.json`.

No starlette, FastAPI, or duckdb dependency.
"""

import importlib.resources
import json
from typing import Any

import msgpack
from jsonschema import Draft7Validator
from jsonschema import exceptions as js_exceptions

from n6k_protocol.protocol import frame_type_name

# ── Frame codec (msgpack header map + raw appended body) ──────────────────────
# A frame is one self-delimiting msgpack map (the header) optionally followed by
# a raw body (Arrow IPC bytes). No length prefix: the map is self-delimiting, so
# on decode the bytes after it are the body — sliced zero-copy from the same
# buffer, never re-wrapped through msgpack.


def _load_header_validators() -> dict[int, Draft7Validator]:
    """Build per-frame-type header validators from the generated schema.json
    (the SSOT mirror). Dispatching on `t` keeps validation O(1) on the frame
    type and then a tiny `oneOf` (1 variant for most frames, 3 for RESP_CHUNK,
    11 for REQ); the Arrow body is never schema-validated."""
    schema = json.loads(importlib.resources.files("n6k_protocol").joinpath("schema.json").read_text(encoding="utf-8"))
    by_t: dict[int, list[dict[str, Any]]] = {}
    for variant in schema["oneOf"]:
        t = variant["properties"]["t"]["const"]
        by_t.setdefault(t, []).append(variant)
    return {t: Draft7Validator({"oneOf": variants}) for t, variants in by_t.items()}


_HEADER_VALIDATORS = _load_header_validators()


def validate_header(header: dict[str, Any]) -> None:
    """Validate a frame header map against the generated schema. Raises
    `ValueError` on any violation (unknown frame type, wrong field type, missing
    required field, unknown key)."""
    t = header.get("t")
    if not isinstance(t, int) or t not in _HEADER_VALIDATORS:
        raise ValueError(f"invalid frame header: unknown frame type t={t!r}")
    error = js_exceptions.best_match(_HEADER_VALIDATORS[t].iter_errors(header))
    if error is not None:
        raise ValueError(f"invalid {frame_type_name(t)} header: {error.message}")


def pack_frame(header: dict[str, Any], body: bytes = b"") -> bytes:
    validate_header(header)
    return msgpack.packb(header, use_bin_type=True) + body


def unpack_frame(raw: bytes) -> tuple[dict[str, Any], memoryview]:
    """Decode one frame into `(header map, body)`. The body is a zero-copy
    `memoryview` slice of `raw` — the bytes after the self-delimiting header
    map. Does NOT validate: callers run `validate_header` where a malformed peer
    frame should be reported rather than crash the receive loop."""
    unpacker = msgpack.Unpacker(raw=False)
    unpacker.feed(raw)
    try:
        header = unpacker.unpack()
    except msgpack.OutOfData:
        raise ValueError("short frame: no complete msgpack header")
    if not isinstance(header, dict):
        raise ValueError(f"frame header is not a map: {type(header).__name__}")
    body = memoryview(raw)[unpacker.tell() :]
    return header, body
