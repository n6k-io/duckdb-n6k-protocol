"""Single source of truth for protocol constants.

Edit this file to change any cross-language protocol value, then run:

    make protocol-gen

…to regenerate every mirror: the TypeScript and C++ headers and this package's
`schema.json`. The full list is `PROTOCOL_GENERATED` in the Makefile. Note that
Python's runtime header validation reads the generated `schema.json`, so editing
`_FRAME_HEADERS` without regenerating changes nothing at runtime.

`make protocol-check` verifies the generated files are up to date, but nothing
runs it automatically — there is no pre-commit config and no CI job invoking it.

This module is imported directly by integration tests; TypeScript, C++ and Go
consume the generated mirror files.
"""

from __future__ import annotations

import struct
from typing import Any, Final, Union

# The major version: bump only on incompatible changes (frame layout, op semantics, etc.).
# Additive changes are negotiated as capabilities instead (HELLO_CAPS / OPEN_CAPS). Both ends check
# it: the server against HELLO (HELLO_ERR on mismatch), the client against HELLO_ACK.
N6K_PROTOCOL_VERSION = 13

# ── Frame types ──────────────────────────────────────────────────────────────
FT_REQ: Final = 1
FT_RESP_SCHEMA: Final = 2
FT_RESP_CHUNK: Final = 3
FT_RESP_END: Final = 4
FT_ERR: Final = 5
FT_CANCEL: Final = 6
FT_CREDIT: Final = 7
FT_PUSH: Final = 8
FT_PING: Final = 9
FT_PONG: Final = 10
FT_HELLO: Final = 11
FT_HELLO_ACK: Final = 12
FT_HELLO_ERR: Final = 13
FT_OPEN: Final = 15
FT_OPEN_ACK: Final = 16
FT_OPEN_ERR: Final = 17
FT_OPEN_PENDING: Final = 18
FT_HELLO_PENDING: Final = 19
FT_LINK_OPEN: Final = 20
FT_LINK_CANCEL: Final = 21
FT_LINK_CLOSE: Final = 22
FT_LINK_END: Final = 23
FT_STREAM_DATA: Final = 24
FT_STREAM_END: Final = 25
FT_STREAM_CREDIT: Final = 26

_FRAME_TYPES = [
    ("REQ", FT_REQ),
    ("RESP_SCHEMA", FT_RESP_SCHEMA),
    ("RESP_CHUNK", FT_RESP_CHUNK),
    ("RESP_END", FT_RESP_END),
    ("ERR", FT_ERR),
    ("CANCEL", FT_CANCEL),
    ("CREDIT", FT_CREDIT),
    ("PUSH", FT_PUSH),
    ("PING", FT_PING),
    ("PONG", FT_PONG),
    ("HELLO", FT_HELLO),
    ("HELLO_ACK", FT_HELLO_ACK),
    ("HELLO_ERR", FT_HELLO_ERR),
    ("OPEN", FT_OPEN),
    ("OPEN_ACK", FT_OPEN_ACK),
    ("OPEN_ERR", FT_OPEN_ERR),
    ("OPEN_PENDING", FT_OPEN_PENDING),
    ("HELLO_PENDING", FT_HELLO_PENDING),
    ("LINK_OPEN", FT_LINK_OPEN),
    ("LINK_CANCEL", FT_LINK_CANCEL),
    ("LINK_CLOSE", FT_LINK_CLOSE),
    ("LINK_END", FT_LINK_END),
    ("STREAM_DATA", FT_STREAM_DATA),
    ("STREAM_END", FT_STREAM_END),
    ("STREAM_CREDIT", FT_STREAM_CREDIT),
]

# ── Frame flags ─────────────────────────────────────────────────────────────
# Legacy: no longer carried on the wire. Headers are msgpack maps with no `flags`
# key, and the C++ client synthesizes these locally from the frame type
# (src/n6k_client/ws_client.cpp). Kept because the generated mirrors still expose them.
FLAG_END_OF_STREAM = 0x01
FLAG_IS_ARROW_IPC = 0x04

_FLAGS = [
    ("END_OF_STREAM", FLAG_END_OF_STREAM),
    ("IS_ARROW_IPC", FLAG_IS_ARROW_IPC),
]

# ── REQ op codes ─────────────────────────────────────────────────────────────
# `Final` for the same reason as the frame types: usable as `Literal[...]`.
OP_SCHEMA_LIST: Final = 1
OP_TABLES_LIST: Final = 2
OP_TABLE_SCHEMA: Final = 3
OP_INSERT: Final = 5
OP_EXEC: Final = 6
OP_QUERY: Final = 7
OP_RPC_SCALAR: Final = 8
OP_RPC_TABLE: Final = 9
OP_CATALOG_INVALIDATED: Final = 12
OP_CATALOG_LIST: Final = 19
OP_CATALOGS_CHANGED: Final = 20
OP_HAS_FUNCTION: Final = 23
OP_CREATE_SCHEMA: Final = 24

_OPS = [
    ("SCHEMA_LIST", OP_SCHEMA_LIST),
    ("TABLES_LIST", OP_TABLES_LIST),
    ("TABLE_SCHEMA", OP_TABLE_SCHEMA),
    ("INSERT", OP_INSERT),
    ("EXEC", OP_EXEC),
    ("QUERY", OP_QUERY),
    ("RPC_SCALAR", OP_RPC_SCALAR),
    ("RPC_TABLE", OP_RPC_TABLE),
    ("CATALOG_INVALIDATED", OP_CATALOG_INVALIDATED),
    ("CATALOG_LIST", OP_CATALOG_LIST),
    ("CATALOGS_CHANGED", OP_CATALOGS_CHANGED),
    ("HAS_FUNCTION", OP_HAS_FUNCTION),
    ("CREATE_SCHEMA", OP_CREATE_SCHEMA),
]


def frame_type_name(ft: int) -> str:
    return {code: name for name, code in _FRAME_TYPES}[ft]


# ── Frame header schema (SSOT for the msgpack header map of every frame) ──────
#
# Each frame is one msgpack map (the header) optionally followed by a raw Arrow
# body. This table is the single source of truth for the header shape of every
# frame: `codegen.render_schema_json()` renders it into `schema.json` (a
# JSON-Schema `oneOf` discriminated on `t` — and on `op` for REQ/PUSH — that the
# generated mirrors document) and `engine.validate_header()` enforces it at the
# trust boundary. Keep headers strictly JSON-typed (no msgpack bin/ext) so the
# decoded map validates directly.
#
# A field is `(name, type, required)` where `type` is either a JSON-Schema type
# string ("string"/"integer"/"boolean"/"object"/"array") or a full schema dict
# (for typed arrays / nullable / const values). Every variant also carries `t`
# (frame type) implicitly, and REQ/PUSH variants carry an `op` discriminator;
# both are emitted as `const` in the schema, so callers never list them here.

# Reusable field specs.
_F_ID: Any = ("id", "integer", True)
# `ns` (session routing key): one socket carries many catalog sessions, each opened
# by an OPEN that names its `ns`. The server routes inbound REQ/CANCEL/CREDIT by
# `ns` and stamps every response with it so the client bridge can demux. Added to
# every session-scoped variant (see _NS_EXEMPT); OPEN* declare it required.
_F_NS: Any = ("ns", "integer", False)
_F_NS_REQUIRED: Any = ("ns", "integer", True)
_ARRAY_OF_STR: dict[str, Any] = {"type": "array", "items": {"type": "string"}}
_ARRAY_OF_STR_ARRAYS: dict[str, Any] = {"type": "array", "items": _ARRAY_OF_STR}
# Each TABLES_LIST entry is a row object ({schema, name, verbs, primary_keys, not_null, unique_keys}); each schema
# entry {name, verbs}.
_ARRAY_OF_OBJ: dict[str, Any] = {"type": "array", "items": {"type": "object"}}

# variant: {"name", "t", optional "op", "fields": [field, ...], optional
#           "body": "arrow"|"arrow?"}. `body` documents that a raw Arrow body
# follows the header (and whether it is optional); it is metadata for the codec
# and docs, not part of the validated header map.
_FRAME_HEADERS: list[dict[str, Any]] = [
    # ── Handshake / lifecycle ────────────────────────────────────────────────
    # `capabilities` on HELLO/OPEN is what the sender can use; on the ACK it is the intersection with
    # the server's set, less any APP_HELLO_CAPS the host's app does not serve. An unknown capability
    # is ignored, never an error.
    {
        "name": "HELLO",
        "t": FT_HELLO,
        "fields": [
            ("token", "string", True),
            ("protocol_version", "integer", True),
            ("capabilities", _ARRAY_OF_STR, False),
        ],
    },
    {
        "name": "HELLO_ACK",
        "t": FT_HELLO_ACK,
        "fields": [("protocol_version", "integer", True), ("capabilities", _ARRAY_OF_STR, True)],
    },
    {
        "name": "HELLO_ERR",
        "t": FT_HELLO_ERR,
        "fields": [
            ("exception_type", "string", True),
            ("exception_message", "string", True),
            ("retriable", "boolean", False),
        ],
    },
    # The HELLO counterpart of OPEN_PENDING: the server is still authenticating. Same cadence and
    # stall rule, ended by exactly one HELLO_ACK or HELLO_ERR.
    {"name": "HELLO_PENDING", "t": FT_HELLO_PENDING, "fields": [("message", "string", False)]},
    {
        "name": "OPEN",
        "t": FT_OPEN,
        "fields": [_F_NS_REQUIRED, ("catalog", "string", True), ("capabilities", _ARRAY_OF_STR, False)],
    },
    {
        "name": "OPEN_ACK",
        "t": FT_OPEN_ACK,
        "fields": [
            _F_NS_REQUIRED,
            ("max_concurrent_reqs", "integer", True),
            ("default_batch_credits", "integer", True),
            ("capabilities", _ARRAY_OF_STR, True),
        ],
    },
    {
        "name": "OPEN_ERR",
        "t": FT_OPEN_ERR,
        "fields": [
            _F_NS_REQUIRED,
            ("exception_type", "string", True),
            ("exception_message", "string", True),
            ("retriable", "boolean", False),
        ],
    },
    # Sent instead of OPEN_ACK while the catalog is accepted but not ready, then repeated every
    # OPEN_PENDING_INTERVAL_MS until exactly one OPEN_ACK or OPEN_ERR closes the open.
    {"name": "OPEN_PENDING", "t": FT_OPEN_PENDING, "fields": [_F_NS_REQUIRED, ("message", "string", False)]},
    {"name": "PING", "t": FT_PING, "fields": [("id", "integer", False)]},
    {"name": "PONG", "t": FT_PONG, "fields": [("id", "integer", False)]},
    # ── Control plane ────────────────────────────────────────────────────────
    {
        "name": "ERR",
        "t": FT_ERR,
        "fields": [
            _F_ID,
            ("exception_type", "string", True),
            ("exception_message", "string", True),
            ("retriable", "boolean", False),
        ],
    },
    {"name": "CANCEL", "t": FT_CANCEL, "fields": [_F_ID]},
    {"name": "CREDIT", "t": FT_CREDIT, "fields": [_F_ID, ("n", "integer", True)]},
    # ── Links: one far transaction and its streams. See docs/n6k-network-protocol.md ─────
    {"name": "LINK_OPEN", "t": FT_LINK_OPEN, "fields": [("link", "integer", True)]},
    {"name": "LINK_CANCEL", "t": FT_LINK_CANCEL, "fields": [("link", "integer", True)]},
    {"name": "LINK_CLOSE", "t": FT_LINK_CLOSE, "fields": [("link", "integer", True)]},
    {
        "name": "LINK_END",
        "t": FT_LINK_END,
        "fields": [
            ("link", "integer", True),
            ("exception_type", "string", False),
            ("exception_message", "string", False),
        ],
    },
    {
        "name": "STREAM_DATA",
        "t": FT_STREAM_DATA,
        "fields": [("link", "integer", True), ("stream", "integer", True), ("fin", "boolean", True)],
        "body": "bytes",
    },
    {
        "name": "STREAM_END",
        "t": FT_STREAM_END,
        "fields": [
            ("link", "integer", True),
            ("stream", "integer", True),
            ("exception_type", "string", False),
            ("exception_message", "string", False),
        ],
    },
    {
        "name": "STREAM_CREDIT",
        "t": FT_STREAM_CREDIT,
        "fields": [("link", "integer", True), ("stream", "integer", True), ("n", "integer", True)],
    },
    {
        "name": "PUSH_CATALOG_INVALIDATED",
        "t": FT_PUSH,
        "op": OP_CATALOG_INVALIDATED,
        "fields": [("schemas", _ARRAY_OF_STR, False)],
    },
    {
        "name": "PUSH_CATALOGS_CHANGED",
        "t": FT_PUSH,
        "op": OP_CATALOGS_CHANGED,
        "fields": [("catalogs", _ARRAY_OF_STR, True)],
    },
    # ── Responses ────────────────────────────────────────────────────────────
    {"name": "RESP_SCHEMA", "t": FT_RESP_SCHEMA, "fields": [_F_ID], "body": "arrow"},
    {
        "name": "RESP_CHUNK_ARROW",
        "t": FT_RESP_CHUNK,
        "fields": [_F_ID, ("arrow", {"const": True}, True)],
        "body": "arrow",
    },
    {"name": "RESP_CHUNK_SCHEMA", "t": FT_RESP_CHUNK, "fields": [_F_ID, ("schemas", _ARRAY_OF_STR, True)]},
    {"name": "RESP_CHUNK_CATALOG", "t": FT_RESP_CHUNK, "fields": [_F_ID, ("catalogs", _ARRAY_OF_STR, True)]},
    {
        "name": "RESP_CHUNK_TABLES",
        "t": FT_RESP_CHUNK,
        "fields": [_F_ID, ("tables", _ARRAY_OF_OBJ, True), ("schemas", _ARRAY_OF_OBJ, True)],
    },
    {
        "name": "RESP_END",
        "t": FT_RESP_END,
        "fields": [
            _F_ID,
            ("rowcount", {"type": ["integer", "null"]}, False),
            ("cancelled", "boolean", False),
            ("ok", "boolean", False),
            ("txn", "integer", False),
            ("exists", "boolean", False),
        ],
    },
    # ── Requests (t=REQ, discriminated on op) ────────────────────────────────
    {"name": "REQ_CATALOG_LIST", "t": FT_REQ, "op": OP_CATALOG_LIST, "fields": [_F_ID]},
    {"name": "REQ_SCHEMA_LIST", "t": FT_REQ, "op": OP_SCHEMA_LIST, "fields": [_F_ID]},
    {
        "name": "REQ_TABLES_LIST",
        "t": FT_REQ,
        "op": OP_TABLES_LIST,
        "fields": [_F_ID, ("schema", "string", False)],
    },
    {
        "name": "REQ_TABLE_SCHEMA",
        "t": FT_REQ,
        "op": OP_TABLE_SCHEMA,
        "fields": [_F_ID, ("schema", "string", True), ("table", "string", True)],
    },
    {
        "name": "REQ_INSERT",
        "t": FT_REQ,
        "op": OP_INSERT,
        "fields": [_F_ID, ("schema", "string", True), ("table", "string", True)],
        "body": "arrow",
    },
    {"name": "REQ_EXEC", "t": FT_REQ, "op": OP_EXEC, "fields": [_F_ID, ("sql", "string", True)]},
    {
        "name": "REQ_QUERY",
        "t": FT_REQ,
        "op": OP_QUERY,
        "fields": [_F_ID, ("sql", "string", True), ("_batch_rows", "integer", False)],
    },
    {
        "name": "REQ_RPC_SCALAR",
        "t": FT_REQ,
        "op": OP_RPC_SCALAR,
        "fields": [_F_ID, ("function", "string", True), ("args", "array", False)],
    },
    {
        "name": "REQ_RPC_TABLE",
        "t": FT_REQ,
        "op": OP_RPC_TABLE,
        "fields": [_F_ID, ("function", "string", True), ("args", "array", False)],
        "body": "arrow?",
    },
    {
        "name": "REQ_CREATE_SCHEMA",
        "t": FT_REQ,
        "op": OP_CREATE_SCHEMA,
        "fields": [_F_ID, ("schema", "string", True), ("on_conflict", "string", True)],
    },
    # `args` are the argument types of a bound call, as DuckDB spells them.
    {
        "name": "REQ_HAS_FUNCTION",
        "t": FT_REQ,
        "op": OP_HAS_FUNCTION,
        "fields": [_F_ID, ("name", "string", True), ("args", _ARRAY_OF_STR, True)],
    },
]


# Inject the `ns` routing key into every session-scoped variant. HELLO*,
# PING/PONG, REQ_CATALOG_LIST, RESP_CHUNK_CATALOG and PUSH_CATALOGS_CHANGED are connection-scoped; OPEN* already declare `ns` as required. Done as
# a post-pass rather than repeating _F_NS in ~20 field lists — the mutation runs at
# import, before codegen/validation read the table.
_NS_EXEMPT: Final = {
    "PING",
    "PONG",
    "HELLO",
    "HELLO_ACK",
    "HELLO_ERR",
    "HELLO_PENDING",
    "OPEN",
    "OPEN_ACK",
    "OPEN_ERR",
    "OPEN_PENDING",
    "REQ_CATALOG_LIST",
    "RESP_CHUNK_CATALOG",
    "PUSH_CATALOGS_CHANGED",
}
for _variant in _FRAME_HEADERS:
    if _variant["name"] not in _NS_EXEMPT:
        _variant["fields"] = [*_variant["fields"], _F_NS]


def _field_schema(json_type: Union[str, dict[str, Any]]) -> dict[str, Any]:
    if isinstance(json_type, str):
        return {"type": json_type}
    return dict(json_type)


def header_variant_schema(variant: dict[str, Any]) -> dict[str, Any]:
    """Render one `_FRAME_HEADERS` entry into a JSON-Schema object: `t` (and
    `op`) as discriminating consts, declared fields as properties, required
    fields collected, `additionalProperties` closed so unknown keys are
    rejected (drift detection)."""
    properties: dict[str, Any] = {"t": {"const": variant["t"]}}
    required: list[str] = ["t"]
    if "op" in variant:
        properties["op"] = {"const": variant["op"]}
        required.append("op")
    for name, json_type, is_required in variant["fields"]:
        properties[name] = _field_schema(json_type)
        if is_required:
            required.append(name)
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


# ── Stream framing ───────────────────────────────────────────────────────────
# A raw byte stream frames every message as [4-byte BE payload length][1-byte type][payload].
# JSONRPC and N6K are the RFC 6455 text/binary opcodes. CLOSE is extension -> host only, on the host's own
# socketpair: msgpack {code, reason}, the WebSocket close the host performs; nothing follows it.
STREAM_FRAME_JSONRPC: Final = 0x01
STREAM_FRAME_N6K: Final = 0x02
STREAM_FRAME_CLOSE: Final = 0x04
STREAM_HEADER: Final = struct.Struct(">IB")

# ── Server tunables (advertised via OPEN_ACK) ────────────────────────────────
DEFAULT_BATCH_CREDITS = 8
MAX_CONCURRENT_REQS = 64

# ── Pending open ─────────────────────────────────────────────────────────────
# The server repeats OPEN_PENDING per parked `ns` (and HELLO_PENDING per socket) at this cadence; a
# client fails the open (or handshake) once OPEN_PENDING_STALL_MS passes with no pending, ACK or ERR.
# There is no overall deadline.
OPEN_PENDING_INTERVAL_MS: Final = 2000
OPEN_PENDING_STALL_MS: Final = 6000

# ── Exception types (the `exception_type` field of ERR / HELLO_ERR / OPEN_ERR) ─
#
# The closed set of values a server may put in `exception_type`. The value names the DuckDB
# Exception subclass the client reconstructs; anything outside this set MUST be
# treated as `IOException` by the client, which means an unrecognized value silently
# loses its type rather than failing loudly.
#
# This lives here because it was already maintained in three places — the prose table
# in docs/n6k-network-protocol.md, the Python server's own duckdb-class tuple, and
# the client's own reconstruction in src/n6k_client/include/n6k_err_throw.hpp — and they had
# drifted: `DependencyException` was documented and reachable but unmapped, so it went
# out as `IOException`.
#
# Note this is the *wire* spelling, which is the DuckDB class name. It is NOT what
# `Exception::ExceptionTypeToString` returns (that yields "Catalog", "TransactionContext",
# "Not implemented"); converting between the two is what src/common/n6k_exception_types.cpp
# owns, and getting it wrong is how a typed error degrades to IOException.
EXCEPTION_TYPES: Final = (
    "BinderException",
    "ConstraintException",
    "ConversionException",
    "DependencyException",
    "InvalidInputException",
    "InvalidTypeException",
    "OutOfRangeException",
    "ParserException",
    "SyntaxException",
    "TypeMismatchException",
    "CatalogException",
    "NotImplementedException",
    "ConnectionException",
    "FatalException",
    "HTTPException",
    "InternalException",
    "InterruptException",
    "IOException",
    "OutOfMemoryException",
    "PermissionException",
    "SequenceException",
    "SerializationException",
    "TransactionException",
)


# ── Capabilities ─────────────────────────────────────────────────────────────
# Negotiated as an intersection, per socket on HELLO and per catalog on OPEN. The client needs the
# answer before it plans a query, not when it runs one.
CAP_CROSSING: Final = "crossing"
CAP_JSONRPC: Final = "jsonrpc"
CAP_MCP: Final = "mcp"
CAP_CATALOG_PLANS: Final = "catalog_plans"
CAP_HAS_FUNCTION: Final = "has_function"
CAP_SCHEMA_DDL: Final = "schema_ddl"

# What this implementation supports at each level; client and server both send their set.
HELLO_CAPS: Final = (CAP_CROSSING, CAP_JSONRPC, CAP_MCP)
# The subset of HELLO_CAPS the server grants only when the app serving the socket provides it.
APP_HELLO_CAPS: Final = (CAP_JSONRPC, CAP_MCP)
OPEN_CAPS: Final = (CAP_CATALOG_PLANS, CAP_HAS_FUNCTION, CAP_SCHEMA_DDL)
