"""n6k protocol: the wire format, and the codec for speaking it.

This package does not *serve* the protocol — the server is `src/n6k_server`, a
DuckDB extension, reached from Python via
`n6k_server.pump.serve_and_close_connection`. This package holds the definition and the
codec: `protocol.py` is the single source of truth that the TypeScript and C++
mirrors are generated from, and `pack_frame`/`unpack_frame` let a Python client or
a test speak the wire directly.
"""

from n6k_protocol.engine import (
    pack_frame,
    unpack_frame,
    validate_header,
)

__all__ = [
    "pack_frame",
    "unpack_frame",
    "validate_header",
]
