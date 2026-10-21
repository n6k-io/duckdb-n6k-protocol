"""HTTP credential extraction for the test server's prefix-gated mounts."""

from typing import Optional

from fastapi import Request


def _bearer(header_value: Optional[str]) -> Optional[str]:
    if header_value and header_value.startswith("Bearer "):
        return header_value[len("Bearer ") :]
    return None


def http_present_token(request: Request) -> Optional[str]:
    """The credential an HTTP request presents (Bearer header or `?token=`)."""
    return _bearer(request.headers.get("authorization")) or request.query_params.get("token")
