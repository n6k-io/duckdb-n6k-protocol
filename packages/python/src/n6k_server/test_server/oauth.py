"""Minimal standards-based OAuth Authorization Server for tests.

Implements just enough of RFC 8414 (metadata), RFC 8693 (token exchange), and
JWKS to exercise the n6k client's discovery + mint. Signs EdDSA (Ed25519) JWTs
via PyJWT, so the data-side auth verifies real signatures + `exp` + `aud` rather
than comparing opaque strings. Single process serves both the AS endpoints and
the data WS/HTTP.
"""

import base64
import time
from typing import Any
from urllib.parse import parse_qsl

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# Audience stamped on minted JWTs and required by the data-side verifier.
AUDIENCE = "n6k-data-service"
# The (opaque) subject token the device flow would have issued; the token
# exchange only mints for this value.
ACCEPTED_SUBJECT_TOKEN = "test-subject-token"
JWT_TTL_SECONDS = 900  # 15 min, like production

TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
DEVICE_CODE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"
# Approve a device after this many polls (so the client exercises the
# authorization_pending → success transition).
APPROVE_AFTER_POLLS = 1


class OAuthServer:
    def __init__(self) -> None:
        self._device_polls: dict[str, int] = {}
        self._key = Ed25519PrivateKey.generate()
        self._priv_pem = self._key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        self._pub_pem = self._key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    def metadata(self, issuer: str) -> dict[str, Any]:
        return {
            "issuer": issuer,
            "token_endpoint": f"{issuer}/oauth/token",
            "device_authorization_endpoint": f"{issuer}/oauth/device",
            "jwks_uri": f"{issuer}/jwks",
            "grant_types_supported": [TOKEN_EXCHANGE_GRANT, DEVICE_CODE_GRANT],
        }

    def jwks(self) -> dict[str, Any]:
        raw = self._key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        x = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
        return {"keys": [{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig", "x": x}]}

    def sign(self, audience: str) -> str:
        now = int(time.time())
        claims = {"sub": "test-user", "aud": audience or AUDIENCE, "iat": now, "exp": now + JWT_TTL_SECONDS}
        return jwt.encode(claims, self._priv_pem, algorithm="EdDSA")

    def verify(self, token: str) -> bool:
        try:
            jwt.decode(token, self._pub_pem, algorithms=["EdDSA"], audience=AUDIENCE)
            return True
        except Exception:
            return False

    def device_authorize(self) -> dict[str, Any]:
        code = "test-device-code"
        self._device_polls[code] = 0
        return {
            "device_code": code,
            "user_code": "WXYZ-1234",
            "verification_uri": "/device",
            "verification_uri_complete": "/device?user_code=WXYZ-1234",
            "expires_in": 300,
            "interval": 1,
        }

    def device_poll(self, device_code: str) -> str | None:
        """Return the subject token once 'approved', else None (still pending)."""
        if device_code not in self._device_polls:
            return None
        self._device_polls[device_code] += 1
        if self._device_polls[device_code] > APPROVE_AFTER_POLLS:
            return ACCEPTED_SUBJECT_TOKEN
        return None


def mount_oauth_as(app: FastAPI) -> OAuthServer:
    """Mount the OAuth Authorization Server endpoints (discovery, JWKS, token,
    device) and return the server. Does NOT change global auth state — callers
    wire `server.verify` wherever they want JWT enforcement (the /oauth prefix
    mount)."""
    server = OAuthServer()

    def _issuer(request: Request) -> str:
        return str(request.base_url).rstrip("/")

    @app.get("/.well-known/oauth-authorization-server")
    def _metadata(request: Request) -> dict[str, Any]:
        return server.metadata(_issuer(request))

    @app.get("/jwks")
    def _jwks() -> dict[str, Any]:
        return server.jwks()

    @app.post("/oauth/device")
    async def _device(request: Request) -> Any:
        return server.device_authorize()

    @app.post("/oauth/token")
    async def _token(request: Request) -> Any:
        # Parse the application/x-www-form-urlencoded body directly so we don't
        # depend on python-multipart (request.form() requires it).
        raw = await request.body()
        form = dict(parse_qsl(raw.decode("utf-8")))
        grant = form.get("grant_type")
        if grant == TOKEN_EXCHANGE_GRANT:
            if form.get("subject_token") != ACCEPTED_SUBJECT_TOKEN:
                return JSONResponse({"error": "invalid_grant"}, status_code=400)
            resource = str(form.get("resource") or AUDIENCE)
            return {
                "access_token": server.sign(resource),
                "issued_token_type": ACCESS_TOKEN_TYPE,
                "token_type": "Bearer",
                "expires_in": JWT_TTL_SECONDS,
            }
        if grant == DEVICE_CODE_GRANT:
            subject = server.device_poll(str(form.get("device_code", "")))
            if subject is None:
                return JSONResponse({"error": "authorization_pending"}, status_code=400)
            return {"access_token": subject, "token_type": "Bearer", "expires_in": 604800}
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    return server
