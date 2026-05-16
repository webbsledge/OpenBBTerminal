"""``oidc_jwt`` auth backend — verify JWTs against a JWKS URL."""

from __future__ import annotations

import logging
from typing import Any

import jwt
from fastapi import HTTPException, Request
from jwt import PyJWKClient

from openbb_agent_server.runtime.plugins import AuthBackend
from openbb_agent_server.runtime.principal import UserPrincipal

logger = logging.getLogger("openbb_agent_server.auth.oidc_jwt")


class OidcJwtAuthBackend(AuthBackend):
    """Verify JWT bearer tokens against a JWKS URL."""

    name = "oidc_jwt"

    def __init__(
        self,
        *,
        jwks_url: str,
        audience: str | None = None,
        issuer: str | None = None,
        algorithms: tuple[str, ...] = ("RS256",),
        jwks_cache_seconds: int = 3600,
    ) -> None:
        if not jwks_url:
            raise RuntimeError("OidcJwtAuthBackend requires jwks_url")
        self._jwks_client = PyJWKClient(
            jwks_url, cache_keys=True, lifespan=jwks_cache_seconds
        )
        self._audience = audience
        self._issuer = issuer
        self._algorithms = list(algorithms)

    async def authenticate(self, request: Request) -> UserPrincipal:
        auth = request.headers.get("authorization") or ""
        if not auth.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="missing bearer token")
        token = auth[len("bearer ") :].strip()

        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token).key
        except Exception as exc:
            logger.warning("oidc_jwt: JWKS lookup failed: %s", exc)
            raise HTTPException(
                status_code=403, detail="cannot verify signing key"
            ) from exc

        try:
            claims = jwt.decode(
                token,
                key=signing_key,
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
            )
        except jwt.InvalidTokenError as exc:
            raise HTTPException(
                status_code=403, detail=f"invalid token: {exc}"
            ) from exc

        sub = claims.get("sub")
        if not sub:
            raise HTTPException(status_code=403, detail="token missing sub")

        scopes = self._extract_scopes(claims)
        return UserPrincipal(
            user_id=str(sub),
            display_name=claims.get("name") or claims.get("preferred_username"),
            email=claims.get("email"),
            scopes=tuple(scopes),
            raw_claims=dict(claims),
        )

    @staticmethod
    def _extract_scopes(claims: dict[str, Any]) -> tuple[str, ...]:
        raw = claims.get("scope") or claims.get("scopes") or ""
        if isinstance(raw, str):
            return tuple(s for s in raw.split() if s)
        if isinstance(raw, (list, tuple)):
            return tuple(str(s) for s in raw)
        return ()
