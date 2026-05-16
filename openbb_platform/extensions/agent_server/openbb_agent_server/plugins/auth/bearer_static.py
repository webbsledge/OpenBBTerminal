"""``bearer_static`` — single shared secret, single-user dev backend."""

from __future__ import annotations

import hmac
import logging
import os

from fastapi import HTTPException, Request

from openbb_agent_server.runtime.plugins import AuthBackend
from openbb_agent_server.runtime.principal import UserPrincipal

logger = logging.getLogger("openbb_agent_server.auth.bearer_static")


class BearerStaticAuthBackend(AuthBackend):
    """Single shared bearer token. Dev/test only."""

    name = "bearer_static"

    def __init__(
        self,
        *,
        token: str | None = None,
        user_id: str = "static-user",
        display_name: str | None = None,
        scopes: tuple[str, ...] = ("agent:query", "memory:read", "memory:write"),
    ) -> None:
        token = token or os.environ.get("OPENBB_AGENT_AUTH_BEARER")
        if not token:
            raise RuntimeError(
                "BearerStaticAuthBackend requires a non-empty token "
                "(``token`` config key or OPENBB_AGENT_AUTH_BEARER env var)."
            )
        self._token = token
        self._user_id = user_id
        self._display_name = display_name
        self._scopes = scopes
        logger.warning(
            "BearerStaticAuthBackend resolves every authenticated request "
            "to user '%s'. Use the api_key_table or oidc_jwt backend in "
            "production.",
            user_id,
        )

    async def authenticate(self, request: Request) -> UserPrincipal:
        header = request.headers.get("authorization") or ""
        if not header.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="missing bearer token")
        supplied = header[len("bearer ") :].strip()
        if not hmac.compare_digest(supplied, self._token):
            raise HTTPException(status_code=403, detail="invalid bearer token")
        return UserPrincipal(
            user_id=self._user_id,
            display_name=self._display_name,
            scopes=self._scopes,
        )
