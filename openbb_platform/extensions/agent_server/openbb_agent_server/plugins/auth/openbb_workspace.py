"""OpenBB Workspace auth backend."""

from __future__ import annotations

import logging

from fastapi import HTTPException, Request

from openbb_agent_server.runtime.identity import hash_user_id, is_email
from openbb_agent_server.runtime.plugins import AuthBackend
from openbb_agent_server.runtime.principal import UserPrincipal

logger = logging.getLogger("openbb_agent_server.auth.openbb_workspace")

DEFAULT_HEADER = "X-OpenBB-User"

DEFAULT_SCOPES: tuple[str, ...] = (
    "agent:query",
    "memory:read",
    "memory:write",
)


class OpenBBWorkspaceAuthBackend(AuthBackend):
    """Trust the X-OpenBB-User header from an upstream Workspace gateway."""

    name = "openbb_workspace"

    def __init__(
        self,
        *,
        header: str = DEFAULT_HEADER,
        scopes: tuple[str, ...] = DEFAULT_SCOPES,
        require_email: bool = True,
    ) -> None:
        self._header = header
        self._scopes = tuple(scopes)
        self._require_email = require_email

    async def authenticate(self, request: Request) -> UserPrincipal:
        raw = request.headers.get(self._header)
        if not raw:
            raise HTTPException(
                status_code=401,
                detail=f"missing required header {self._header}",
            )
        cleaned = raw.strip().lower()
        if not cleaned:
            raise HTTPException(
                status_code=401,
                detail=f"empty {self._header} header",
            )
        if self._require_email and not is_email(cleaned):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"{self._header} must be an email address; got a value "
                    "that doesn't match RFC-5321"
                ),
            )
        email = cleaned if is_email(cleaned) else None
        user_id = hash_user_id(cleaned)
        return UserPrincipal(
            user_id=user_id,
            email=email,
            scopes=self._scopes,
        )
