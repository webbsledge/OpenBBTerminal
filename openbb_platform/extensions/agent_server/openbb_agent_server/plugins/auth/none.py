"""No-auth, single-user dev backend."""

from __future__ import annotations

import logging

from fastapi import Request

from openbb_agent_server.runtime.plugins import AuthBackend
from openbb_agent_server.runtime.principal import UserPrincipal

logger = logging.getLogger("openbb_agent_server.auth.none")

ANONYMOUS_USER_ID = "anonymous"


class NoneAuthBackend(AuthBackend):
    """No authentication. Dev-only."""

    name = "none"

    def __init__(self, **_config: object) -> None:
        logger.warning(
            "NoneAuthBackend is enabled — every request resolves to the "
            "single shared user '%s'. Do not use in production.",
            ANONYMOUS_USER_ID,
        )

    async def authenticate(self, request: Request) -> UserPrincipal:
        return UserPrincipal(
            user_id=ANONYMOUS_USER_ID,
            display_name="Anonymous",
            scopes=("agent:query", "memory:read"),
        )
