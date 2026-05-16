"""User principal — the resolved identity attached to every request."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class UserPrincipal(BaseModel):
    """The authenticated identity for one request."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    user_id: str = Field(..., min_length=1)
    display_name: str | None = None
    email: str | None = None
    scopes: tuple[str, ...] = ()
    raw_claims: dict[str, Any] = Field(default_factory=dict)

    def has_scope(self, scope: str) -> bool:
        """Return True iff ``scope`` is granted (exact match)."""
        return scope in self.scopes
