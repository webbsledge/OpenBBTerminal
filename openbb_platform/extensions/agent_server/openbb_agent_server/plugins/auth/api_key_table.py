"""``api_key_table`` auth backend."""

from __future__ import annotations

import datetime as _dt
import logging
import secrets
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from openbb_agent_server.persistence import models as m
from openbb_agent_server.runtime.plugins import AuthBackend
from openbb_agent_server.runtime.principal import UserPrincipal

logger = logging.getLogger("openbb_agent_server.auth.api_key_table")

KEY_PREFIX = "oba_"
KEY_SEP = "."


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


class IssuedKey:
    """Plaintext key + its row metadata. Returned by :meth:`issue`."""

    __slots__ = ("plaintext", "key_id", "user_id", "scopes", "label")

    def __init__(
        self,
        *,
        plaintext: str,
        key_id: str,
        user_id: str,
        scopes: tuple[str, ...],
        label: str | None,
    ) -> None:
        self.plaintext = plaintext
        self.key_id = key_id
        self.user_id = user_id
        self.scopes = scopes
        self.label = label


class ApiKeyTableAuthBackend(AuthBackend):
    """Hashed API keys keyed off the ``api_keys`` table."""

    name = "api_key_table"

    def __init__(self, *, db_url: str) -> None:
        self._engine = create_async_engine(db_url, future=True)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)
        self._hasher = PasswordHasher()

    @staticmethod
    def _extract_key(request: Request) -> str | None:
        header = request.headers.get("x-api-key")
        if header:
            return header.strip()
        auth = request.headers.get("authorization") or ""
        if auth.lower().startswith("bearer "):
            return auth[len("bearer ") :].strip()
        return None

    @staticmethod
    def _split(raw: str) -> tuple[str, str]:
        if not (raw.startswith(KEY_PREFIX) and KEY_SEP in raw):
            raise HTTPException(status_code=403, detail="invalid api key")
        key_id, secret = raw[len(KEY_PREFIX) :].split(KEY_SEP, 1)
        if not key_id or not secret:
            raise HTTPException(status_code=403, detail="invalid api key")
        return key_id, secret

    async def authenticate(self, request: Request) -> UserPrincipal:
        raw = self._extract_key(request)
        if not raw:
            raise HTTPException(status_code=401, detail="missing api key")
        key_id, secret = self._split(raw)

        async with self._sessionmaker() as session:
            row = await session.get(m.ApiKey, key_id)
            if row is None or row.revoked_at is not None:
                raise HTTPException(status_code=403, detail="invalid api key")
            try:
                self._hasher.verify(row.hashed_secret, secret)
            except VerifyMismatchError as exc:
                raise HTTPException(status_code=403, detail="invalid api key") from exc
            user = await session.get(m.User, row.user_id)
            if user is None:
                raise HTTPException(status_code=403, detail="invalid api key")
            return UserPrincipal(
                user_id=user.user_id,
                display_name=user.display_name,
                email=user.email,
                scopes=tuple(row.scopes or ()),
            )

    async def aclose(self) -> None:
        await self._engine.dispose()

    async def issue(
        self,
        *,
        user_id: str,
        scopes: tuple[str, ...] = ("agent:query", "memory:read"),
        label: str | None = None,
        display_name: str | None = None,
        email: str | None = None,
    ) -> IssuedKey:
        """Mint a key. Upserts the user row and returns the plaintext key once."""
        key_id = secrets.token_urlsafe(8)
        secret = secrets.token_urlsafe(32)
        hashed = self._hasher.hash(secret)
        async with self._sessionmaker() as session:
            user = await session.get(m.User, user_id)
            if user is None:
                user = m.User(
                    user_id=user_id,
                    display_name=display_name,
                    email=email,
                )
                session.add(user)
            else:
                if display_name and not user.display_name:
                    user.display_name = display_name
                if email and not user.email:
                    user.email = email
                user.last_seen_at = _now()
            session.add(
                m.ApiKey(
                    key_id=key_id,
                    user_id=user_id,
                    hashed_secret=hashed,
                    label=label,
                    scopes=list(scopes),
                )
            )
            await session.commit()
        return IssuedKey(
            plaintext=f"{KEY_PREFIX}{key_id}{KEY_SEP}{secret}",
            key_id=key_id,
            user_id=user_id,
            scopes=tuple(scopes),
            label=label,
        )

    async def revoke(self, *, key_id: str) -> bool:
        async with self._sessionmaker() as session:
            row = await session.get(m.ApiKey, key_id)
            if row is None:
                return False
            row.revoked_at = _now()
            await session.commit()
            return True

    async def list_keys(self, *, user_id: str | None = None) -> list[dict[str, Any]]:
        """Return non-secret metadata for every key (or every key for one user)."""
        async with self._sessionmaker() as session:
            stmt = select(m.ApiKey)
            if user_id:
                stmt = stmt.where(m.ApiKey.user_id == user_id)
            rows = (await session.execute(stmt)).scalars().all()
            return [
                {
                    "key_id": r.key_id,
                    "user_id": r.user_id,
                    "label": r.label,
                    "scopes": list(r.scopes or ()),
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "revoked_at": (r.revoked_at.isoformat() if r.revoked_at else None),
                }
                for r in rows
            ]
