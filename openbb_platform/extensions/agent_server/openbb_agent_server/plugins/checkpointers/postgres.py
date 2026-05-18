"""Async Postgres checkpointer."""

from __future__ import annotations

import logging
import os
from typing import Any

from openbb_agent_server.runtime.plugins import CheckpointerProvider

logger = logging.getLogger("openbb_agent_server.checkpointer.postgres")


def _normalise_pg_url(url: str) -> str:
    """Strip SQLAlchemy-style driver suffixes that psycopg cannot parse."""
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url[len("postgresql+psycopg://") :]
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url[len("postgresql+asyncpg://") :]
    return url


class PostgresCheckpointerProvider(CheckpointerProvider):
    """Persistent Postgres-backed checkpointer (production default)."""

    name = "postgres"

    def __init__(self, *, url: str | None = None, **_config: Any) -> None:
        self._explicit_url = url
        self._cm: Any = None

    def _resolve_url(self, settings: Any) -> str:
        url = (
            self._explicit_url
            or os.environ.get("OPENBB_AGENT_CHECKPOINTER_URL")
            or settings.resolved_db_url()
        )
        url = _normalise_pg_url(url)
        if not url.startswith("postgresql://"):
            raise RuntimeError(
                f"Postgres checkpointer expects a postgresql:// URL, got {url!r}"
            )
        return url

    async def open(
        self, settings: Any
    ) -> Any:  # pragma: no cover — needs live Postgres
        try:
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        except ImportError as exc:
            raise RuntimeError(
                "Postgres checkpointer requires langgraph-checkpoint-postgres "
                "and psycopg[binary]. Install the agent_server with the "
                "[postgres] extra."
            ) from exc

        url = self._resolve_url(settings)
        self._cm = AsyncPostgresSaver.from_conn_string(url)
        saver = await self._cm.__aenter__()
        await saver.setup()
        logger.info("postgres checkpointer opened against %s", url)
        return saver

    async def close(self, saver: Any) -> None:  # pragma: no cover — paired with open()
        if self._cm is not None:
            try:
                await self._cm.__aexit__(None, None, None)
            finally:
                self._cm = None
