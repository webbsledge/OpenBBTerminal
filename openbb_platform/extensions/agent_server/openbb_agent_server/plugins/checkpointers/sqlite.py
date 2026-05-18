"""Async SQLite checkpointer with proper pragmas."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import aiosqlite
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from openbb_agent_server.runtime.plugins import CheckpointerProvider

logger = logging.getLogger("openbb_agent_server.checkpointer.sqlite")

_BUSY_TIMEOUT_MS = 30_000


async def _configure_connection(conn: aiosqlite.Connection) -> None:
    """Apply the pragmas every writer to this file relies on."""
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.commit()


class SqliteCheckpointerProvider(CheckpointerProvider):
    """Persistent SQLite-backed checkpointer."""

    name = "sqlite"

    def __init__(self, *, path: str | None = None, **_config: Any) -> None:
        self._explicit_path = path
        self._conn: aiosqlite.Connection | None = None

    async def open(self, settings: Any) -> AsyncSqliteSaver:
        path = (
            self._explicit_path
            or os.environ.get("OPENBB_AGENT_CHECKPOINTER_PATH")
            or str(Path(settings.data_dir) / "checkpoints.db")
        )
        await asyncio.to_thread(Path(path).parent.mkdir, parents=True, exist_ok=True)

        conn = await aiosqlite.connect(
            path,
            timeout=_BUSY_TIMEOUT_MS / 1000.0,
        )
        await _configure_connection(conn)
        self._conn = conn

        saver = AsyncSqliteSaver(conn)
        await saver.setup()
        logger.info("sqlite checkpointer opened at %s", path)
        return saver

    async def close(self, saver: Any) -> None:
        del saver
        if self._conn is not None:
            try:
                await self._conn.close()
            finally:
                self._conn = None
