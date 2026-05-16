"""``sqlite`` — async SQLite checkpointer with proper pragmas.

LangGraph's ``AsyncSqliteSaver.from_conn_string()`` opens the file
with ``aiosqlite.connect(path)`` and the sqlite3 defaults — rollback
journal, ``busy_timeout=0``. Any concurrent writer (history store,
widget store, pdf store) collides on the file's reserved lock and the
saver write fails or blocks. We open the connection ourselves, set
the pragmas the rest of the codebase relies on (WAL + busy_timeout),
and hand the configured connection to ``AsyncSqliteSaver`` directly
so its operations inherit the same lock-tolerant connection settings.
"""

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

# 30 s — long enough to outlast normal contention (a write barrel
# under load), short enough that a genuine deadlock surfaces
# instead of freezing forever. Matches the timeout the other stores
# in this codebase use, scaled up for the checkpointer's longer
# transactions.
_BUSY_TIMEOUT_MS = 30_000


async def _configure_connection(conn: aiosqlite.Connection) -> None:
    """Apply the pragmas every writer to this file relies on.

    ``journal_mode=WAL`` is persistent (file-level) but harmless to
    re-issue. ``busy_timeout``, ``synchronous`` and ``temp_store``
    are per-connection and must be set on EVERY connection — including
    the one LangGraph uses for its own writes.
    """
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
        # ``mkdir`` is the one sync call in this startup path — off-
        # load it so the event loop isn't blocked on the filesystem.
        await asyncio.to_thread(Path(path).parent.mkdir, parents=True, exist_ok=True)

        # Open OUR connection — not LangGraph's default — so we control
        # the pragmas. ``timeout`` here is the connection-level
        # ``sqlite3.connect(timeout=...)`` (seconds) which sets
        # ``busy_timeout`` for the initial DDL; the explicit PRAGMA
        # below pins it persistently. aiosqlite runs the connection
        # in its own worker thread, so this ``await`` doesn't block
        # the loop.
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
        del saver  # the saver doesn't own the connection — we do
        if self._conn is not None:
            try:
                await self._conn.close()
            finally:
                self._conn = None
