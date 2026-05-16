"""Async SQLAlchemy ``HistoryStore`` for SQLite/Postgres."""

from __future__ import annotations

import datetime as _dt
from typing import Any

from sqlalchemy import delete, event, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from openbb_agent_server.persistence import models as m
from openbb_agent_server.persistence.store import (
    HistoryStore,
    MessageRecord,
    UsageRecord,
)
from openbb_agent_server.runtime.principal import UserPrincipal


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _apply_sqlite_pragmas(engine: AsyncEngine, url: str) -> None:
    """Enable WAL + a generous busy_timeout so writers don't deadlock.

    SQLiteVec (sync) and aiosqlite (async) both write to the same file in
    a default dev install; without WAL one will hit ``database is locked``
    the moment the other holds the write lock. WAL allows concurrent
    readers + a queued writer; busy_timeout makes the loser wait instead
    of immediately erroring.
    """
    if "sqlite" not in url:
        return

    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_conn: Any, _: Any) -> None:
        cur = dbapi_conn.cursor()
        try:
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.execute("PRAGMA synchronous=NORMAL")
        finally:
            cur.close()


class SqliteHistoryStore(HistoryStore):
    """Single ``HistoryStore`` implementation for SQLite + Postgres."""

    def __init__(self, url: str) -> None:
        self._engine: AsyncEngine = create_async_engine(url, future=True)
        _apply_sqlite_pragmas(self._engine, url)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False)

    async def init_schema(self) -> None:
        """Create tables if they don't exist (zero-config bootstrap)."""
        async with self._engine.begin() as conn:
            await conn.run_sync(m.Base.metadata.create_all)

    async def aclose(self) -> None:
        await self._engine.dispose()

    async def upsert_user(self, principal: UserPrincipal) -> None:
        async with self._sessionmaker() as session:
            existing = await session.get(m.User, principal.user_id)
            if existing is None:
                session.add(
                    m.User(
                        user_id=principal.user_id,
                        display_name=principal.display_name,
                        email=principal.email,
                    )
                )
            else:
                existing.display_name = principal.display_name
                existing.email = principal.email
                existing.last_seen_at = _now()
            await session.commit()

    async def delete_user(self, principal: UserPrincipal) -> None:
        async with self._sessionmaker() as session:
            await self._cascade_delete(session, principal.user_id)
            await session.commit()

    async def begin_trace(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
        conversation_id: str | None,
        run_id: str | None,
    ) -> None:
        async with self._sessionmaker() as session:
            row = await session.get(m.Trace, trace_id)
            if row is None:
                session.add(
                    m.Trace(
                        trace_id=trace_id,
                        user_id=principal.user_id,
                        conversation_id=conversation_id,
                        run_id=run_id,
                    )
                )
            else:
                if row.user_id != principal.user_id:
                    raise PermissionError(f"trace {trace_id} belongs to another user")
                row.run_id = run_id
                row.ended_at = None
                row.status = "running"
            await session.commit()

    async def end_trace(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
        status: str,
    ) -> None:
        async with self._sessionmaker() as session:
            row = await self._scoped_get_trace(session, principal, trace_id)
            if row is None:
                return
            row.ended_at = _now()
            row.status = status
            await session.commit()

    async def append_message(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str,
        role: str,
        content: str,
        trace_id: str | None,
    ) -> int:
        async with self._sessionmaker() as session:
            conv = await session.get(m.Conversation, conversation_id)
            if conv is None:
                conv = m.Conversation(
                    conversation_id=conversation_id,
                    user_id=principal.user_id,
                )
                session.add(conv)
                seq = 0
            elif conv.user_id != principal.user_id:
                # Cross-user lookup. Fail closed without leaking existence.
                raise PermissionError("conversation not found")
            else:
                last = await session.scalar(
                    select(m.Message.seq)
                    .where(m.Message.conversation_id == conversation_id)
                    .order_by(m.Message.seq.desc())
                    .limit(1)
                )
                seq = 0 if last is None else last + 1
            session.add(
                m.Message(
                    conversation_id=conversation_id,
                    user_id=principal.user_id,
                    seq=seq,
                    role=role,
                    content=content,
                    trace_id=trace_id,
                )
            )
            conv.updated_at = _now()
            await session.commit()
            return seq

    async def list_conversations(
        self,
        *,
        principal: UserPrincipal,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        async with self._sessionmaker() as session:
            rows = (
                (
                    await session.execute(
                        select(m.Conversation)
                        .where(m.Conversation.user_id == principal.user_id)
                        .order_by(m.Conversation.updated_at.desc())
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )
            return [
                {
                    "conversation_id": r.conversation_id,
                    "title": r.title,
                    "updated_at": r.updated_at.isoformat(),
                }
                for r in rows
            ]

    async def get_messages(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str,
        limit: int = 200,
    ) -> list[MessageRecord]:
        async with self._sessionmaker() as session:
            rows = (
                (
                    await session.execute(
                        select(m.Message)
                        .where(
                            m.Message.user_id == principal.user_id,
                            m.Message.conversation_id == conversation_id,
                        )
                        .order_by(m.Message.seq.asc())
                        .limit(limit)
                    )
                )
                .scalars()
                .all()
            )
            return [
                MessageRecord(
                    conversation_id=r.conversation_id,
                    seq=r.seq,
                    role=r.role,
                    content=r.content,
                    user_id=r.user_id,
                    trace_id=r.trace_id,
                    ts=r.ts,
                )
                for r in rows
            ]

    async def record_tool_call(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
        tool_name: str,
        args: dict[str, Any],
        result: dict[str, Any] | None,
        error: str | None,
        latency_ms: int | None,
        side: str,
        state: str,
    ) -> None:
        async with self._sessionmaker() as session:
            seq = await self._next_seq(session, m.ToolCall, trace_id, principal)
            session.add(
                m.ToolCall(
                    trace_id=trace_id,
                    user_id=principal.user_id,
                    seq=seq,
                    tool_name=tool_name,
                    args_json=args,
                    result_json=result,
                    error=error,
                    latency_ms=latency_ms,
                    side=side,
                    state=state,
                )
            )
            await session.commit()

    async def record_usage(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
        usage: UsageRecord,
    ) -> None:
        if usage.user_id != principal.user_id:
            raise PermissionError("usage record user mismatch")
        async with self._sessionmaker() as session:
            seq = await self._next_seq(session, m.Usage, trace_id, principal)
            session.add(
                m.Usage(
                    trace_id=trace_id,
                    user_id=principal.user_id,
                    seq=seq,
                    model=usage.model,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    cache_read=usage.cache_read,
                    cache_creation=usage.cache_creation,
                    cost_usd=usage.cost_usd,
                )
            )
            await session.commit()

    async def get_trace_bundle(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
    ) -> dict[str, Any] | None:
        from sqlalchemy import select

        async with self._sessionmaker() as session:
            trace = await session.get(m.Trace, trace_id)
            if trace is None or trace.user_id != principal.user_id:
                return None

            tool_calls = (
                (
                    await session.execute(
                        select(m.ToolCall)
                        .where(
                            m.ToolCall.trace_id == trace_id,
                            m.ToolCall.user_id == principal.user_id,
                        )
                        .order_by(m.ToolCall.seq.asc())
                    )
                )
                .scalars()
                .all()
            )

            usage = (
                (
                    await session.execute(
                        select(m.Usage)
                        .where(
                            m.Usage.trace_id == trace_id,
                            m.Usage.user_id == principal.user_id,
                        )
                        .order_by(m.Usage.seq.asc())
                    )
                )
                .scalars()
                .all()
            )

            artifacts = (
                (
                    await session.execute(
                        select(m.Artifact)
                        .where(
                            m.Artifact.trace_id == trace_id,
                            m.Artifact.user_id == principal.user_id,
                        )
                        .order_by(m.Artifact.seq.asc())
                    )
                )
                .scalars()
                .all()
            )

            citations = (
                (
                    await session.execute(
                        select(m.CitationRow)
                        .where(
                            m.CitationRow.trace_id == trace_id,
                            m.CitationRow.user_id == principal.user_id,
                        )
                        .order_by(m.CitationRow.seq.asc())
                    )
                )
                .scalars()
                .all()
            )

            messages: list[m.Message] = []
            if trace.conversation_id:
                messages = list(
                    (
                        await session.execute(
                            select(m.Message)
                            .where(
                                m.Message.user_id == principal.user_id,
                                m.Message.conversation_id == trace.conversation_id,
                                m.Message.trace_id == trace_id,
                            )
                            .order_by(m.Message.seq.asc())
                        )
                    )
                    .scalars()
                    .all()
                )

            return {
                "trace": {
                    "trace_id": trace.trace_id,
                    "run_id": trace.run_id,
                    "conversation_id": trace.conversation_id,
                    "started_at": trace.started_at.isoformat()
                    if trace.started_at
                    else None,
                    "ended_at": trace.ended_at.isoformat() if trace.ended_at else None,
                    "status": trace.status,
                },
                "messages": [
                    {
                        "seq": r.seq,
                        "role": r.role,
                        "content": r.content,
                        "ts": r.ts.isoformat() if r.ts else None,
                    }
                    for r in messages
                ],
                "tool_calls": [
                    {
                        "seq": r.seq,
                        "tool_name": r.tool_name,
                        "args": r.args_json,
                        "result": r.result_json,
                        "error": r.error,
                        "latency_ms": r.latency_ms,
                        "side": r.side,
                        "state": r.state,
                    }
                    for r in tool_calls
                ],
                "usage": [
                    {
                        "model": r.model,
                        "input_tokens": r.input_tokens,
                        "output_tokens": r.output_tokens,
                        "cache_read": r.cache_read,
                        "cache_creation": r.cache_creation,
                        "cost_usd": r.cost_usd,
                    }
                    for r in usage
                ],
                "artifacts": [
                    {
                        "seq": r.seq,
                        "kind": r.kind,
                        "payload": r.payload_json,
                        "mime": r.mime,
                    }
                    for r in artifacts
                ],
                "citations": [
                    {
                        "seq": r.seq,
                        "source": r.source,
                        "source_url": r.source_url,
                        "page": r.page,
                        "text_snippet": r.text_snippet,
                    }
                    for r in citations
                ],
            }

    async def usage_summary(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str | None = None,
        conversation_id: str | None = None,
    ) -> dict[str, Any]:
        from sqlalchemy import func, select

        async with self._sessionmaker() as session:
            stmt = select(
                m.Usage.model,
                func.sum(m.Usage.input_tokens),
                func.sum(m.Usage.output_tokens),
                func.sum(m.Usage.cache_read),
                func.sum(m.Usage.cache_creation),
                func.sum(m.Usage.cost_usd),
                func.count(m.Usage.id),
            ).where(m.Usage.user_id == principal.user_id)
            if trace_id:
                stmt = stmt.where(m.Usage.trace_id == trace_id)
            if conversation_id:
                stmt = stmt.join(m.Trace, m.Trace.trace_id == m.Usage.trace_id).where(
                    m.Trace.conversation_id == conversation_id
                )
            stmt = stmt.group_by(m.Usage.model)
            rows = (await session.execute(stmt)).all()
            return {
                "by_model": [
                    {
                        "model": r[0],
                        "input_tokens": int(r[1] or 0),
                        "output_tokens": int(r[2] or 0),
                        "cache_read": int(r[3] or 0),
                        "cache_creation": int(r[4] or 0),
                        "cost_usd": float(r[5] or 0.0),
                        "calls": int(r[6] or 0),
                    }
                    for r in rows
                ]
            }

    async def _scoped_get_trace(
        self,
        session: AsyncSession,
        principal: UserPrincipal,
        trace_id: str,
    ) -> m.Trace | None:
        row = await session.get(m.Trace, trace_id)
        if row is None or row.user_id != principal.user_id:
            return None
        return row

    async def _next_seq(
        self,
        session: AsyncSession,
        table: type[Any],
        trace_id: str,
        principal: UserPrincipal,
    ) -> int:
        last = await session.scalar(
            select(table.seq)
            .where(
                table.trace_id == trace_id,
                table.user_id == principal.user_id,
            )
            .order_by(table.seq.desc())
            .limit(1)
        )
        return 0 if last is None else last + 1

    async def _cascade_delete(self, session: AsyncSession, user_id: str) -> None:
        for table in (
            m.Message,
            m.ToolCall,
            m.Usage,
            m.Artifact,
            m.CitationRow,
            m.PendingRun,
            m.Run,
            m.Trace,
            m.Conversation,
            m.ApiKey,
        ):
            await session.execute(delete(table).where(table.user_id == user_id))
        await session.execute(delete(m.User).where(m.User.user_id == user_id))
