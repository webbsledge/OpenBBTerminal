"""HistoryStore — the multi-tenant persistence ABC."""

from __future__ import annotations

import datetime as _dt
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict

from openbb_agent_server.runtime.principal import UserPrincipal


class TraceRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    trace_id: str
    user_id: str
    conversation_id: str | None
    run_id: str | None
    started_at: _dt.datetime
    ended_at: _dt.datetime | None
    status: str


class MessageRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    conversation_id: str
    seq: int
    role: str
    content: str
    user_id: str
    trace_id: str | None
    ts: _dt.datetime


class ToolCallRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    trace_id: str
    seq: int
    user_id: str
    tool_name: str
    args: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    latency_ms: int | None
    side: str
    state: str


class UsageRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    trace_id: str
    user_id: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_read: int
    cache_creation: int
    cost_usd: float


class HistoryStore(ABC):
    """Persistence ABC. All queries are principal-scoped."""

    @abstractmethod
    async def upsert_user(self, principal: UserPrincipal) -> None: ...

    @abstractmethod
    async def begin_trace(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
        conversation_id: str | None,
        run_id: str | None,
    ) -> None: ...

    @abstractmethod
    async def end_trace(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
        status: str,
    ) -> None: ...

    @abstractmethod
    async def append_message(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str,
        role: str,
        content: str,
        trace_id: str | None,
    ) -> int:
        """Append one message; returns the new ``seq``."""

    @abstractmethod
    async def list_conversations(
        self,
        *,
        principal: UserPrincipal,
        limit: int = 50,
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def get_messages(
        self,
        *,
        principal: UserPrincipal,
        conversation_id: str,
        limit: int = 200,
    ) -> list[MessageRecord]: ...

    @abstractmethod
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
    ) -> None: ...

    @abstractmethod
    async def record_usage(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
        usage: UsageRecord,
    ) -> None: ...

    @abstractmethod
    async def delete_user(self, principal: UserPrincipal) -> None:
        """Cascade-delete every row owned by the user."""

    @abstractmethod
    async def get_trace_bundle(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str,
    ) -> dict[str, Any] | None:
        """Return the full audit-join for one trace, scoped to the principal."""

    @abstractmethod
    async def usage_summary(
        self,
        *,
        principal: UserPrincipal,
        trace_id: str | None = None,
        conversation_id: str | None = None,
    ) -> dict[str, Any]:
        """Return aggregated usage rows scoped to the principal."""
