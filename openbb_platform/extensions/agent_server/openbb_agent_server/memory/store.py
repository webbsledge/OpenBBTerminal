"""MemoryStore ABC — per-user vector memory for cross-thread recall."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel

from openbb_agent_server.runtime.principal import UserPrincipal


class Memory(BaseModel):
    memory_id: str
    user_id: str
    text: str
    kind: str = "fact"
    pinned: bool = False
    source_trace_id: str | None = None
    score: float | None = None  # populated on retrieval


class MemoryStore(ABC):
    """All methods are principal-scoped."""

    @abstractmethod
    async def write(
        self,
        *,
        principal: UserPrincipal,
        text: str,
        kind: str = "fact",
        source_trace_id: str | None = None,
    ) -> Memory: ...

    @abstractmethod
    async def recall(
        self,
        *,
        principal: UserPrincipal,
        query: str,
        k: int = 8,
    ) -> list[Memory]: ...

    @abstractmethod
    async def list_memories(
        self,
        *,
        principal: UserPrincipal,
        limit: int = 100,
    ) -> list[Memory]: ...

    @abstractmethod
    async def pin(
        self,
        *,
        principal: UserPrincipal,
        memory_id: str,
        pinned: bool,
    ) -> Memory | None: ...

    @abstractmethod
    async def forget(
        self,
        *,
        principal: UserPrincipal,
        memory_id: str,
    ) -> bool: ...

    @abstractmethod
    async def delete_all_for_user(self, principal: UserPrincipal) -> int:
        """Right-to-erasure: drop every memory for ``principal.user_id``."""
