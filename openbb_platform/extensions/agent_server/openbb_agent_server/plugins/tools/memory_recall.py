"""``recall_user_memory`` tool source."""

from __future__ import annotations

from typing import Any

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from openbb_agent_server.memory.store import MemoryStore
from openbb_agent_server.runtime import context as run_context
from openbb_agent_server.runtime.context import RunContext
from openbb_agent_server.runtime.plugins import ToolSource


class _RecallArgs(BaseModel):
    query: str = Field(description="What to recall about the user.")
    k: int = Field(default=8, ge=1, le=32, description="How many memories to return.")


class MemoryRecallToolSource(ToolSource):
    """Wrap a MemoryStore instance as a LangChain tool."""

    name = "recall_user_memory"

    def __init__(self, *, store: MemoryStore | None = None) -> None:
        self._store = store

    def _bind_store(self, store: MemoryStore) -> None:
        self._store = store

    async def tools(self, ctx: RunContext, config: dict[str, Any]) -> list[Any]:
        store = self._store
        if store is None:
            return []

        async def _recall(query: str, k: int = 8) -> list[dict[str, Any]]:
            principal = run_context.current().principal
            results = await store.recall(principal=principal, query=query, k=k)
            return [
                {
                    "id": m.memory_id,
                    "text": m.text,
                    "kind": m.kind,
                    "pinned": m.pinned,
                    "score": m.score,
                }
                for m in results
            ]

        return [
            StructuredTool.from_function(
                coroutine=_recall,
                name="recall_user_memory",
                description=(
                    "Recall durable facts/preferences this user has accumulated "
                    "across prior conversations. Always scoped to the current "
                    "user; cannot leak data across users."
                ),
                args_schema=_RecallArgs,
            )
        ]
