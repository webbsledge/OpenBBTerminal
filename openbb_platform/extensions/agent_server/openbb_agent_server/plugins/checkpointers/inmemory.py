"""In-memory checkpointer provider."""

from __future__ import annotations

from typing import Any

from langgraph.checkpoint.memory import InMemorySaver

from openbb_agent_server.runtime.plugins import CheckpointerProvider


class InMemoryCheckpointerProvider(CheckpointerProvider):
    """Construct a process-local InMemorySaver."""

    name = "inmemory"

    def __init__(self, **_config: Any) -> None:
        pass

    async def open(self, settings: Any) -> InMemorySaver:
        return InMemorySaver()

    async def close(self, saver: Any) -> None:
        return None
